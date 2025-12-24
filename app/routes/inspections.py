# app/routes/inspections.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx

inspections_bp = Blueprint("inspections", __name__)

# Estados válidos de cada paso
ALLOWED_STEP_STATUS = {"Apto", "Condicional", "Rechazado"}


# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

def _norm_status(s: str | None) -> str | None:
    """
    Normaliza cosas tipo 'apto' -> 'Apto'
    """
    if not s:
        return None
    s = str(s).strip()
    return s[:1].upper() + s[1:].lower()


async def _get_workshop_for_inspection(conn, inspection_id: int) -> int | None:
    """
    Devuelve el workshop_id de la inspección.
    Lo usamos para saber qué catálogo de observaciones aplica,
    y también para validar que el paso pertenece al mismo taller.
    """
    return await conn.fetchval(
        """
        SELECT a.workshop_id
        FROM inspections i
        JOIN applications a ON a.id = i.application_id
        WHERE i.id = $1
        """,
        inspection_id,
    )


async def _step_belongs_to_workshop(conn, step_id: int, workshop_id: int) -> bool:
    """
    Confirma que ese paso (step_id) exista para ese taller en steps_order.
    """
    return await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM steps_order so
          WHERE so.workshop_id = $1
            AND so.step_id = $2
        )
        """,
        workshop_id,
        step_id,
    )


async def _ensure_inspection_detail(conn, inspection_id: int, step_id: int) -> int:
    """
    Helper legacy:
    Antes necesitábamos un inspection_details fila por paso para guardar
    observaciones tildadas. El front nuevo NO usa esto ya,
    pero lo dejamos para compatibilidad con flows viejos.
    """
    det_id = await conn.fetchval(
        """
        SELECT id
        FROM inspection_details
        WHERE inspection_id = $1
          AND step_id = $2
        """,
        inspection_id,
        step_id
    )
    if det_id:
        return det_id

    row = await conn.fetchrow(
        """
        INSERT INTO inspection_details (inspection_id, step_id, status, observations)
        VALUES ($1, $2, $3, NULL)
        RETURNING id
        """,
        inspection_id,
        step_id,
        "Condicional"  # default legacy
    )
    return row["id"]


# ---------------------------------------------------------------------------
# 1. Crear inspección
# ---------------------------------------------------------------------------

@inspections_bp.route("/inspections", methods=["POST"])
async def create_inspection():
    """
    Crea o recupera la inspección activa de una aplicación.

    - Si el trámite está en estado "Segunda Inspección" se crea (o reutiliza) una
      inspección nueva con `is_second = TRUE`, siempre que el resultado previo
      sea "Condicional".
    - Caso contrario se aplica la lógica tradicional de primera inspección.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = (await request.get_json()) or {}
    app_id = data.get("application_id")
    if app_id is None:
        return jsonify({"error": "Falta application_id"}), 400

    try:
        app_id_int = int(app_id)
    except (TypeError, ValueError):
        return jsonify({"error": "application_id inválido"}), 400

    async with get_conn_ctx() as conn:
        application = await conn.fetchrow(
            """
            SELECT id, status, result, result_2
            FROM applications
            WHERE id = $1
            """,
            app_id_int,
        )

        if not application:
            return jsonify({"error": "Aplicación no encontrada"}), 404

        app_status = (application["status"] or "").strip()
        is_second_request = app_status == "Segunda Inspección"

        if is_second_request:
            result_value = (application["result"] or "").strip()
            if result_value != "Condicional":
                return jsonify({
                    "error": "Solo aplicaciones con resultado 'Condicional' pueden tener segunda inspección"
                }), 400

            if application.get("result_2") is not None:
                return jsonify({
                    "error": f"Esta aplicación ya completó su segunda inspección con resultado '{application['result_2']}'"
                }), 400

            existing_second = await conn.fetchrow(
                """
                SELECT id
                FROM inspections
                WHERE application_id = $1 AND COALESCE(is_second, FALSE) = TRUE
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                app_id_int,
            )

            if existing_second:
                return jsonify({
                    "message": "Segunda inspección ya existente",
                    "inspection_id": existing_second["id"],
                    "is_new": False,
                    "is_second": True,
                }), 200

            row = await conn.fetchrow(
                """
                INSERT INTO inspections (application_id, user_id, is_second, created_at)
                VALUES ($1, $2, TRUE, NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')
                RETURNING id
                """,
                app_id_int,
                user_id,
            )

            return jsonify({
                "message": "Segunda inspección creada",
                "inspection_id": row["id"],
                "is_new": True,
                "is_second": True,
            }), 201

        existing_first = await conn.fetchrow(
            """
            SELECT id
            FROM inspections
            WHERE application_id = $1 AND COALESCE(is_second, FALSE) = FALSE
            ORDER BY id ASC
            LIMIT 1
            """,
            app_id_int,
        )

        if existing_first:
            return jsonify({
                "message": "Inspección ya existente",
                "inspection_id": existing_first["id"],
                "is_new": False,
                "is_second": False,
            }), 200

        row = await conn.fetchrow(
            """
            INSERT INTO inspections (application_id, user_id, is_second, created_at)
            VALUES ($1, $2, FALSE, NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')
            RETURNING id
            """,
            app_id_int,
            user_id,
        )

        await conn.execute(
            "UPDATE applications SET status = $1 WHERE id = $2",
            "En curso",
            app_id_int,
        )

        return jsonify({
            "message": "Inspección creada",
            "inspection_id": row["id"],
            "is_new": True,
            "is_second": False,
        }), 201


# ---------------------------------------------------------------------------
# 2. Update inspección: global_observations (textarea grande) + opcional user_id
# ---------------------------------------------------------------------------

@inspections_bp.route("/inspections/<int:inspection_id>", methods=["PUT"])
async def update_inspection(inspection_id: int):
    """
    Se usa para guardar:
      - global_observations (texto final que ve el inspector en el textarea)
      - opcionalmente reasignar user_id

    Este endpoint se llama al apretar "Guardar" (junto con /details/bulk)
    y también antes de generar el certificado.

    Ya no persistimos las observaciones tildadas "por paso".
    Eso vive solamente como texto dentro de global_observations.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    sets = []
    values = []
    idx = 1

    # permitir cambio de user_id explícito
    if "user_id" in data and data["user_id"] is not None:
        sets.append(f"user_id = ${idx}")
        values.append(data["user_id"])
        idx += 1

    # guardar el textarea combinado
    if "global_observations" in data:
        go = (data.get("global_observations") or "").strip()

        # front usa límite 1200 chars; chequeamos acá también
        if len(go) > 1200:
            return jsonify(
                {"error": "Observaciones globales supera 1200 caracteres"}
            ), 400

        sets.append(f"global_observations = NULLIF(${idx}, '')")
        values.append(go)
        idx += 1

    if not sets:
        return jsonify({"error": "No hay datos para actualizar"}), 400

    values.append(inspection_id)

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM inspections WHERE id = $1",
            inspection_id
        )
        if not exists:
            return jsonify({"error": "Inspección no encontrada"}), 404

        await conn.execute(
            f"UPDATE inspections SET {', '.join(sets)} WHERE id = ${idx}",
            *values
        )

    return jsonify({"message": "Inspección actualizada"}), 200


# ---------------------------------------------------------------------------
# 3. Delete inspección
# ---------------------------------------------------------------------------

@inspections_bp.route("/inspections/<int:inspection_id>", methods=["DELETE"])
async def delete_inspection(inspection_id: int):
    """
    Borra la inspección y lo asociado (inspection_details y observation_details legacy).
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM inspections WHERE id = $1",
            inspection_id
        )
        if not exists:
            return jsonify({"error": "Inspección no encontrada"}), 404

        async with conn.transaction():
            # limpiamos observation_details de esos inspection_details
            await conn.execute(
                """
                DELETE FROM observation_details
                WHERE inspection_detail_id IN (
                  SELECT id
                  FROM inspection_details
                  WHERE inspection_id = $1
                )
                """,
                inspection_id,
            )

            # borramos inspection_details
            await conn.execute(
                "DELETE FROM inspection_details WHERE inspection_id = $1",
                inspection_id,
            )

            # y la inspección
            await conn.execute(
                "DELETE FROM inspections WHERE id = $1",
                inspection_id,
            )

    return jsonify({"message": "Inspección eliminada"}), 200


# ---------------------------------------------------------------------------
# 4. Obtener inspección por application_id y is_second
# ---------------------------------------------------------------------------

@inspections_bp.route("/applications/<int:app_id>/inspection", methods=["GET"])
async def get_inspection_by_application(app_id: int):
    """
    Devuelve la inspección de una aplicación.
    Query params:
      - is_second: boolean (opcional, por defecto False para primera inspección)
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    is_second = request.args.get("is_second", "false").lower() == "true"

    async with get_conn_ctx() as conn:
        inspection = await conn.fetchrow(
            """
            SELECT id
            FROM inspections
            WHERE application_id = $1 AND COALESCE(is_second, FALSE) = $2
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            app_id,
            is_second,
        )

        if not inspection:
            return jsonify({"error": "Inspección no encontrada"}), 404

        return jsonify({
            "inspection_id": inspection["id"],
            "is_second": is_second,
        }), 200


# ---------------------------------------------------------------------------
# 5. Steps de la aplicación (para render de la pantalla)
# ---------------------------------------------------------------------------

@inspections_bp.route("/applications/<int:app_id>/steps", methods=["GET"])
async def list_steps_for_application(app_id: int):
    """
    Devuelve pasos (steps) configurados para el taller de esa aplicación.
    Esto alimenta la lista principal en el front.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        ws_id = await conn.fetchval(
            "SELECT workshop_id FROM applications WHERE id = $1",
            app_id,
        )
        if not ws_id:
            return jsonify({"error": "Application no encontrada"}), 404

        rows = await conn.fetch(
            """
            SELECT s.id AS step_id,
                   s.name,
                   s.description,
                   so.number AS order_index
            FROM steps_order so
            JOIN steps s ON s.id = so.step_id
            WHERE so.workshop_id = $1
            ORDER BY so.number ASC
            """,
            ws_id,
        )

    return jsonify([
        {
            "step_id": r["step_id"],
            "name": r["name"],
            "description": r["description"],
            "order": r["order_index"],
        }
        for r in rows
    ]), 200


# ---------------------------------------------------------------------------
# 5. Guardar estados (Apto / Condicional / Rechazado) de TODOS los pasos
# ---------------------------------------------------------------------------

@inspections_bp.route("/inspections/<int:inspection_id>/details/bulk", methods=["POST"])
async def bulk_upsert_inspection_details(inspection_id: int):
    """
    Guarda/actualiza el estado por paso (Apto/Condicional/Rechazado).
    Body:
      {
        "items": [
          { "step_id": 1, "status": "Apto", "observations": "" },
          ...
        ]
      }

    observations (campo string por paso) hoy lo mandamos "" desde el front nuevo;
    el texto final está en global_observations a nivel inspección.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    items = payload.get("items") or []

    # Si no hay items, no tiramos 400 porque el front puede llamar igual.
    if not items:
        return jsonify({"message": "Sin cambios"}), 200

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        async with conn.transaction():
            out = []
            for it in items:
                step_id = it.get("step_id")
                status = _norm_status(it.get("status"))
                observations = (it.get("observations") or "").strip()

                if not step_id or status not in ALLOWED_STEP_STATUS:
                    raise ValueError("Datos de paso inválidos")

                # validar pertenencia del paso
                belongs = await _step_belongs_to_workshop(
                    conn, int(step_id), int(ws_id)
                )
                if not belongs:
                    raise ValueError(
                        f"El paso {step_id} no corresponde al taller"
                    )

                row = await conn.fetchrow(
                    """
                    INSERT INTO inspection_details
                      (inspection_id, step_id, status, observations)
                    VALUES ($1, $2, $3, NULLIF($4, ''))
                    ON CONFLICT (inspection_id, step_id) DO UPDATE SET
                      status = EXCLUDED.status,
                      observations = COALESCE(
                        NULLIF(EXCLUDED.observations, ''),
                        inspection_details.observations
                      )
                    RETURNING
                      id,
                      inspection_id,
                      step_id,
                      status,
                      observations
                    """,
                    inspection_id,
                    int(step_id),
                    status,
                    observations,
                )
                out.append(
                    {
                        "id": row["id"],
                        "inspection_id": row["inspection_id"],
                        "step_id": row["step_id"],
                        "status": row["status"],
                        "observations": row["observations"],
                    }
                )

            # Si hay 12 items, significa que todos los pasos están completados
            # Actualizamos el estado de la aplicación a 'Emitir CRT'
            
            if (len(items) == 12):
                await conn.execute(
                        """
                        UPDATE applications
                        SET status = 'Emitir CRT'
                        WHERE id = (
                            SELECT application_id
                            FROM inspections
                            WHERE id = $1
                        )
                        """,
                        inspection_id,
                )

    return jsonify({"message": "Detalles guardados", "items": out}), 200


# ---------------------------------------------------------------------------
# 6. GET /inspections/:inspection_id/details
#    (para ver estado general, certificado, PDF, etc.)
# ---------------------------------------------------------------------------

@inspections_bp.route("/inspections/<int:inspection_id>/details", methods=["GET"])
async def list_inspection_details(inspection_id: int):
    """
    Devuelve:
    - patente
    - texto global guardado (global_observations)
    - cada paso con status y observaciones por paso (legacy)

    El front de edición no usa esto todo el tiempo, pero es útil
    para vistas read-only / certificado.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        # patente
        car_row = await conn.fetchrow(
            """
            SELECT c.license_plate
            FROM inspections i
            JOIN applications a ON a.id = i.application_id
            JOIN cars c        ON c.id = a.car_id
            WHERE i.id = $1
            """,
            inspection_id,
        )
        license_plate = car_row["license_plate"] if car_row else None

        # observaciones globales (textarea final)
        global_row = await conn.fetchrow(
            "SELECT global_observations FROM inspections WHERE id = $1",
            inspection_id,
        )
        global_obs = global_row["global_observations"] if global_row else None

        # estado de cada paso
        rows = await conn.fetch(
            """
            SELECT
              so.number AS order_index,
              s.id      AS step_id,
              s.name,
              s.description,
              d.id      AS detail_id,
              d.status,
              d.observations AS detail_observations
            FROM steps_order so
            JOIN steps s
              ON s.id = so.step_id
            LEFT JOIN inspection_details d
              ON d.step_id = s.id
             AND d.inspection_id = $1
            WHERE so.workshop_id = $2
            ORDER BY so.number ASC
            """,
            inspection_id,
            ws_id,
        )

    return jsonify({
        "license_plate": license_plate,
        "global_observations": global_obs,
        "items": [
            {
                "order": r["order_index"],
                "step_id": r["step_id"],
                "name": r["name"],
                "description": r["description"],
                "detail": (
                    {
                        "detail_id": r["detail_id"],
                        "status": r["status"],
                        "observations": r["detail_observations"],
                    }
                    if r["detail_id"] is not None
                    else None
                ),
            }
            for r in rows
        ],
    }), 200


# ---------------------------------------------------------------------------
# 7. Endpoints jerárquicos NUEVOS para el modal (2 niveles):
#
#   1) GET /inspections/:inspection_id/steps/:step_id/categories
#        → lista las categorías disponibles para ese paso
#
#   2) GET /inspections/:inspection_id/steps/:step_id/categories/:category_id/observations
#        → lista las observaciones finales (ítems hoja) de esa categoría
#
# IMPORTANTE:
# - Ya NO usamos más subcategorías en el front (no hay tercer paso).
# - Estos endpoints NO guardan nada, sólo devuelven catálogo.
# ---------------------------------------------------------------------------

@inspections_bp.route(
    "/inspections/<int:inspection_id>/steps/<int:step_id>/categories",
    methods=["GET"],
)
async def list_step_categories(inspection_id: int, step_id: int):
    """
    Primer nivel del modal.

    Devuelve categorías que estén definidas en DEFAULT_TREE para este paso,
    incluso si no tienen observaciones activas.

    [
      { "category_id": 100, "name": "Luces traseras" },
      { "category_id": 101, "name": "Luces frontales" },
      { "category_id": 102, "name": "Frenos" }
    ]
    """
    from app.routes.workshops import DEFAULT_TREE, SUBCAT_NAME
    
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        # validar que el paso pertenece al taller
        belongs = await _step_belongs_to_workshop(conn, step_id, ws_id)
        if not belongs:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Obtener el nombre del paso
        step_row = await conn.fetchrow(
            "SELECT name FROM steps WHERE id = $1",
            step_id
        )
        if not step_row:
            return jsonify({"error": "Paso no encontrado"}), 404
        
        step_name = step_row["name"]
        
        # Obtener las categorías definidas en DEFAULT_TREE para este paso (para mantener orden)
        categories_for_step = DEFAULT_TREE.get(step_name, {})
        category_names_for_step = list(categories_for_step.keys()) if categories_for_step else []
        
        # Crear un diccionario para mantener el orden original del DEFAULT_TREE
        category_order = {name: idx for idx, name in enumerate(category_names_for_step)}
        
        # Obtener categorías que:
        # 1. Están en DEFAULT_TREE para este paso, O
        # 2. Tienen observaciones (activas o placeholders) para este paso específico
        rows = await conn.fetch(
            """
            SELECT DISTINCT oc.id AS category_id, oc.name AS category_name
            FROM observation_categories oc
            LEFT JOIN observation_subcategories osc ON osc.category_id = oc.id AND osc.name = $4
            LEFT JOIN observations o ON o.subcategory_id = osc.id AND o.step_id = $2
            WHERE oc.workshop_id = $1
              AND (
                oc.name = ANY($3::text[])  -- Está en DEFAULT_TREE para este paso
                OR o.id IS NOT NULL        -- Tiene observaciones (activas o placeholders) para este paso
              )
            ORDER BY oc.id
            """,
            ws_id, step_id, category_names_for_step if category_names_for_step else [], SUBCAT_NAME
        )
        
        # Ordenar: primero las del DEFAULT_TREE (en su orden original), luego las demás (por ID)
        rows_list = list(rows)
        rows_list.sort(key=lambda r: (
            category_order.get(r["category_name"], 999999),  # Las del DEFAULT_TREE primero
            r["category_id"]  # Luego por ID para mantener orden consistente
        ))

    out = [
        {
            "category_id": r["category_id"],
            "name": r["category_name"],
        }
        for r in rows_list
    ]
    return jsonify(out), 200


@inspections_bp.route(
    "/inspections/<int:inspection_id>/steps/<int:step_id>/categories/<int:category_id>/observations",
    methods=["GET"],
)
async def list_category_observations(
    inspection_id: int,
    step_id: int,
    category_id: int
):
    """
    Segundo (y último) nivel del modal.

    Dada una categoría, devolvemos TODAS las observaciones finales activas
    que estén debajo de ESA categoría para ESTE paso en ESTE taller,
    sin pedir subcategoría intermedia.

    Respuesta:
    [
      { "observation_id": 555, "description": "izquierda" },
      { "observation_id": 556, "description": "derecha" },
      { "observation_id": 557, "description": "pastilla gastada" }
    ]
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        # de qué taller es esta inspección
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        # validamos que el paso pertenezca a ese taller
        belongs = await _step_belongs_to_workshop(conn, step_id, ws_id)
        if not belongs:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Traemos TODAS las observaciones activas para:
        #   - ese workshop
        #   - ese step
        #   - cualquier subcategoría que cuelgue de esa categoría
        rows = await conn.fetch(
            """
            SELECT
              o.id          AS observation_id,
              o.description AS observation_desc
            FROM observation_categories oc
            JOIN observation_subcategories osc
              ON osc.category_id = oc.id
            JOIN observations o
              ON o.subcategory_id = osc.id
            WHERE oc.id          = $1        -- la categoría elegida
              AND oc.workshop_id = $2        -- mismo taller
              AND o.workshop_id  = $2
              AND o.step_id      = $3        -- mismo paso
              AND o.is_active    = TRUE
            ORDER BY o.sort_order NULLS LAST, o.id
            """,
            category_id,
            ws_id,
            step_id,
        )

    out = [
        {
            "observation_id": r["observation_id"],
            "description": r["observation_desc"],
        }
        for r in rows
    ]

    return jsonify(out), 200


@inspections_bp.route(
    "/inspections/<int:inspection_id>/steps/<int:step_id>/observations",
    methods=["GET"],
)
async def list_step_observations(
    inspection_id: int,
    step_id: int
):
    """
    Devuelve observaciones sin categoría (subcategory_id IS NULL)
    para este paso en este taller.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        belongs = await _step_belongs_to_workshop(conn, step_id, ws_id)
        if not belongs:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        rows = await conn.fetch(
            """
            SELECT
              o.id          AS observation_id,
              o.description AS observation_desc
            FROM observations o
            WHERE o.workshop_id = $1
              AND o.step_id      = $2
              AND o.subcategory_id IS NULL
              AND o.is_active    = TRUE
            ORDER BY o.id
            """,
            ws_id,
            step_id,
        )

    out = [
        {
            "observation_id": r["observation_id"],
            "description": r["observation_desc"],
        }
        for r in rows
    ]

    return jsonify(out), 200


# ---------------------------------------------------------------------------
# 8. Legacy compat: bulk_set_step_observations
# ---------------------------------------------------------------------------

@inspections_bp.route(
    "/inspections/<int:inspection_id>/steps/<int:step_id>/observations/bulk",
    methods=["POST"],
)
async def bulk_set_step_observations(inspection_id: int, step_id: int):
    """
    LEGACY:
    Antes guardábamos en observation_details cuáles observaciones estaban
    tildadas en un paso.

    El front NUEVO ya NO usa esto.
    Lo dejamos por compatibilidad con flujos viejos.

    Body:
      { "checked_ids": [1,5,9] }
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    checked_ids = payload.get("checked_ids") or []
    if not isinstance(checked_ids, list):
        return jsonify({"error": "Formato inválido, se espera checked_ids: number[]"}), 400

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        belongs = await _step_belongs_to_workshop(conn, step_id, ws_id)
        if not belongs:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # aseguramos inspection_detail legacy
        det_id = await _ensure_inspection_detail(conn, inspection_id, step_id)

        # validar que las observation_ids existen y matchean step y taller
        if checked_ids:
            valid_rows = await conn.fetch(
                """
                SELECT id
                FROM observations
                WHERE id = ANY($1::bigint[])
                  AND workshop_id = $2
                  AND step_id = $3
                """,
                checked_ids,
                ws_id,
                step_id
            )
            valid_set = {r["id"] for r in valid_rows}
            invalid = [x for x in checked_ids if x not in valid_set]
            if invalid:
                return jsonify({
                    "error": f"Observaciones inválidas para el paso, ids: {invalid}"
                }), 400

        async with conn.transaction():
            # borramos las que NO quedaron seleccionadas
            if checked_ids:
                await conn.execute(
                    """
                    DELETE FROM observation_details
                    WHERE inspection_detail_id = $1
                      AND observation_id <> ALL($2::bigint[])
                    """,
                    det_id,
                    checked_ids
                )
            else:
                await conn.execute(
                    """
                    DELETE FROM observation_details
                    WHERE inspection_detail_id = $1
                    """,
                    det_id
                )

            # insertamos las nuevas faltantes
            if checked_ids:
                await conn.execute(
                    """
                    INSERT INTO observation_details (observation_id, inspection_detail_id)
                    SELECT x, $1
                    FROM unnest($2::bigint[]) AS t(x)
                    ON CONFLICT (observation_id, inspection_detail_id) DO NOTHING
                    """,
                    det_id,
                    checked_ids
                )

    return jsonify({"message": "Observaciones guardadas (legacy)"}), 200
