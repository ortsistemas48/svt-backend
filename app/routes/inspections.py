# app/routes/inspections.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx

inspections_bp = Blueprint("inspections", __name__)

ALLOWED_STEP_STATUS = {"Apto", "Condicional", "Rechazado"}


def _norm_status(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    return s[:1].upper() + s[1:].lower()


@inspections_bp.route("/inspections", methods=["POST"])
async def create_inspection():
    user_id = g.get("user_id")
    print(user_id)
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()
    app_id = data.get("application_id")
    if not app_id:
        return jsonify({"error": "Falta application_id"}), 400

    async with get_conn_ctx() as conn:
        app_row = await conn.fetchrow(
            "SELECT id FROM applications WHERE id = $1",
            int(app_id),
        )
        if not app_row:
            return jsonify({"error": "Application no encontrada"}), 404

        existing = await conn.fetchrow(
            """
            SELECT id, application_id, user_id
            FROM inspections
            WHERE application_id = $1
            ORDER BY id ASC
            LIMIT 1
            """,
            int(app_id),
        )
        if existing:
            return jsonify(
                {
                    "message": "Inspección ya existente",
                    "inspection_id": existing["id"],
                }
            ), 200

        row = await conn.fetchrow(
            """
            INSERT INTO inspections (application_id, user_id)
            VALUES ($1, $2)
            RETURNING id
            """,
            int(app_id),
            user_id,
        )

        await conn.execute(
            "UPDATE applications SET status = $1 WHERE id = $2",
            "En curso",
            int(app_id),
        )

    return jsonify(
        {
            "message": "Inspección creada",
            "inspection_id": row["id"],
        }
    ), 201
    
    
@inspections_bp.route("/inspections/<int:inspection_id>", methods=["PUT"])
async def update_inspection(inspection_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    sets = []
    values = []
    idx = 1

    # opcional, seguir permitiendo cambiar el user_id
    if "user_id" in data and data["user_id"] is not None:
        sets.append(f"user_id = ${idx}")
        values.append(data["user_id"])
        idx += 1

    # nuevo, observaciones globales
    if "global_observations" in data:
        go = (data.get("global_observations") or "").strip()
        if len(go) > 400:
            return jsonify({"error": "Observaciones globales supera 400 caracteres"}), 400
        sets.append(f"global_observations = NULLIF(${idx}, '')")
        values.append(go)
        idx += 1

    if not sets:
        return jsonify({"error": "No hay datos para actualizar"}), 400

    values.append(inspection_id)

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM inspections WHERE id = $1", inspection_id
        )
        if not exists:
            return jsonify({"error": "Inspección no encontrada"}), 404

        await conn.execute(
            f"UPDATE inspections SET {', '.join(sets)} WHERE id = ${idx}",
            *values
        )

    return jsonify({"message": "Inspección actualizada"}), 200



@inspections_bp.route("/inspections/<int:inspection_id>", methods=["DELETE"])
async def delete_inspection(inspection_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM inspections WHERE id = $1", inspection_id
        )
        if not exists:
            return jsonify({"error": "Inspección no encontrada"}), 404

        async with conn.transaction():
            await conn.execute(
                "DELETE FROM inspection_details WHERE inspection_id = $1",
                inspection_id,
            )
            await conn.execute(
                "DELETE FROM inspections WHERE id = $1",
                inspection_id,
            )

    return jsonify({"message": "Inspección eliminada"}), 200


async def _get_workshop_for_inspection(conn, inspection_id: int) -> int | None:
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


@inspections_bp.route("/applications/<int:app_id>/steps", methods=["GET"])
async def list_steps_for_application(app_id: int):
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
            SELECT s.id AS step_id, s.name, s.description, so.number AS order_index
            FROM steps_order so
            JOIN steps s ON s.id = so.step_id
            WHERE so.workshop_id = $1
            ORDER BY so.number ASC
            """,
            ws_id,
        )

    return jsonify(
        [
            {
                "step_id": r["step_id"],
                "name": r["name"],
                "description": r["description"],
                "order": r["order_index"],
            }
            for r in rows
        ]
    ), 200


@inspections_bp.route("/inspections/<int:inspection_id>/details", methods=["POST"])
async def upsert_inspection_detail(inspection_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    step_id = data.get("step_id")
    status = _norm_status(data.get("status"))
    observations = (data.get("observations") or "").strip()

    if not step_id:
        return jsonify({"error": "Falta step_id"}), 400
    if status not in ALLOWED_STEP_STATUS:
        return jsonify({"error": f"Estado inválido, use {', '.join(sorted(ALLOWED_STEP_STATUS))}"}), 400

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        belongs = await _step_belongs_to_workshop(conn, int(step_id), int(ws_id))
        if not belongs:
            return jsonify({"error": "El paso no corresponde al taller de la aplicación"}), 400

        row = await conn.fetchrow(
            """
            INSERT INTO inspection_details (inspection_id, step_id, status, observations)
            VALUES ($1, $2, $3, NULLIF($4, ''))
            ON CONFLICT (inspection_id, step_id) DO UPDATE SET
              status = EXCLUDED.status,
              observations = COALESCE(NULLIF(EXCLUDED.observations, ''), inspection_details.observations)
            RETURNING id, inspection_id, step_id, status, observations
            """,
            inspection_id,
            int(step_id),
            status,
            observations,
        )

    return jsonify(
        {
            "message": "Detalle guardado",
            "detail": {
                "id": row["id"],
                "inspection_id": row["inspection_id"],
                "step_id": row["step_id"],
                "status": row["status"],
                "observations": row["observations"],
            },
        }
    ), 200


@inspections_bp.route("/inspections/<int:inspection_id>/details/bulk", methods=["POST"])
async def bulk_upsert_inspection_details(inspection_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    items = payload.get("items") or []
    if not items:
        return jsonify({"error": "No hay items para guardar."}), 400

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

                belongs = await _step_belongs_to_workshop(conn, int(step_id), int(ws_id))
                if not belongs:
                    raise ValueError(f"El paso {step_id} no corresponde al taller")

                row = await conn.fetchrow(
                    """
                    INSERT INTO inspection_details (inspection_id, step_id, status, observations)
                    VALUES ($1, $2, $3, NULLIF($4, ''))
                    ON CONFLICT (inspection_id, step_id) DO UPDATE SET
                      status = EXCLUDED.status,
                      observations = COALESCE(NULLIF(EXCLUDED.observations, ''), inspection_details.observations)
                    RETURNING id, inspection_id, step_id, status, observations
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

    return jsonify({"message": "Detalles guardados", "items": out}), 200


@inspections_bp.route("/inspections/<int:inspection_id>/details", methods=["GET"])
async def list_inspection_details(inspection_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        # dominio del auto
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

        # observación global
        global_row = await conn.fetchrow(
            "SELECT global_observations FROM inspections WHERE id = $1",
            inspection_id,
        )
        global_obs = global_row["global_observations"] if global_row else None

        rows = await conn.fetch(
            """
            SELECT
              so.number AS order_index,
              s.id      AS step_id,
              s.name,
              s.description,
              d.id      AS detail_id,
              d.status,
              d.observations AS detail_observations,
              COALESCE(
                json_agg(
                  DISTINCT jsonb_build_object(
                    'id',          o.id,
                    'description', o.description,
                    'checked',     (od.id IS NOT NULL)
                  )
                ) FILTER (WHERE o.id IS NOT NULL),
                '[]'::json
              ) AS obs_list
            FROM steps_order so
            JOIN steps s
              ON s.id = so.step_id
            LEFT JOIN inspection_details d
              ON d.step_id = s.id
             AND d.inspection_id = $1
            LEFT JOIN observations o
              ON o.step_id = s.id
             AND o.workshop_id = $2
            LEFT JOIN observation_details od
              ON od.observation_id = o.id
             AND od.inspection_detail_id = d.id
            WHERE so.workshop_id = $2
            GROUP BY so.number, s.id, s.name, s.description, d.id, d.status, d.observations
            ORDER BY so.number ASC
            """,
            inspection_id,
            ws_id,
        )

    return jsonify({
        "license_plate": license_plate,  # dominio del auto
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
                "observations": r["obs_list"],
            }
            for r in rows
        ],
    }), 200


async def _ensure_inspection_detail(conn, inspection_id: int, step_id: int) -> int:
    det_id = await conn.fetchval(
        """
        SELECT id FROM inspection_details
        WHERE inspection_id = $1 AND step_id = $2
        """,
        inspection_id, step_id
    )
    if det_id:
        return det_id
    # Si no existe, creamos el detalle con un status por defecto, por ejemplo "Condicional"
    row = await conn.fetchrow(
        """
        INSERT INTO inspection_details (inspection_id, step_id, status, observations)
        VALUES ($1, $2, $3, NULL)
        RETURNING id
        """,
        inspection_id, step_id, "Condicional"
    )
    return row["id"]


@inspections_bp.route("/inspections/<int:inspection_id>/steps/<int:step_id>/observations", methods=["GET"])
async def list_step_observations(inspection_id: int, step_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        ws_id = await _get_workshop_for_inspection(conn, inspection_id)
        if not ws_id:
            return jsonify({"error": "Inspección no encontrada"}), 404

        # Verifica que el step pertenezca al workshop
        belongs = await _step_belongs_to_workshop(conn, step_id, ws_id)
        if not belongs:
            return jsonify({"error": "El paso no corresponde al taller de la aplicación"}), 400

        # Puede o no existir inspection_detail todavía
        det_id = await conn.fetchval(
            "SELECT id FROM inspection_details WHERE inspection_id = $1 AND step_id = $2",
            inspection_id, step_id
        )

        rows = await conn.fetch(
            """
            SELECT
              o.id,
              o.description,
              CASE WHEN od.id IS NULL THEN FALSE ELSE TRUE END AS checked
            FROM observations o
            LEFT JOIN observation_details od
              ON od.observation_id = o.id
             AND od.inspection_detail_id = $3
            WHERE o.workshop_id = $1
              AND o.step_id = $2
            ORDER BY o.id
            """,
            ws_id, step_id, det_id
        )

    return jsonify([{"id": r["id"], "description": r["description"], "checked": r["checked"]} for r in rows]), 200


@inspections_bp.route("/inspections/<int:inspection_id>/steps/<int:step_id>/observations/bulk", methods=["POST"])
async def bulk_set_step_observations(inspection_id: int, step_id: int):
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
            return jsonify({"error": "El paso no corresponde al taller de la aplicación"}), 400

        # Asegura que exista el inspection_detail
        det_id = await _ensure_inspection_detail(conn, inspection_id, step_id)

        # Valida que las observations pertenezcan al taller y paso
        if checked_ids:
            valid_ids = await conn.fetch(
                """
                SELECT id FROM observations
                WHERE id = ANY($1::bigint[])
                  AND workshop_id = $2
                  AND step_id = $3
                """,
                checked_ids, ws_id, step_id
            )
            valid_set = {r["id"] for r in valid_ids}
            invalid = [x for x in checked_ids if x not in valid_set]
            if invalid:
                return jsonify({"error": f"Observaciones inválidas para el paso, ids: {invalid}"}), 400

        async with conn.transaction():
            # Limpia los no seleccionados
            if checked_ids:
                await conn.execute(
                    """
                    DELETE FROM observation_details
                    WHERE inspection_detail_id = $1
                      AND observation_id <> ALL($2::bigint[])
                    """,
                    det_id, checked_ids
                )
            else:
                # Si nada marcado, borra todo
                await conn.execute(
                    "DELETE FROM observation_details WHERE inspection_detail_id = $1",
                    det_id
                )

            # Inserta los seleccionados que falten
            if checked_ids:
                await conn.execute(
                    """
                    INSERT INTO observation_details (observation_id, inspection_detail_id)
                    SELECT x, $1 FROM unnest($2::bigint[]) AS t(x)
                    ON CONFLICT (observation_id, inspection_detail_id) DO NOTHING
                    """,
                    det_id, checked_ids
                )

    return jsonify({"message": "Observaciones guardadas"}), 200
