# stickers_bp.py
from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx

stickers_bp = Blueprint("stickers", __name__, url_prefix="/stickers")


def _norm_plate(p: str | None) -> str | None:
    if not p:
        return None
    return p.strip().upper().replace("-", "").replace(" ", "")


@stickers_bp.route("/available", methods=["GET"])
async def list_available_stickers():
    """
    Devuelve SOLO las obleas disponibles (status='Disponible') del taller indicado.
    - No incluye obleas 'En Uso'.
    - Excluye obleas vencidas (si tienen fecha de expiración).
    - Excluye obleas que ya estén asignadas a algún auto (cars.sticker_id IS NOT NULL).
    Parámetros:
      - workshop_id: int (requerido)
    """
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN cars c        ON c.sticker_id = s.id
            WHERE
              so.workshop_id = $1
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND c.id IS NULL                    -- no asignada a ningún auto
            ORDER BY s.id DESC
            """,
            workshop_id,
        )

    # Serializa fechas
    out = []
    for r in rows:
        d = dict(r)
        for k in ("expiration_date", "issued_at"):
            if d.get(k) is not None and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        out.append(d)

    return jsonify(out), 200


@stickers_bp.route("/available-orders", methods=["GET"])
async def list_available_orders():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
              so.id,
              so.name,
              COUNT(s.id) AS available_count
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN cars c        ON c.sticker_id = s.id
            WHERE
              so.workshop_id = $1
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND c.id IS NULL
            GROUP BY so.id, so.name
            ORDER BY so.id DESC
            """,
            workshop_id,
        )

    out = [dict(r) for r in rows]
    print(out)
    return jsonify(out), 200

@stickers_bp.route("/orders", methods=["GET"])
async def list_sticker_orders():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
              so.id,
              so.name,
              so.status,
              so.amount,
              so.created_at,
              COUNT(s.id) AS available_count
            FROM sticker_orders so
            LEFT JOIN stickers s ON s.sticker_order_id = so.id
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND NOT EXISTS (SELECT 1 FROM cars c WHERE c.sticker_id = s.id)
            WHERE so.workshop_id = $1
            GROUP BY so.id, so.name, so.status, so.amount, so.created_at
            ORDER BY so.id DESC
            """,
            workshop_id,
        )

    out = []
    for r in rows:
        d = dict(r)
        if d.get("created_at") is not None and hasattr(d["created_at"], "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        out.append(d)
    return jsonify(out), 200



@stickers_bp.route("/<int:sticker_id>", methods=["GET"])
async def get_sticker(sticker_id: int):
    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status,
              s.sticker_order_id,
              so.workshop_id
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            WHERE s.id = $1
            """,
            sticker_id
        )
    return jsonify(dict(row) if row else None), 200


@stickers_bp.route("/assign-to-car", methods=["POST"])
async def assign_sticker_to_car():
    """
    Body JSON:
      - license_plate: string, requerido
      - sticker_id: int, requerido
      - workshop_id: int, recomendado para validar taller
      - mark_used: bool, opcional, por defecto true, actualiza status='used'
    """
    data = await request.get_json()
    license_plate = _norm_plate(data.get("license_plate"))
    sticker_id = data.get("sticker_id")
    workshop_id = data.get("workshop_id", None)
    mark_used = bool(data.get("mark_used", True))

    if not license_plate or not sticker_id:
        return jsonify({"error": "license_plate y sticker_id son requeridos"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            # Validar que la oblea exista, sea del taller, y no esté en otro auto
            ok = await conn.fetchval(
                """
                SELECT CASE WHEN COUNT(*)>0 THEN true ELSE false END
                FROM stickers s
                JOIN sticker_orders so ON so.id = s.sticker_order_id
                LEFT JOIN cars c        ON c.sticker_id = s.id
                WHERE s.id = $1
                  AND (c.id IS NULL OR c.license_plate = $2)
                  AND ($3::bigint IS NULL OR so.workshop_id = $3)
                """,
                sticker_id, license_plate, workshop_id
            )
            if not ok:
                return jsonify({"error": "Oblea inválida o ya asignada"}), 400

            # Asignar a car por patente, creando o actualizando según exista
            car_id = await conn.fetchval(
                """
                INSERT INTO cars (license_plate, sticker_id)
                VALUES ($1, $2)
                ON CONFLICT (license_plate) DO UPDATE
                  SET sticker_id = EXCLUDED.sticker_id
                RETURNING id
                """,
                license_plate, sticker_id
            )

            # Opcional, marcar status como used
            if mark_used:
                await conn.execute(
                    "UPDATE stickers SET status = 'used' WHERE id = $1",
                    sticker_id
                )

    return jsonify({"ok": True, "car_id": car_id, "sticker_id": sticker_id})


@stickers_bp.route("/unassign-from-car", methods=["POST"])
async def unassign_sticker_from_car():
    """
    Quita la oblea del auto, deja sticker_id en NULL.
    Body JSON:
      - license_plate: string, requerido
      - set_available: bool, opcional, si true pone status='available'
    """
    data = await request.get_json()
    license_plate = _norm_plate(data.get("license_plate"))
    set_available = bool(data.get("set_available", False))

    if not license_plate:
        return jsonify({"error": "license_plate requerido"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            old_sticker = await conn.fetchval(
                "SELECT sticker_id FROM cars WHERE license_plate = $1",
                license_plate
            )

            await conn.execute(
                "UPDATE cars SET sticker_id = NULL WHERE license_plate = $1",
                license_plate
            )

            if set_available and old_sticker:
                await conn.execute(
                    "UPDATE stickers SET status = 'available' WHERE id = $1",
                    old_sticker
                )

    return jsonify({"ok": True})

@stickers_bp.route("/<int:sticker_id>/mark-used", methods=["POST"])
async def mark_sticker_as_used(sticker_id: int):
    async with get_conn_ctx() as conn:
        await conn.execute(
            "UPDATE stickers SET status = 'En Uso' WHERE id = $1",
            sticker_id
        )
        
    return jsonify({"ok": True, "sticker_id": sticker_id, "status": "En Uso"}), 200


@stickers_bp.route("/workshop/<int:workshop_id>", methods=["GET"])
async def get_stickers_by_workshop(workshop_id: int):
    """
    Devuelve todas las obleas (stickers) de un taller específico con paginación.
    Verifica que las obleas pertenezcan al taller a través de sticker_orders.
    Incluye la patente del auto asignado (si existe).
    Parámetros:
      - workshop_id: int (en la URL)
      - page: int (opcional, por defecto 1)
      - per_page: int (opcional, por defecto 20, máximo 100)
    Respuesta:
      - Objeto con lista de obleas y metadatos de paginación
      - license_plate: null si la oblea no está asignada a ningún auto
    """
    # Obtener parámetros de paginación
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    
    # Validar parámetros
    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 20
    if per_page > 100:
        per_page = 100
    
    # Calcular offset
    offset = (page - 1) * per_page

    async with get_conn_ctx() as conn:
        # Obtener total de registros
        total_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            WHERE so.workshop_id = $1
            """,
            workshop_id,
        )
        
        # Obtener registros paginados
        rows = await conn.fetch(
            """
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status,
              s.sticker_order_id,
              so.name as order_name,
              so.workshop_id,
              c.license_plate
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN cars c ON c.sticker_id = s.id
            WHERE so.workshop_id = $1
            ORDER BY s.id DESC
            LIMIT $2 OFFSET $3
            """,
            workshop_id, per_page, offset,
        )

    # Serializa fechas
    stickers = []
    for r in rows:
        d = dict(r)
        for k in ("expiration_date", "issued_at"):
            if d.get(k) is not None and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        stickers.append(d)

    # Calcular metadatos de paginación
    total_pages = (total_count + per_page - 1) // per_page
    has_next = page < total_pages
    has_prev = page > 1

    response = {
        "stickers": stickers,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total_count,
            "total_pages": total_pages,
            "has_next": has_next,
            "has_prev": has_prev,
            "next_page": page + 1 if has_next else None,
            "prev_page": page - 1 if has_prev else None
        }
    }

    return jsonify(response), 200


@stickers_bp.route("/next-available", methods=["GET"])
async def get_next_available_sticker():
    """
    Devuelve la próxima oblea disponible (la que tomaría el sistema)
    para una orden dada, sin asignarla.
    Parámetros:
      - sticker_order_id: int requerido
    Respuesta:
      - { id, sticker_number } o 404 si no hay
    """
    sticker_order_id = request.args.get("sticker_order_id", type=int)
    if not isinstance(sticker_order_id, int):
        return jsonify({"error": "sticker_order_id requerido"}), 400

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT s.id, s.sticker_number
            FROM stickers s
            WHERE s.sticker_order_id = $1
              AND lower(s.status) = 'disponible'
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND NOT EXISTS (SELECT 1 FROM cars c WHERE c.sticker_id = s.id)
            ORDER BY s.id ASC
            LIMIT 1
            """,
            sticker_order_id
        )

    if not row:
        return jsonify({"error": "No hay obleas disponibles en esa orden"}), 404

    return jsonify({"id": row["id"], "sticker_number": row["sticker_number"]}), 200
