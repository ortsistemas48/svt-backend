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

@stickers_bp.route("/reassign-sticker", methods=["POST"])
async def reassign_sticker():
    """
    Reasigna la oblea a un auto identificado por su patente tomando la primera
    oblea disponible de una orden dada.

    Body JSON:
      - license_plate: string requerido
      - sticker_order_id: int requerido

    Reglas:
      - Si el auto no existe → 404
      - Si no hay oblea disponible en esa orden → 400
      - Asigna la oblea al auto
      - Si el auto tenía otra oblea distinta → la marca como 'No Disponible'
    """
    data = await request.get_json()
    license_plate_raw = data.get("license_plate")
    sticker_order_id = data.get("sticker_order_id")

    license_plate = _norm_plate(license_plate_raw)
    if not license_plate or not isinstance(sticker_order_id, int):
        return jsonify({"error": "license_plate y sticker_order_id son requeridos"}), 400

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            # 1) Buscar auto
            car_row = await conn.fetchrow(
                """
                SELECT id, sticker_id
                FROM cars
                WHERE UPPER(regexp_replace(license_plate, '[-\\s]', '', 'g')) = $1
                """,
                license_plate,
            )
            if not car_row:
                return jsonify({"error": "Vehículo no encontrado"}), 404

            car_id = car_row["id"]
            old_sticker_id = car_row["sticker_id"]

            # 2) Tomar una oblea disponible de esa orden
            # Criterios:
            # - status 'disponible' (case insensitive)
            # - no expirada
            # - no asignada a ningún auto
            # - orden coincide
            # - FOR UPDATE SKIP LOCKED para concurrencia
            available_row = await conn.fetchrow(
                """
                WITH candidate AS (
                  SELECT s.id
                  FROM stickers s
                  WHERE s.sticker_order_id = $1
                    AND lower(s.status) = 'disponible'
                    AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
                    AND NOT EXISTS (
                      SELECT 1 FROM cars c WHERE c.sticker_id = s.id
                    )
                  ORDER BY s.id ASC
                  LIMIT 1
                )
                SELECT s.id
                FROM stickers s
                JOIN candidate c ON c.id = s.id
                FOR UPDATE SKIP LOCKED
                """,
                sticker_order_id,
            )

            if not available_row:
                return jsonify({"error": "No hay obleas disponibles en esa orden"}), 400

            new_sticker_id = available_row["id"]

            # 3) Asignar al auto
            await conn.execute(
                "UPDATE cars SET sticker_id = $1 WHERE id = $2",
                new_sticker_id, car_id,
            )

            # 4) Marcar la nueva como No Disponible para que no vuelva al pool
            await conn.execute(
                "UPDATE stickers SET status = 'En Uso' WHERE id = $1",
                new_sticker_id,
            )

            # 5) Si había una oblea anterior distinta → marcarla No Disponible
            if old_sticker_id and old_sticker_id != new_sticker_id:
                await conn.execute(
                    "UPDATE stickers SET status = 'No Disponible' WHERE id = $1",
                    old_sticker_id,
                )

    return jsonify({
        "ok": True,
        "car_id": car_id,
        "old_sticker_id": old_sticker_id,
        "new_sticker_id": new_sticker_id,
    }), 200

@stickers_bp.route("/<int:sticker_id>/mark-used", methods=["POST"])
async def mark_sticker_as_used(sticker_id: int):
    async with get_conn_ctx() as conn:
        await conn.execute(
            "UPDATE stickers SET status = 'En Uso' WHERE id = $1",
            sticker_id
        )
        
    return jsonify({"ok": True, "sticker_id": sticker_id, "status": "En Uso"}), 200
