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
    workshop_id = request.args.get("workshop_id", type=int)
    current_car_id = request.args.get("current_car_id", type=int)
    current_plate = _norm_plate(request.args.get("current_license_plate"))

    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            -- 1) Obleas disponibles del taller (vigentes y no asignadas a otro auto)
            SELECT
              s.id,
              s.sticker_number,
              s.expiration_date,
              s.issued_at,
              s.status
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            WHERE
              so.workshop_id = $1
              AND (s.status IS NULL OR s.status IN ('vigente'))
              AND (s.expiration_date IS NULL OR s.expiration_date >= CURRENT_DATE)
              AND NOT EXISTS (
                SELECT 1
                FROM cars c
                WHERE c.sticker_id = s.id
                  AND NOT (
                        ($2::int  IS NOT NULL AND c.id = $2) OR
                        ($3::text IS NOT NULL AND UPPER(regexp_replace(c.license_plate, '[-\\s]', '', 'g')) = $3)
                  )
              )

            UNION

            -- 2) La oblea actualmente asignada al auto (de cualquier taller)
            SELECT
              s2.id,
              s2.sticker_number,
              s2.expiration_date,
              s2.issued_at,
              s2.status
            FROM stickers s2
            WHERE EXISTS (
              SELECT 1
              FROM cars c2
              WHERE c2.sticker_id = s2.id
                AND (
                      ($2::int  IS NOT NULL AND c2.id = $2) OR
                      ($3::text IS NOT NULL AND UPPER(regexp_replace(c2.license_plate, '[-\\s]', '', 'g')) = $3)
                    )
            )

            ORDER BY id DESC
            """,
            workshop_id, current_car_id, current_plate
        )

    # Serializamos fechas si vienen como date/datetime
    out = []
    for r in rows:
        d = dict(r)
        for k in ("expiration_date", "issued_at"):
            if d.get(k) is not None and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
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
