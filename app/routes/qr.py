from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx

qr_bp = Blueprint("qr", __name__)

@qr_bp.route("/get-qr-data/<string:sticker_number>", methods=["GET"])
async def get_qr_data(sticker_number: str):
    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            WITH base AS (
              SELECT
                c.id              AS car_id,
                c.license_plate,
                c.brand,
                c.model,
                c.registration_year,
                s.sticker_number,
                s.status          AS sticker_status,
                w.cuit,
                w.razon_social,
                w.province,
                w.city,
                w.address,
                w.name
              FROM stickers s
              LEFT JOIN cars c           ON c.sticker_id = s.id
              LEFT JOIN sticker_orders so ON so.id = s.sticker_order_id
              LEFT JOIN workshop w        ON w.id = so.workshop_id
              WHERE s.sticker_number = $1
            )
            SELECT
              b.car_id,
              b.license_plate,
              b.brand,
              b.model,
              b.name,
              b.registration_year,
              b.sticker_number,
              b.sticker_status,
              b.cuit,
              b.razon_social,
              b.province,
              b.city,
              b.address,
              la.id      AS application_id,
              la.date    AS application_date,
              la.status  AS application_status,
              la.result  AS application_result
            FROM base b
            LEFT JOIN LATERAL (
              SELECT a.id, a.date, a.status, a.result
              FROM applications a
              WHERE a.car_id = b.car_id
              ORDER BY a.date DESC NULLS LAST, a.id DESC
              LIMIT 1
            ) la ON TRUE
            """,
            sticker_number
        )

    if not row:
        return jsonify({"error": "Sticker no encontrado"}), 404

    car = None
    if row["license_plate"] is not None:
        car = {
            "license_plate": row["license_plate"],
            "brand": row["brand"],
            "model": row["model"],
            "registration_year": row["registration_year"],
        }

    inspection = None
    if row["application_id"] is not None:
        inspection = {
            "application_id": row["application_id"],
            "inspection_date": row["application_date"],
            "status": row["application_status"],
            "result": row["application_result"],
            "expiration_date": None,
        }

    return jsonify({
        "car": car,
        "sticker_number": row["sticker_number"],
        "sticker_status": row["sticker_status"],
        "workshop": {
            "name": row["name"],
            "cuit": row["cuit"],
            "razon_social": row["razon_social"],
            "province": row["province"],
            "city": row["city"],
            "province": row["province"],
            "city": row["city"],
            "address": row["address"],
            "address": row["address"],
        },
        "inspection": inspection
    }), 200
