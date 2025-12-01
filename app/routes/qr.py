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
              la.result  AS application_result,
              la.result_2 AS application_result_2,
              li.expiration_date AS expiration_date,
              li.is_second AS is_second_inspection
            FROM base b
            LEFT JOIN LATERAL (
              SELECT a.id, a.date, a.status, a.result, a.result_2
              FROM applications a
              WHERE a.car_id = b.car_id
              ORDER BY a.date DESC NULLS LAST, a.id DESC
              LIMIT 1
            ) la ON TRUE
            LEFT JOIN LATERAL (
              SELECT i.expiration_date, COALESCE(i.is_second, FALSE) AS is_second
              FROM inspections i
              WHERE i.application_id = la.id
              ORDER BY 
                CASE WHEN COALESCE(i.is_second, FALSE) = TRUE THEN 0 ELSE 1 END,
                i.id DESC
              LIMIT 1
            ) li ON TRUE
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
        exp = row["expiration_date"]
        if exp is not None and hasattr(exp, "isoformat"):
            exp = exp.isoformat()
        
        # Si estamos mostrando la segunda inspección, usar result_2; si no, usar result (primera inspección)
        is_second = bool(row.get("is_second_inspection"))
        print
        inspection_result = row.get("application_result_2") if is_second else row.get("application_result")
        
        inspection = {
            "application_id": row["application_id"],
            "inspection_date": row["application_date"],
            "status": row["application_status"],
            "result": inspection_result,
            "expiration_date": exp,
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
            "address": row["address"],
        },
        "inspection": inspection
    }), 200
