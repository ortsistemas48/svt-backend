from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx

qr_bp = Blueprint("qr", __name__)

@qr_bp.route("/get-qr-data/<string:sticker_number>", methods=["GET"])
async def get_qr_data(sticker_number: str):
    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT 
                -- Car data
                c.license_plate,
                c.brand,
                c.model,
                c.registration_year,
                -- Sticker data
                s.sticker_number,
                -- Workshop data
                w.cuit,
                w.razon_social,
                w.plant_number
            FROM stickers s
            LEFT JOIN cars c ON c.sticker_id = s.id
            LEFT JOIN sticker_orders so ON so.id = s.sticker_order_id
            LEFT JOIN workshop w ON w.id = so.workshop_id
            WHERE s.sticker_number = $1
            """,
            sticker_number
        )
    if not row:
        return jsonify({"error": "Sticker no encontrado"}), 404

    # Check if car exists
    car = None
    if row["license_plate"] is not None:
        car = {
            "license_plate": row["license_plate"],
            "brand": row["brand"],
            "model": row["model"],
            "registration_year": row["registration_year"]
        }

    return jsonify({
        "car": car,
        "sticker_number": row["sticker_number"],
        "workshop": {
            "cuit": row["cuit"],
            "razon_social": row["razon_social"],
            "plant_number": row["plant_number"]
        }
    }), 200