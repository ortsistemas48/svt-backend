
from quart import Blueprint, jsonify
from app.db import get_conn_ctx

vehicles_bp = Blueprint("vehicles", __name__, url_prefix="/vehicles")

@vehicles_bp.route("/get-vehicle-data/<string:license_plate>", methods=["GET"])
async def get_vehicle_data(license_plate: str):
    """
    Devuelve SOLO la información del auto (tabla cars) para la patente dada.
    No incluye joins ni datos de owner/driver.
    """
    async with get_conn_ctx() as conn:
        # Normalizamos la comparación por si la patente viene en minúsculas
        row = await conn.fetchrow(
            """
            SELECT
                id,
                license_plate,
                brand,
                model,
                fuel_type,
                weight,
                manufacture_year,
                engine_brand,
                engine_number,
                chassis_number,
                chassis_brand,
                green_card_number,
                green_card_expiration,
                license_number,
                license_expiration,
                vehicle_type,
                usage_type,
                owner_id,
                driver_id,
                sticker_id
            FROM cars
            WHERE LOWER(license_plate) = LOWER($1)
            """,
            license_plate.strip(),
        )

        if not row:
            return jsonify({"error": "Vehículo no encontrado"}), 404

        # Serializamos fechas a ISO (Quart/JSON no serializa date/datetime nativo)
        car = dict(row)
        for k in ("green_card_expiration", "license_expiration"):
            if car.get(k) is not None and hasattr(car[k], "isoformat"):
                car[k] = car[k].isoformat()

        return jsonify(car), 200
