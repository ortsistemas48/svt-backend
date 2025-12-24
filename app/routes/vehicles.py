
from quart import Blueprint, jsonify
from app.db import get_conn_ctx
import datetime

vehicles_bp = Blueprint("vehicles", __name__, url_prefix="/vehicles")

@vehicles_bp.route("/get-vehicle-data/<string:license_plate>", methods=["GET"])
async def get_vehicle_data(license_plate: str):
    """
    Devuelve SOLO la información del auto (tabla cars) para la patente dada,
    e incluye (si existe) la información COMPLETA del sticker vinculado bajo la
    clave 'sticker'.
    No incluye datos de owner/driver.
    
    Si el vehículo tiene revisiones (applications) con resultado 'Condicional'
    dentro de los últimos 60 días que no han sido continuadas (solo tienen 1 inspección,
    no 2), devuelve un error indicando que debe continuar el trámite en lugar de
    devolver la información del vehículo.
    """
    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                -- Car
                c.id,
                c.license_plate,
                c.brand,
                c.model,
                c.fuel_type,
                c.manufacture_year,
                c.registration_year,
                c.engine_brand,
                c.engine_number,
                c.chassis_number,
                c.chassis_brand,
                c.green_card_number,
                c.green_card_expiration,
                c.license_number,
                c.license_expiration,
                c.vehicle_type,
                c.usage_type,
                c.owner_id,
                c.driver_id,
                c.sticker_id,
                c.total_weight,
                c.front_weight,
                c.back_weight,
                c.license_class,
                c.insurance,
                c.type_ced,
                -- Sticker (LEFT JOIN porque puede no existir)
                s.id                  AS s_id,
                s.sticker_number      AS s_sticker_number,
                s.expiration_date     AS s_expiration_date,
                s.issued_at           AS s_issued_at,
                s.status              AS s_status,
                s.sticker_order_id    AS s_sticker_order_id,
                so.workshop_id        AS s_workshop_id
            FROM cars c
            LEFT JOIN stickers s
              ON s.id = c.sticker_id
            LEFT JOIN sticker_orders so
              ON so.id = s.sticker_order_id
            WHERE LOWER(c.license_plate) = LOWER($1)
            """,
            license_plate.strip(),
        )

        if not row:
            return jsonify({"error": "Vehículo no encontrado"}), 404

        # Verificar si tiene revisiones con resultado Condicional en los últimos 60 días
        # que no hayan sido continuadas (result_2 es NULL)
        # Si result_2 existe, significa que ya se completó la segunda inspección y se puede crear una nueva
        car_id = row["id"]
        sixty_days_ago = datetime.date.today() - datetime.timedelta(days=60)
        
        condicional_app = await conn.fetchrow(
            """
            SELECT a.id
            FROM applications a
            WHERE a.car_id = $1
              AND a.result = 'Condicional'
              AND a.date::date >= $2
              AND a.result_2 IS NULL
            LIMIT 1
            """,
            car_id,
            sixty_days_ago
        )
        print(condicional_app)
        if condicional_app:
            return jsonify({
                "error": "El dominio presenta revisiones con resultado: 'Condicional', tiene que continuar el tramite"
            }), 400

        car = {
            "id": row["id"],
            "license_plate": row["license_plate"],
            "brand": row["brand"],
            "model": row["model"],
            "fuel_type": row["fuel_type"],
            "manufacture_year": row["manufacture_year"],
            "engine_brand": row["engine_brand"],
            "engine_number": row["engine_number"],
            "chassis_number": row["chassis_number"],
            "chassis_brand": row["chassis_brand"],
            "green_card_number": row["green_card_number"],
            "green_card_expiration": row["green_card_expiration"],
            "license_number": row["license_number"],
            "license_expiration": row["license_expiration"],
            "vehicle_type": row["vehicle_type"],
            "usage_type": row["usage_type"],
            "owner_id": row["owner_id"],
            "driver_id": row["driver_id"],
            "sticker_id": row["sticker_id"],
            "total_weight": row["total_weight"],
            "front_weight": row["front_weight"],
            "back_weight": row["back_weight"],
            "insurance": row["insurance"],
            "license_class": row["license_class"],
            "registration_year": row["registration_year"],
            "type_ced": row["type_ced"]
        }

        # Serializar fechas del auto
        for k in ("green_card_expiration", "license_expiration"):
            if car.get(k) is not None and hasattr(car[k], "isoformat"):
                car[k] = car[k].isoformat()

        # Armar objeto sticker si existe
        if row["s_id"] is not None:
            sticker = {
                "id": row["s_id"],
                "sticker_number": row["s_sticker_number"],
                "expiration_date": row["s_expiration_date"],
                "issued_at": row["s_issued_at"],
                "status": row["s_status"],
                "sticker_order_id": row["s_sticker_order_id"],
                "workshop_id": row["s_workshop_id"],
            }
            # Serializar fechas del sticker
            for k in ("expiration_date", "issued_at"):
                if sticker.get(k) is not None and hasattr(sticker[k], "isoformat"):
                    sticker[k] = sticker[k].isoformat()
        else:
            sticker = None

        car["sticker"] = sticker
        return jsonify(car), 200
