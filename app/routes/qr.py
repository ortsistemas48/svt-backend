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
              li.inspection_id,
              li.expiration_date AS expiration_date,
              li.inspection_created_at,
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
              SELECT i.id AS inspection_id, i.expiration_date, i.created_at AS inspection_created_at, COALESCE(i.is_second, FALSE) AS is_second
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
        
        inspection_created_at = row.get("inspection_created_at")
        if inspection_created_at is not None and hasattr(inspection_created_at, "isoformat"):
            inspection_created_at = inspection_created_at.isoformat()
        
        # Si estamos mostrando la segunda inspección, usar result_2; si no, usar result (primera inspección)
        is_second = bool(row.get("is_second_inspection"))
        print
        inspection_result = row.get("application_result_2") if is_second else row.get("application_result")
        
        inspection = {
            "id": row.get("inspection_id"),
            "application_id": row["application_id"],
            "inspection_date": row["application_date"],
            "status": row["application_status"],
            "result": inspection_result,
            "expiration_date": exp,
            "created_at": inspection_created_at,
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


@qr_bp.route("/get-vehicle-photos/<int:inspection_id>", methods=["GET"])
async def get_vehicle_photos(inspection_id: int):
    """
    Endpoint público para obtener las fotos del vehículo de una inspección.
    Solo devuelve fotos si la inspección pertenece a un sticker válido.
    """
    async with get_conn_ctx() as conn:
        # Verificar que la inspección existe y pertenece a un vehículo con sticker
        row = await conn.fetchrow(
            """
            SELECT i.id
            FROM inspections i
            JOIN applications a ON a.id = i.application_id
            JOIN cars c ON c.id = a.car_id
            JOIN stickers s ON s.id = c.sticker_id
            WHERE i.id = $1
            """,
            inspection_id
        )
        
        if not row:
            return jsonify({"error": "Inspección no encontrada o no válida"}), 404
        
        # Obtener las fotos del vehículo
        photos = await conn.fetch(
            """
            SELECT id, inspection_id, step_id, role,
                   type, file_name, bucket, object_path, file_url,
                   size_bytes, mime_type, created_at,
                   COALESCE(is_front, false) AS is_front
            FROM inspection_documents
            WHERE inspection_id = $1
              AND type = 'vehicle_photo'
            ORDER BY created_at DESC
            """,
            inspection_id
        )
        
        return jsonify([dict(r) for r in photos]), 200
