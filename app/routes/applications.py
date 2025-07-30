from quart import Blueprint, request, jsonify, g
from app.db import get_conn
import uuid
import datetime

applications_bp = Blueprint("applications", __name__)

# Paso 1: Crear trámite vacío vinculado al user actual
@applications_bp.route("/applications", methods=["POST"])
async def create_application():
    application_id = str(uuid.uuid4())
    user_id = g.get("user_id")

    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn() as conn:
        await conn.execute("""
            INSERT INTO applications (id, user_id, date)
            VALUES ($1, $2, $3)
        """, application_id, user_id, datetime.datetime.utcnow())

    return jsonify({"message": "Trámite iniciado", "application_id": application_id}), 201


# Paso 2: Agregar o editar titular (owner)
@applications_bp.route("/applications/<app_id>/owner", methods=["PUT"])
async def add_or_update_owner(app_id):
    data = await request.get_json()
    async with get_conn() as conn:
        owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)

        if not owner_id:
            owner_id = str(uuid.uuid4())
            await conn.execute("""
                INSERT INTO persons (id, first_name, last_name, email, phone, address, city, province, dni)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, owner_id, data["first_name"], data["last_name"], data["email"], data["phone"],
                 data["address"], data["city"], data["province"], data["dni"])
            await conn.execute("UPDATE applications SET owner_id = $1 WHERE id = $2", owner_id, app_id)
        else:
            await conn.execute("""
                UPDATE persons
                SET first_name = $1, last_name = $2, email = $3, phone = $4, address = $5,
                    city = $6, province = $7, dni = $8
                WHERE id = $9
            """, data["first_name"], data["last_name"], data["email"], data["phone"],
                 data["address"], data["city"], data["province"], data["dni"], owner_id)

    return jsonify({"message": "Titular guardado"}), 200


@applications_bp.route("/applications/<app_id>/driver", methods=["PUT"])
async def add_or_update_driver(app_id):
    data = await request.get_json()
    is_same = data.get("is_same_person", False)

    async with get_conn() as conn:
        if is_same:
            owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
            if not owner_id:
                return jsonify({"error": "Primero debe cargarse el titular (owner)"}), 400

            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", owner_id, app_id)
            return jsonify({"message": "Conductor asignado como titular"}), 200

        # Modo normal (crear o actualizar driver distinto)
        driver_id = await conn.fetchval("SELECT driver_id FROM applications WHERE id = $1", app_id)

        if not driver_id:
            driver_id = str(uuid.uuid4())
            await conn.execute("""
                INSERT INTO persons (id, first_name, last_name, email, phone, address, city, province, dni)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, driver_id, data["first_name"], data["last_name"], data["email"], data["phone"],
                 data["address"], data["city"], data["province"], data["dni"])
            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", driver_id, app_id)
        else:
            await conn.execute("""
                UPDATE persons
                SET first_name = $1, last_name = $2, email = $3, phone = $4, address = $5,
                    city = $6, province = $7, dni = $8
                WHERE id = $9
            """, data["first_name"], data["last_name"], data["email"], data["phone"],
                 data["address"], data["city"], data["province"], data["dni"], driver_id)

    return jsonify({"message": "Conductor guardado"}), 200


# Paso 4: Agregar o editar auto (relacionado con owner y driver actuales)
@applications_bp.route("/applications/<app_id>/car", methods=["PUT"])
async def add_or_update_car(app_id):
    data = await request.get_json()
    async with get_conn() as conn:
        car_id = await conn.fetchval("SELECT car_id FROM applications WHERE id = $1", app_id)
        owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
        driver_id = await conn.fetchval("SELECT driver_id FROM applications WHERE id = $1", app_id)

        if not owner_id or not driver_id:
            return jsonify({"error": "Faltan owner o driver. Deben agregarse antes"}), 400

        if not car_id:
            car_id = str(uuid.uuid4())
            await conn.execute("""
                INSERT INTO cars (id, license_plate, brand, model, fuel_type, weight, owner_id, driver_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, car_id, data["license_plate"], data["brand"], data["model"],
                 data["fuel_type"], data["weight"], owner_id, driver_id)
            await conn.execute("UPDATE applications SET car_id = $1 WHERE id = $2", car_id, app_id)
        else:
            await conn.execute("""
                UPDATE cars
                SET license_plate = $1, brand = $2, model = $3, fuel_type = $4,
                    weight = $5, owner_id = $6, driver_id = $7
                WHERE id = $8
            """, data["license_plate"], data["brand"], data["model"],
                 data["fuel_type"], data["weight"], owner_id, driver_id, car_id)

    return jsonify({"message": "Vehículo guardado"}), 200


# Paso 5: Editar cualquier dato del trámite (application)
@applications_bp.route("/applications/<app_id>", methods=["PUT"])
async def update_application(app_id):
    data = await request.get_json()
    campos = []
    valores = []

    for i, (key, value) in enumerate(data.items(), start=1):
        campos.append(f"{key} = ${i}")
        valores.append(value)

    if not campos:
        return jsonify({"error": "No hay datos para actualizar"}), 400

    query = f"UPDATE applications SET {', '.join(campos)} WHERE id = ${len(valores)+1}"
    valores.append(app_id)

    async with get_conn() as conn:
        await conn.execute(query, *valores)

    return jsonify({"message": "Trámite actualizado"}), 200


# Eliminar trámite
@applications_bp.route("/applications/<app_id>", methods=["DELETE"])
async def delete_application(app_id):
    async with get_conn() as conn:
        await conn.execute("DELETE FROM applications WHERE id = $1", app_id)
    return jsonify({"message": "Trámite eliminado"}), 200
