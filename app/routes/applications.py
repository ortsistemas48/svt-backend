from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
import uuid
import datetime
from dateutil import parser

applications_bp = Blueprint("applications", __name__)

# Paso 1: Crear trámite vacío vinculado al user actual
@applications_bp.route("/applications", methods=["POST"])
async def create_application():
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()
    workshop_id = data.get("workshop_id")

    if not workshop_id:
        return jsonify({"error": "Falta el workshop_id"}), 400

    async with get_conn_ctx() as conn:
        result = await conn.fetchrow("""
            INSERT INTO applications (user_id, workshop_id, date)
            VALUES ($1, $2, $3)
            RETURNING id
        """, user_id, int(workshop_id), datetime.datetime.utcnow())

    application_id = result["id"]

    return jsonify({"message": "Trámite iniciado", "application_id": application_id}), 201


@applications_bp.route("/<app_id>/owner", methods=["PUT"])
async def add_or_update_owner(app_id):
    data = await request.get_json()
    app_id = int(app_id)
    async with get_conn_ctx() as conn:
        owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
        
        if not owner_id:
            owner_id = await conn.fetchval("""
                INSERT INTO persons (first_name, last_name, email, phone_number, street, city, province, dni, is_owner)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
                RETURNING id
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), data.get("dni"))
            await conn.execute("UPDATE applications SET owner_id = $1 WHERE id = $2", owner_id, app_id)
        else:
            await conn.execute("""
                UPDATE persons
                SET first_name = $1, last_name = $2, email = $3, phone_number = $4, street = $5,
                    city = $6, province = $7, dni = $8, is_owner = TRUE
                WHERE id = $9
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), data.get("dni"), owner_id)

    return jsonify({"message": "Titular guardado"}), 200


@applications_bp.route("/<app_id>/driver", methods=["PUT"])
async def add_or_update_driver(app_id):
    data = await request.get_json()
    is_same = data.get("is_same_person", False)
    app_id = int(app_id)
    async with get_conn_ctx() as conn:

        if is_same:
            owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
            if not owner_id:
                await conn.close()
                return jsonify({"error": "Primero debe cargarse el titular (owner)"}), 400

            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", owner_id, app_id)
            await conn.close()
            return jsonify({"message": "Conductor asignado como titular"}), 200

        owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
        driver_id = await conn.fetchval("SELECT driver_id FROM applications WHERE id = $1", app_id)

        # Si no hay driver o es igual al owner, se crea uno nuevo
        if not driver_id or driver_id == owner_id:
            driver_id = await conn.fetchval("""
                INSERT INTO persons (first_name, last_name, email, phone_number, street, city, province, dni, is_owner)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE)
                RETURNING id
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), data.get("dni"))

            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", driver_id, app_id)

        else:
            # Actualiza al conductor si ya existe y es diferente del owner
            await conn.execute("""
                UPDATE persons
                SET first_name = $1, last_name = $2, email = $3, phone_number = $4, street = $5,
                    city = $6, province = $7, dni = $8, is_owner = FALSE
                WHERE id = $9
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), data.get("dni"), driver_id)

    return jsonify({"message": "Conductor guardado"}), 200


# Paso 4: Agregar o editar auto (relacionado con owner y driver actuales)
@applications_bp.route("/<app_id>/car", methods=["PUT"])
async def add_or_update_car(app_id):
    data = await request.get_json()
    app_id = int(app_id)  
    async with get_conn_ctx() as conn:

        car_id = await conn.fetchval("SELECT car_id FROM applications WHERE id = $1", app_id)
        owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
        driver_id = await conn.fetchval("SELECT driver_id FROM applications WHERE id = $1", app_id)
        
        green_card_expiration = (
            parser.parse(data["green_card_expiration"]).date()
            if "green_card_expiration" in data and data["green_card_expiration"]
            else None
        )
        license_expiration = (
            parser.parse(data["license_expiration"]).date()
            if "license_expiration" in data and data["license_expiration"]
            else None
        )

        if not owner_id or not driver_id:
            await conn.close()
            return jsonify({"error": "Faltan owner o driver. Deben agregarse antes"}), 400

        if not car_id:
            car_id = await conn.fetchval("""
                INSERT INTO cars (
                    license_plate, brand, model, fuel_type, weight,
                    manufacture_year, engine_brand, engine_number,
                    chassis_number, chassis_brand, green_card_number,
                    green_card_expiration, license_number, license_expiration,
                    vehicle_type, usage_type,
                    owner_id, driver_id
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8,
                    $9, $10, $11,
                    $12, $13, $14,
                    $15, $16, $17,
                    $18
                )
                RETURNING id
            """, data.get("license_plate"), data.get("brand"), data.get("model"), data.get("fuel_type"), data.get("weight"),
                data.get("manufacture_year"), data.get("engine_brand"), data.get("engine_number"),
                data.get("chassis_number"), data.get("chassis_brand"), data.get("green_card_number"),
                green_card_expiration, data.get("license_number"), license_expiration,
                data.get("vehicle_type"), data.get("usage_type"), 
                owner_id, driver_id)

            await conn.execute("UPDATE applications SET car_id = $1 WHERE id = $2", car_id, app_id)

        else:
            await conn.execute("""
                UPDATE cars SET
                    license_plate = $1,
                    brand = $2,
                    model = $3,
                    fuel_type = $4,
                    weight = $5,
                    manufacture_year = $6,
                    engine_brand = $7,
                    engine_number = $8,
                    chassis_number = $9,
                    chassis_brand = $10,
                    green_card_number = $11,
                    green_card_expiration = $12,
                    license_number = $13,
                    license_expiration = $14,
                    vehicle_type = $15,
                    usage_type = $16,
                    owner_id = $17,
                    driver_id = $18
                WHERE id = $19
            """, data.get("license_plate"), data.get("brand"), data.get("model"), data.get("fuel_type"), data.get("weight"),
                data.get("manufacture_year"), data.get("engine_brand"), data.get("engine_number"),
                data.get("chassis_number"), data.get("chassis_brand"), data.get("green_card_number"),
                green_card_expiration, data.get("license_number"), license_expiration,
                data.get("vehicle_type"), data.get("usage_type"),
                owner_id, driver_id, car_id)

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

    async with get_conn_ctx() as conn:
        await conn.execute(query, *valores)

    return jsonify({"message": "Trámite actualizado"}), 200


# Eliminar trámite
@applications_bp.route("/applications/<app_id>", methods=["DELETE"])
async def delete_application(app_id):
    async with get_conn_ctx() as conn:
        await conn.execute("DELETE FROM applications WHERE id = $1", app_id)
    return jsonify({"message": "Trámite eliminado"}), 200


@applications_bp.route("/get-applications/<int:id>", methods=["GET"])
async def get_application(id):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        application = await conn.fetchrow("""
            SELECT id, user_id, date
            FROM applications
            WHERE id = $1 AND user_id = $2
        """, id, user_id)

    if not application:
        return jsonify({"error": "Trámite no encontrado"}), 404

    return jsonify(dict(application)), 200


@applications_bp.route("/<int:id>/data", methods=["GET"])
async def get_application_full(id):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        application = await conn.fetchrow("""
            SELECT id, owner_id, driver_id, car_id
            FROM applications
            WHERE id = $1 AND user_id = $2
        """, id, user_id)

        if not application:
            await conn.close()
            return jsonify({"error": "Trámite no encontrado"}), 404

        # Traer owner, driver y car si existen
        owner = None
        driver = None
        car = None

        if application["owner_id"]:
            owner = await conn.fetchrow("SELECT * FROM persons WHERE id = $1", application["owner_id"])
        if application["driver_id"]:
            driver = await conn.fetchrow("SELECT * FROM persons WHERE id = $1", application["driver_id"])
        if application["car_id"]:
            car = await conn.fetchrow("SELECT * FROM cars WHERE id = $1", application["car_id"])


    return jsonify({
        "application_id": application["id"],
        "owner": dict(owner) if owner else None,
        "driver": dict(driver) if driver else None,
        "car": dict(car) if car else None
    }), 200

from quart import request, jsonify, g

@applications_bp.route("/workshop/<int:workshop_id>/full", methods=["GET"])
async def list_full_applications_by_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    # --- Query params ---
    try:
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
        page = max(1, page)
        per_page = max(1, min(per_page, 100))  # cap de seguridad
    except ValueError:
        return jsonify({"error": "Parámetros inválidos"}), 400

    q = (request.args.get("q") or "").strip()
    status_in_raw = (request.args.get("status_in") or "").strip()
    status_list = [s.strip() for s in status_in_raw.split(",") if s.strip()]
    offset = (page - 1) * per_page

    # --- Filtros dinámicos ---
    filters = ["a.workshop_id = $1"]
    params = [workshop_id]

    # Búsqueda (patrón similar a tu front)
    if q:
        filters.append("""
            (
                c.license_plate ILIKE $2 OR
                o.first_name     ILIKE $2 OR
                o.last_name      ILIKE $2 OR
                o.dni::text      ILIKE $2
            )
        """)
        params.append(f"%{q}%")

    # Filtro de estado opcional: status_in=En Cola,En curso
    if status_list:
        filters.append(f"a.status = ANY(${len(params)+1}::text[])")
        params.append(status_list)

    # ---- Filtro "no vacío" (equivalente a isDataEmpty) ----
    # Se excluyen aplicaciones donde car Y owner están vacíos (ignorando id/is_owner/owner_id/driver_id).
    # Consideramos "no vacío":
    #   car: license_plate/brand/model con texto no vacío
    #   owner: first_name/last_name con texto no vacío o dni no nulo
    filters.append("""
        (
            ( NULLIF(trim(c.license_plate), '') IS NOT NULL
              OR NULLIF(trim(c.brand), '')         IS NOT NULL
              OR NULLIF(trim(c.model), '')         IS NOT NULL )
        )
        OR
        (
            ( NULLIF(trim(o.first_name), '') IS NOT NULL
              OR NULLIF(trim(o.last_name), '')  IS NOT NULL
              OR o.dni IS NOT NULL )
        )
    """)

    where_sql = " AND ".join(f"({f.strip()})" for f in filters)

    async with get_conn_ctx() as conn:
        # --- Total para paginación ---
        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM applications a
            LEFT JOIN persons o ON a.owner_id = o.id
            LEFT JOIN cars    c ON a.car_id   = c.id
            WHERE {where_sql}
            """,
            *params
        )

        # --- Página de items ---
        limit_idx  = len(params) + 1
        offset_idx = len(params) + 2

        rows = await conn.fetch(
            f"""
            SELECT
                a.id,
                a.user_id,
                a.date,
                a.status,
                -- Owner mínimo
                o.first_name  AS owner_first_name,
                o.last_name   AS owner_last_name,
                o.dni         AS owner_dni,
                -- Driver mínimo (si lo necesitás en UI, lo dejamos también)
                d.first_name  AS driver_first_name,
                d.last_name   AS driver_last_name,
                d.dni         AS driver_dni,
                -- Car mínimo
                c.license_plate,
                c.brand,
                c.model
            FROM applications a
            LEFT JOIN persons o ON a.owner_id = o.id
            LEFT JOIN persons d ON a.driver_id = d.id
            LEFT JOIN cars    c ON a.car_id   = c.id
            WHERE {where_sql}
            ORDER BY a.date DESC NULLS LAST
            LIMIT ${limit_idx} OFFSET ${offset_idx}
            """,
            *params, per_page, offset
        )

    items = []
    for r in rows:
        items.append({
            "application_id": r["id"],
            "user_id": r["user_id"],
            "date": r["date"].isoformat() if r["date"] else None,
            "status": r["status"],
            "owner": (
                {
                    "first_name": r["owner_first_name"],
                    "last_name":  r["owner_last_name"],
                    "dni":        r["owner_dni"],
                }
                if (r["owner_first_name"] or r["owner_last_name"] or r["owner_dni"] is not None)
                else None
            ),
            "driver": (
                {
                    "first_name": r["driver_first_name"],
                    "last_name":  r["driver_last_name"],
                    "dni":        r["driver_dni"],
                }
                if (r["driver_first_name"] or r["driver_last_name"] or r["driver_dni"] is not None)
                else None
            ),
            "car": (
                {
                    "license_plate": r["license_plate"],
                    "brand":         r["brand"],
                    "model":         r["model"],
                }
                if (
                    (r["license_plate"] and r["license_plate"].strip())
                    or (r["brand"] and r["brand"].strip())
                    or (r["model"] and r["model"].strip())
                )
                else None
            ),
        })

    return jsonify({
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page
    }), 200



@applications_bp.route("/<int:app_id>/queue", methods=["POST"])
async def enqueue_application(app_id):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    
    async with get_conn_ctx() as conn:
        application = await conn.fetchrow(
            "SELECT id FROM applications WHERE id = $1",
            app_id
        )
        if not application:
            return jsonify({"error": "Trámite no encontrado o sin permiso"}), 404

        await conn.execute(
            "UPDATE applications SET status = $1 WHERE id = $2",
            "En Cola", app_id
        )

    return jsonify({"message": "Trámite enviado a la cola"}), 200

@applications_bp.route("/workshop/<int:workshop_id>/completed", methods=["GET"])
async def list_completed_applications_by_workshop(workshop_id: int):
    """
    Devuelve las applications del workshop con status = 'Completado',
    incluyendo owner, driver y car cuando existan.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        # 1) Traer solo las applications completadas
        applications = await conn.fetch(
            """
            SELECT id, user_id, owner_id, driver_id, car_id, date, status, result
            FROM applications
            WHERE workshop_id = $1
              AND status = 'Completado'
            ORDER BY date DESC
            """,
            workshop_id,
        )

        # 2) Enriquecer con owner/driver/car (mismo patrón que el endpoint full)
        result = []

        for app in applications:
            owner = None
            driver = None
            car = None

            if app["owner_id"]:
                owner = await conn.fetchrow(
                    "SELECT * FROM persons WHERE id = $1", app["owner_id"]
                )
            if app["driver_id"]:
                driver = await conn.fetchrow(
                    "SELECT * FROM persons WHERE id = $1", app["driver_id"]
                )
            if app["car_id"]:
                car = await conn.fetchrow(
                    "SELECT * FROM cars WHERE id = $1", app["car_id"]
                )

            # Estructura compatible con el front actual
            result.append(
                {
                    "application_id": app["id"],
                    "user_id": app["user_id"],
                    "date": app["date"].isoformat() if app["date"] else None,
                    "status": app.get("status"),   # mismo uso que tu endpoint full
                    "result": app.get("result"),
                    # Si más adelante agregás una columna "result" en applications,
                    # esta línea la podés habilitar:
                    # "result": app.get("result"),
                    "owner": dict(owner) if owner else None,
                    "driver": dict(driver) if driver else None,
                    "car": dict(car) if car else None,
                }
            )

    return jsonify(result), 200