from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
import uuid
import datetime
from dateutil import parser
import pytz

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

    # Obtener la hora actual en Argentina (UTC-3)
    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    now_argentina = datetime.datetime.now(argentina_tz)

    async with get_conn_ctx() as conn:
        result = await conn.fetchrow("""
            INSERT INTO applications (user_id, workshop_id, date)
            VALUES ($1, $2, $3)
            RETURNING id
        """, user_id, int(workshop_id), now_argentina)

    application_id = result["id"]

    return jsonify({"message": "Trámite iniciado", "application_id": application_id}), 201


@applications_bp.route("/<app_id>/owner", methods=["PUT"])
async def add_or_update_owner(app_id):
    data = await request.get_json()
    app_id = int(app_id)
    
    # Validar que se proporcione DNI
    dni = data.get("dni")
    if not dni:
        return jsonify({"error": "DNI es requerido"}), 400
    
    async with get_conn_ctx() as conn:
        # Verificar si ya existe una persona con este DNI
        existing_person_id = await conn.fetchval(
            "SELECT id FROM persons WHERE dni = $1", dni
        )
        
        if existing_person_id:
            # Usar la persona existente y actualizar sus datos
            await conn.execute("""
                UPDATE persons
                SET first_name = $1, last_name = $2, email = $3, phone_number = $4, street = $5,
                    city = $6, province = $7, is_owner = TRUE
                WHERE id = $8
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), existing_person_id)
            
            # Asignar la persona existente a la aplicación
            await conn.execute("UPDATE applications SET owner_id = $1 WHERE id = $2", existing_person_id, app_id)
            owner_id = existing_person_id
        else:
            # Crear nueva persona solo si no existe
            owner_id = await conn.fetchval("""
                INSERT INTO persons (first_name, last_name, email, phone_number, street, city, province, dni, is_owner)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
                RETURNING id
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), dni)
            
            await conn.execute("UPDATE applications SET owner_id = $1 WHERE id = $2", owner_id, app_id)

    return jsonify({"message": "Titular guardado", "person_id": owner_id}), 200


@applications_bp.route("/<app_id>/driver", methods=["PUT"])
async def add_or_update_driver(app_id):
    data = await request.get_json()
    is_same = data.get("is_same_person", False)
    app_id = int(app_id)
    
    async with get_conn_ctx() as conn:
        if is_same:
            owner_id = await conn.fetchval("SELECT owner_id FROM applications WHERE id = $1", app_id)
            if not owner_id:
                return jsonify({"error": "Primero debe cargarse el titular (owner)"}), 400

            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", owner_id, app_id)
            return jsonify({"message": "Conductor asignado como titular", "person_id": owner_id}), 200

        # Validar que se proporcione DNI
        dni = data.get("dni")
        if not dni:
            return jsonify({"error": "DNI es requerido"}), 400

        # Verificar si ya existe una persona con este DNI
        existing_person_id = await conn.fetchval(
            "SELECT id FROM persons WHERE dni = $1", dni
        )
        
        if existing_person_id:
            # Usar la persona existente y actualizar sus datos
            await conn.execute("""
                UPDATE persons
                SET first_name = $1, last_name = $2, email = $3, phone_number = $4, street = $5,
                    city = $6, province = $7, is_owner = FALSE
                WHERE id = $8
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), existing_person_id)
            
            # Asignar la persona existente como conductor
            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", existing_person_id, app_id)
            return jsonify({"message": "Conductor guardado", "person_id": existing_person_id}), 200
        else:
            # Crear nueva persona solo si no existe
            driver_id = await conn.fetchval("""
                INSERT INTO persons (first_name, last_name, email, phone_number, street, city, province, dni, is_owner)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE)
                RETURNING id
            """, data.get("first_name"), data.get("last_name"), data.get("email"), data.get("phone"),
                data.get("address"), data.get("city"), data.get("province"), dni)

            await conn.execute("UPDATE applications SET driver_id = $1 WHERE id = $2", driver_id, app_id)
            return jsonify({"message": "Conductor guardado", "person_id": driver_id}), 200


# Paso 4: Agregar o reutilizar auto por patente, luego vincular a la application
@applications_bp.route("/<app_id>/car", methods=["PUT"])
async def add_or_update_car(app_id):
    data = await request.get_json()
    app_id = int(app_id)

    def normalize_plate(p):
        if not p:
            return None
        return str(p).strip().upper().replace("-", "").replace(" ", "")

    license_plate = normalize_plate(data.get("license_plate"))

    # Handle green card expiration - if no_expiration is true, set to None
    green_card_no_expiration = data.get("green_card_no_expiration", False)
    green_card_expiration = None
    
    if not green_card_no_expiration and data.get("green_card_expiration"):
        green_card_expiration = parser.parse(data["green_card_expiration"]).date()

    print(f"green_card_expiration: {green_card_expiration}")
    print(f"green_card_no_expiration: {green_card_no_expiration}")

    license_expiration = (
        parser.parse(data["license_expiration"]).date()
        if data.get("license_expiration") else None
    )

    async with get_conn_ctx() as conn:
        sticker_id = data.get("sticker_id")

        if sticker_id:
            # si applications tiene workshop_id, validemos contra eso
            app_ws_id = await conn.fetchval("SELECT workshop_id FROM applications WHERE id = $1", app_id)

            ok = await conn.fetchval(
                """
                SELECT CASE WHEN COUNT(*) > 0 THEN true ELSE false END
                FROM stickers s
                JOIN sticker_orders so ON so.id = s.sticker_order_id
                LEFT JOIN cars c        ON c.sticker_id = s.id
                WHERE s.id = $1
                AND (c.id IS NULL OR c.license_plate = $2);
                """,
                sticker_id, license_plate
            )
            print(sticker_id, license_plate, app_ws_id)
            if not ok:
                return jsonify({"error": "Oblea inválida o ya asignada"}), 400


        async with conn.transaction():
            row = await conn.fetchrow("""
                SELECT owner_id, driver_id, car_id
                FROM applications
                WHERE id = $1
                FOR UPDATE
            """, app_id)

            if not row or not row["owner_id"] or not row["driver_id"]:
                return jsonify({"error": "Faltan owner o driver, deben agregarse antes"}), 400

            owner_id = row["owner_id"]
            driver_id = row["driver_id"]
            old_car_id = row["car_id"]

            if not license_plate:
                return jsonify({"error": "Falta license_plate"}), 400

            # Obtener el sticker_id actual del car (si existe) - este será el "old sticker"
            old_sticker_id = None
            if old_car_id:
                old_sticker_id = await conn.fetchval(
                    "SELECT sticker_id FROM cars WHERE id = $1", old_car_id
                )

            car_id = await conn.fetchval("""
                INSERT INTO cars (
                license_plate, brand, model, fuel_type, weight,
                manufacture_year, engine_brand, engine_number,
                chassis_number, chassis_brand, green_card_number,
                green_card_expiration, license_number, license_expiration,
                vehicle_type, usage_type, owner_id, driver_id, insurance, sticker_id,
                total_weight, front_weight, back_weight, registration_year, license_class
                )
                VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11,
                $12, $13, $14,
                $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25
                )
                ON CONFLICT (license_plate) DO UPDATE SET
                brand = COALESCE(EXCLUDED.brand, cars.brand),
                model = COALESCE(EXCLUDED.model, cars.model),
                fuel_type = COALESCE(EXCLUDED.fuel_type, cars.fuel_type),
                weight = COALESCE(EXCLUDED.weight, cars.weight),
                manufacture_year = COALESCE(EXCLUDED.manufacture_year, cars.manufacture_year),
                engine_brand = COALESCE(EXCLUDED.engine_brand, cars.engine_brand),
                engine_number = COALESCE(EXCLUDED.engine_number, cars.engine_number),
                chassis_number = COALESCE(EXCLUDED.chassis_number, cars.chassis_number),
                chassis_brand = COALESCE(EXCLUDED.chassis_brand, cars.chassis_brand),
                green_card_number = COALESCE(EXCLUDED.green_card_number, cars.green_card_number),
                green_card_expiration = EXCLUDED.green_card_expiration,
                license_number = COALESCE(EXCLUDED.license_number, cars.license_number),
                license_expiration = COALESCE(EXCLUDED.license_expiration, cars.license_expiration),
                vehicle_type = COALESCE(EXCLUDED.vehicle_type, cars.vehicle_type),
                usage_type = COALESCE(EXCLUDED.usage_type, cars.usage_type),
                owner_id = COALESCE(EXCLUDED.owner_id, cars.owner_id),
                driver_id = COALESCE(EXCLUDED.driver_id, cars.driver_id),
                insurance = COALESCE(EXCLUDED.insurance, cars.insurance),
                sticker_id = COALESCE(EXCLUDED.sticker_id, cars.sticker_id),
                total_weight = COALESCE(EXCLUDED.total_weight, cars.total_weight),
                front_weight = COALESCE(EXCLUDED.front_weight, cars.front_weight),
                back_weight = COALESCE(EXCLUDED.back_weight, cars.back_weight),
                registration_year = COALESCE(EXCLUDED.registration_year, cars.registration_year),
                license_class = COALESCE(EXCLUDED.license_class, cars.license_class)
                RETURNING id
            """,
            license_plate, data.get("brand"), data.get("model"), data.get("fuel_type"), data.get("weight"),
            data.get("manufacture_year"), data.get("engine_brand"), data.get("engine_number"),
            data.get("chassis_number"), data.get("chassis_brand"), data.get("green_card_number"),
            green_card_expiration, data.get("license_number"), license_expiration, data.get("vehicle_type"), 
            data.get("usage_type"), owner_id, driver_id, data.get("insurance"), sticker_id, data.get("total_weight"),
            data.get("front_weight"), data.get("back_weight"), data.get("registration_year"), data.get("license_class"))

            await conn.execute("""
                UPDATE applications
                SET car_id = $1
                WHERE id = $2
            """, car_id, app_id)

            # Actualizar estados de stickers
            # 1. Liberar el sticker anterior (si existe) - ponerlo como Disponible
            if old_sticker_id:
                await conn.execute(
                    "UPDATE stickers SET status = 'Disponible' WHERE id = $1",
                    old_sticker_id
                )

            # 2. Marcar el nuevo sticker (del request) como En Uso
            if sticker_id:
                await conn.execute(
                    "UPDATE stickers SET status = 'En Uso' WHERE id = $1",
                    sticker_id
                )
            
    return jsonify({"message": "Vehículo vinculado a la aplicación", "car_id": car_id}), 200


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
            SELECT id, user_id, date, workshop_id, status, result, consumed
            FROM applications
            WHERE id = $1 
        """, id)

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
            WHERE id = $1
        """, id)

        if not application:
            return jsonify({"error": "Trámite no encontrado"}), 404

        owner = None
        driver = None
        car = None

        if application["owner_id"]:
            owner = await conn.fetchrow(
                "SELECT * FROM persons WHERE id = $1", application["owner_id"]
            )

        if application["driver_id"]:
            driver = await conn.fetchrow(
                "SELECT * FROM persons WHERE id = $1", application["driver_id"]
            )

        if application["car_id"]:
            row = await conn.fetchrow("""
                SELECT
                  c.*,
                  c.total_weight,
                  c.front_weight,
                  c.back_weight,
                  s.id               AS sticker__id,
                  s.sticker_number   AS sticker__sticker_number,
                  s.expiration_date  AS sticker__expiration_date,
                  s.issued_at        AS sticker__issued_at,
                  s.status           AS sticker__status,
                  s.sticker_order_id AS sticker__sticker_order_id
                FROM cars c
                LEFT JOIN stickers s ON s.id = c.sticker_id
                WHERE c.id = $1
            """, application["car_id"])

            if row:
                car_dict = dict(row)

                sticker = None
                if car_dict.get("sticker__id") is not None:
                    sticker = {
                        "id": car_dict.pop("sticker__id"),
                        "sticker_number": car_dict.pop("sticker__sticker_number", None),
                        "expiration_date": car_dict.pop("sticker__expiration_date", None),
                        "issued_at": car_dict.pop("sticker__issued_at", None),
                        "status": car_dict.pop("sticker__status", None),
                        "sticker_order_id": car_dict.pop("sticker__sticker_order_id", None),
                    }
                    for k in ("expiration_date", "issued_at"):
                        if sticker.get(k) is not None and hasattr(sticker[k], "isoformat"):
                            sticker[k] = sticker[k].isoformat()
                else:
                    for k in list(car_dict.keys()):
                        if k.startswith("sticker__"):
                            car_dict.pop(k, None)

                # Handle green card expiration dates and add no_expiration flag
                for k in ("green_card_expiration", "license_expiration"):
                    if car_dict.get(k) is not None and hasattr(car_dict[k], "isoformat"):
                        car_dict[k] = car_dict[k].isoformat()
                
                # Add green_card_no_expiration field based on whether expiration is NULL
                

                if sticker:
                    car_dict["sticker"] = sticker

                car = car_dict

        # Documentos vinculados a la aplicación
        docs_rows = await conn.fetch("""
            SELECT id, file_name, file_url, size_bytes, mime_type, role, created_at
            FROM application_documents
            WHERE application_id = $1
            ORDER BY created_at DESC
        """, id)

        documents = []
        documents_by_role = {"owner": [], "driver": [], "car": [], "generic": []}

        for r in docs_rows:
            item = {
                "id": r["id"],
                "file_name": r["file_name"],
                "file_url": r["file_url"],
                "size_bytes": r["size_bytes"],
                "mime_type": r["mime_type"],
                "role": r["role"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            documents.append(item)
            role_key = item["role"] if item["role"] in documents_by_role else "generic"
            documents_by_role[role_key].append(item)

    return jsonify({
        "application_id": application["id"],
        "owner": dict(owner) if owner else None,
        "driver": dict(driver) if driver else None,
        "car": car if car else None,
        "documents": documents,
        "documents_by_role": documents_by_role,
    }), 200


@applications_bp.route("/workshop/<int:workshop_id>/full", methods=["GET"])
async def list_full_applications_by_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    try:
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
        page = max(1, page)
        per_page = max(1, min(per_page, 100))
    except ValueError:
        return jsonify({"error": "Parámetros inválidos"}), 400

    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip()
    status_in_raw = (request.args.get("status_in") or "").strip()
    status_list = [s.strip() for s in status_in_raw.split(",") if s.strip()]
    offset = (page - 1) * per_page

    # --- Filtros dinámicos ---
    filters = [
        "a.workshop_id = $1",
        "a.is_deleted IS NOT TRUE"   # <<<<<< EXCLUIR ELIMINADOS
    ]
    params = [workshop_id]

    if q:
        filters.append("""
            (
                a.id::text ILIKE $2 OR
                c.model ILIKE $2 OR
                c.license_plate ILIKE $2 OR
                o.first_name     ILIKE $2 OR
                o.last_name      ILIKE $2 OR
                o.dni::text      ILIKE $2
            )
        """)
        params.append(f"%{q}%")

    # Filtro por status específico (tiene prioridad sobre status_in)
    if status:
        filters.append(f"a.status = ${len(params)+1}")
        params.append(status)
    elif status_list:
        # Solo usar status_in si no se especifica status
        filters.append(f"a.status = ANY(${len(params)+1}::text[])")
        params.append(status_list)

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

        limit_idx  = len(params) + 1
        offset_idx = len(params) + 2

        rows = await conn.fetch(
            f"""
            SELECT
                a.id,
                a.user_id,
                a.date,
                a.status,
                o.first_name  AS owner_first_name,
                o.last_name   AS owner_last_name,
                o.dni         AS owner_dni,
                d.first_name  AS driver_first_name,
                d.last_name   AS driver_last_name,
                d.dni         AS driver_dni,
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
        "per_page": per_page,
        "filters": {
            "q": q,
            "status": status,
            "status_in": status_list
        }
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
    Devuelve las applications del workshop con status = 'Completado' con paginación y filtros,
    incluyendo owner, driver y car cuando existan.
    Parámetros:
      - workshop_id: int (en la URL)
      - page: int (opcional, por defecto 1)
      - per_page: int (opcional, por defecto 20, máximo 100)
      - application_id: int (filtrar por ID de aplicación)
      - license_plate: str (filtrar por patente del auto)
      - car_model: str (filtrar por modelo del auto)
      - owner_fullname: str (filtrar por nombre completo del propietario)
      - owner_dni: str (filtrar por DNI del propietario)
      - result: str (filtrar por resultado de la aplicación)
      - q: str (búsqueda general en patente, nombre, apellido, DNI, resultado)
    Respuesta:
      - Objeto con lista de applications y metadatos de paginación
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    # Obtener parámetros de paginación
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    
    # Validar parámetros de paginación
    if page < 1:
        page = 1
    if per_page < 1:
        per_page = 20
    if per_page > 100:
        per_page = 100
    
    # Obtener parámetros de filtrado
    application_id = request.args.get("application_id", type=int)
    license_plate = (request.args.get("license_plate") or "").strip()
    car_model = (request.args.get("car_model") or "").strip()
    owner_fullname = (request.args.get("owner_fullname") or "").strip()
    owner_dni = (request.args.get("owner_dni") or "").strip()
    result = (request.args.get("result") or "").strip()
    q = (request.args.get("q") or "").strip()
    
    # Calcular offset
    offset = (page - 1) * per_page

    # --- Filtros dinámicos ---
    filters = [
        "a.workshop_id = $1",
        "a.status = 'Completado'",
        "a.is_deleted IS NOT TRUE"
    ]
    params = [workshop_id]
    param_count = 1

    # Filtro por application_id
    if application_id:
        param_count += 1
        filters.append(f"a.id = ${param_count}")
        params.append(application_id)

    # Filtro por license_plate
    if license_plate:
        param_count += 1
        filters.append(f"c.license_plate ILIKE ${param_count}")
        params.append(f"%{license_plate}%")

    # Filtro por car_model
    if car_model:
        param_count += 1
        filters.append(f"c.model ILIKE ${param_count}")
        params.append(f"%{car_model}%")

    # Filtro por owner_fullname (busca en first_name, last_name y nombre completo)
    if owner_fullname:
        param_count += 1
        filters.append(f"""
            (
                o.first_name ILIKE ${param_count} OR
                o.last_name ILIKE ${param_count} OR
                CONCAT(o.first_name, ' ', o.last_name) ILIKE ${param_count}
            )
        """)
        params.append(f"%{owner_fullname}%")

    # Filtro por owner_dni
    if owner_dni:
        param_count += 1
        filters.append(f"o.dni::text ILIKE ${param_count}")
        params.append(f"%{owner_dni}%")

    # Filtro por result
    if result:
        param_count += 1
        filters.append(f"a.result ILIKE ${param_count}")
        params.append(f"%{result}%")

    # Búsqueda general (q)
    if q:
        param_count += 1
        filters.append(f"""
            (
                a.id::text ILIKE ${param_count} OR
                c.model ILIKE ${param_count} OR
                c.license_plate ILIKE ${param_count} OR
                o.first_name     ILIKE ${param_count} OR
                o.last_name      ILIKE ${param_count} OR
                o.dni::text      ILIKE ${param_count} OR
                a.result         ILIKE ${param_count}
            )
        """)
        params.append(f"%{q}%")

    # Filtro para asegurar que hay datos de car o owner
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
        # Obtener total de registros
        total_count = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM applications a
            LEFT JOIN persons o ON a.owner_id = o.id
            LEFT JOIN cars    c ON a.car_id   = c.id
            WHERE {where_sql}
            """,
            *params
        )
        
        # Obtener registros paginados
        limit_idx = len(params) + 1
        offset_idx = len(params) + 2
        
        applications = await conn.fetch(
            f"""
            SELECT
                a.id,
                a.user_id,
                a.owner_id,
                a.driver_id,
                a.car_id,
                a.date,
                a.status,
                a.result,
                o.first_name  AS owner_first_name,
                o.last_name   AS owner_last_name,
                o.dni         AS owner_dni,
                d.first_name  AS driver_first_name,
                d.last_name   AS driver_last_name,
                d.dni         AS driver_dni,
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

        # Procesar resultados (usando la misma estructura que el endpoint full)
        result = []
        for r in applications:
            result.append({
                "application_id": r["id"],
                "user_id": r["user_id"],
                "date": r["date"].isoformat() if r["date"] else None,
                "status": r["status"],
                "result": r.get("result"),
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

    # Calcular metadatos de paginación
    total_pages = (total_count + per_page - 1) // per_page
    has_next = page < total_pages
    has_prev = page > 1

    response = {
        "applications": result,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total_count,
            "total_pages": total_pages,
            "has_next": has_next,
            "has_prev": has_prev,
            "next_page": page + 1 if has_next else None,
            "prev_page": page - 1 if has_prev else None
        },
        "filters": {
            "application_id": application_id,
            "license_plate": license_plate,
            "car_model": car_model,
            "owner_fullname": owner_fullname,
            "owner_dni": owner_dni,
            "result": result,
            "q": q
        }
    }

    return jsonify(response), 200


@applications_bp.route("/workshop/<int:workshop_id>/daily-statistics", methods=["GET"])
async def get_daily_statistics(workshop_id: int):
    """
    Devuelve estadísticas diarias del taller incluyendo:
    - Número de aplicaciones del día
    - Número de aplicaciones en cola ('En Cola')
    - Tasa de aprobación del día
    - Stock actual de stickers del taller
    
    Parámetros:
      - workshop_id: int (en la URL)
      - date: str (opcional, formato YYYY-MM-DD, por defecto hoy)
    """
    try:
        user_id = g.get("user_id")
        if not user_id:
            return jsonify({"error": "No autorizado"}), 401

        # Obtener fecha (por defecto hoy)
        date_str = request.args.get("date", "")
        if date_str:
            try:
                target_date = parser.parse(date_str).date()
            except ValueError:
                return jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD"}), 400
        else:
            # Usar la fecha actual en Argentina
            argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
            target_date = datetime.datetime.now(argentina_tz).date()

        async with get_conn_ctx() as conn:
            # 1. Estadísticas de aplicaciones del día (solo aplicaciones completas)
            app_stats = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) as total_applications,
                    COUNT(CASE WHEN status = 'En Cola' THEN 1 END) as applications_in_queue
                FROM applications a
                LEFT JOIN persons o ON a.owner_id = o.id
                LEFT JOIN persons d ON a.driver_id = d.id
                LEFT JOIN cars c ON a.car_id = c.id
                WHERE a.workshop_id = $1 
                  AND a.date::date = $2
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.driver_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND o.first_name IS NOT NULL
                  AND o.last_name IS NOT NULL
                  AND o.dni IS NOT NULL
                  AND d.first_name IS NOT NULL
                  AND d.last_name IS NOT NULL
                  AND d.dni IS NOT NULL
                  AND c.license_plate IS NOT NULL
                  AND c.brand IS NOT NULL
                  AND c.model IS NOT NULL
                """,
                workshop_id, target_date
            )
            # 1.1. Obtener información del taller por separado
            workshop_info = await conn.fetchrow(
                """
                SELECT available_inspections
                FROM workshop
                WHERE id = $1
                """,
                workshop_id
            )
           
            # 2. Stock de stickers del taller
            sticker_stock = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) as total_stickers,
                    COUNT(CASE WHEN lower(s.status) = 'disponible' THEN 1 END) as available_stickers,
                    COUNT(CASE WHEN lower(s.status) = 'en uso' THEN 1 END) as used_stickers,
                    COUNT(CASE WHEN lower(s.status) = 'no disponible' THEN 1 END) as unavailable_stickers
                FROM stickers s
                JOIN sticker_orders so ON so.id = s.sticker_order_id
                WHERE so.workshop_id = $1
                """,
                workshop_id
            )
            
            # Preparar respuesta
            statistics = {
                "date": target_date.isoformat(),
                "workshop_id": workshop_id,
                "applications": {
                    "total": app_stats["total_applications"] or 0,
                    "in_queue": app_stats["applications_in_queue"] or 0,
                },
                "sticker_stock": {
                    "total": sticker_stock["total_stickers"] or 0,
                    "available": sticker_stock["available_stickers"] or 0,
                    "used": sticker_stock["used_stickers"] or 0,
                    "unavailable": sticker_stock["unavailable_stickers"] or 0
                },
                "workshop": {
                    "available_inspections": workshop_info["available_inspections"] if workshop_info else 0
                },
            }

        return jsonify(statistics), 200

    except Exception as e:
        print(f"Error in get_daily_statistics: {str(e)}")
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500


@applications_bp.route("/<int:app_id>/soft-delete", methods=["POST"])
async def soft_delete_application(app_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval(
            "SELECT id FROM applications WHERE id = $1",
            app_id
        )
        if not exists:
            return jsonify({"error": "Trámite no encontrado o sin permiso"}), 404

        await conn.execute(
            "UPDATE applications SET is_deleted = TRUE WHERE id = $1",
            app_id
        )

    return jsonify({"message": "Trámite marcado como eliminado"}), 200


@applications_bp.route("/<int:app_id>/consume-slot", methods=["POST"])
async def consume_slot(app_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            app_row = await conn.fetchrow(
                """
                SELECT id, workshop_id, consumed, is_deleted
                FROM applications
                WHERE id = $1
                FOR UPDATE
                """,
                app_id
            )
            if not app_row or app_row["is_deleted"]:
                return jsonify({"error": "Trámite no encontrado"}), 404

            if app_row["consumed"] is True:
                ws_row = await conn.fetchrow(
                    "SELECT id, available_inspections FROM workshop WHERE id = $1",
                    app_row["workshop_id"]
                )
                return jsonify({
                    "message": "La aplicación ya había consumido cupo",
                    "already_consumed": True,
                    "workshop_id": app_row["workshop_id"],
                    "available_inspections": ws_row["available_inspections"] if ws_row else None
                }), 200

            ws_row = await conn.fetchrow(
                """
                SELECT id, available_inspections
                FROM workshop
                WHERE id = $1
                FOR UPDATE
                """,
                app_row["workshop_id"]
            )
            if not ws_row:
                return jsonify({"error": "Taller no encontrado para la aplicación"}), 404

            available = int(ws_row["available_inspections"] or 0)
            if available <= 0:
                return jsonify({
                    "error": "No hay inspecciones disponibles",
                    "workshop_id": ws_row["id"],
                    "available_inspections": available
                }), 409

            await conn.execute(
                "UPDATE workshop SET available_inspections = $1 WHERE id = $2",
                available - 1,
                ws_row["id"]
            )
            await conn.execute(
                "UPDATE applications SET consumed = TRUE WHERE id = $1",
                app_id
            )

            return jsonify({
                "message": "Cupo consumido",
                "already_consumed": False,
                "workshop_id": ws_row["id"],
                "available_inspections": available - 1
            }), 200
