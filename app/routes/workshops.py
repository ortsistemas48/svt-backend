from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from asyncpg.exceptions import UniqueViolationError
from uuid import UUID

workshops_bp = Blueprint("workshops", __name__, url_prefix="/workshops")

OWNER_ROLE_ID = 2
VALID_PROVINCES = {
    "Buenos Aires","CABA","Catamarca","Chaco","Chubut","Córdoba","Corrientes",
    "Entre Ríos","Formosa","Jujuy","La Pampa","La Rioja","Mendoza","Misiones",
    "Neuquén","Río Negro","Salta","San Juan","San Luis","Santa Cruz",
    "Santa Fe","Santiago del Estero","Tierra del Fuego","Tucumán"
}

def _clean_int_or_none(v, field_name: str):
    if v in (None, ""):
        return None
    try:
        n = int(v)
        if n <= 0:
            raise ValueError
        return n
    except Exception:
        raise ValueError(f"El {field_name} debe ser numérico y mayor a cero")
    
    
async def _is_admin(conn, user_id: int) -> bool:
    # ajustá según tu schema, por ejemplo users.is_admin boolean
    return await conn.fetchval(
        "SELECT COALESCE(is_admin, false) FROM users WHERE id = $1",
        user_id
    )

@workshops_bp.route("/create-unapproved", methods=["POST"])
async def create_workshop_unapproved():
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    name = (data.get("name") or "").strip()
    razon_social = (data.get("razonSocial") or "").strip()
    province = (data.get("province") or "").strip()
    city = (data.get("city") or "").strip()
    address = (data.get("address") or "").strip() 
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()
    plant_number_raw = data.get("plantNumber")
    disposition_number = (data.get("dispositionNumber") or "").strip()
    
    if not disposition_number:
        return jsonify({"error": "Falta el número de disposición"}), 400
    if len(name) < 3:
        return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400
    if len(razon_social) < 3:
        return jsonify({"error": "Ingresá una razón social válida"}), 400
    if province not in VALID_PROVINCES:
        return jsonify({"error": "Provincia inválida"}), 400
    if not city:
        return jsonify({"error": "Falta la localidad"}), 400

    import re
    digits_only = re.compile(r"\D+")
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    try:
        plant_number = _clean_int_or_none(plant_number_raw, "número de planta")
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    async with get_conn_ctx() as conn:
        try:
            async with conn.transaction():
                # detectar si la tabla workshop tiene columna address
                cols = await conn.fetch("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'workshop'
                """)
                colset = {r["column_name"] for r in cols}

                if "address" in colset:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO workshop (name, razon_social, province, city, address, phone, cuit, plant_number, disposition_number, is_approved)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,false)
                        RETURNING id, name, razon_social, province, city, address, phone, cuit, plant_number, disposition_number, is_approved
                        """,
                        name, razon_social, province, city, address, phone, cuit_norm, plant_number, disposition_number
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO workshop (name, razon_social, province, city, phone, cuit, plant_number, disposition_number, is_approved)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false)
                        RETURNING id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number, is_approved
                        """,
                        name, razon_social, province, city, phone, cuit_norm, plant_number, disposition_number
                    )
                ws_id = row["id"]

                # asignar OWNER al creador
                await conn.execute(
                    """
                    INSERT INTO workshop_users (workshop_id, user_id, user_type_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (workshop_id, user_id) DO UPDATE SET user_type_id = EXCLUDED.user_type_id
                    """,
                    ws_id, user_id, OWNER_ROLE_ID
                )

                # inicializar steps y observaciones por defecto igual que tu create actual
                steps = await conn.fetch("SELECT id, name FROM steps ORDER BY id ASC")
                if not steps:
                    raise RuntimeError("No hay pasos base en la tabla steps")

                for idx, s in enumerate(steps):
                    await conn.execute(
                        """
                        INSERT INTO steps_order (workshop_id, step_id, number)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                        """,
                        ws_id, s["id"], idx + 1
                    )
                    defaults = [
                        "Verificación visual",
                        "Desgaste o grietas",
                        "Fijaciones y holguras",
                        "Funcionamiento general",
                        "Medidas y tolerancias",
                    ]
                    for desc in defaults:
                        await conn.execute(
                            """
                            INSERT INTO observations (workshop_id, step_id, description)
                            SELECT $1, $2, $3
                            WHERE NOT EXISTS (
                              SELECT 1 FROM observations
                              WHERE workshop_id = $1 AND step_id = $2 AND description = $3
                            )
                            """,
                            ws_id, s["id"], desc
                        )

        except UniqueViolationError as e:
            msg = "Ya existe un workshop con ese nombre"
            if "workshop_cuit_uidx" in str(e):
                msg = "Ya existe un workshop con ese CUIT"
            return jsonify({"error": msg}), 409
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 500

    # armar respuesta consistente
    out = {
        "id": row["id"],
        "name": row["name"],
        "razonSocial": row["razon_social"],
        "province": row["province"],
        "city": row["city"],
        "phone": row["phone"],
        "cuit": row["cuit"],
        "plant_number": row["plant_number"],
        "disposition_number": row["disposition_number"],
        "is_approved": row["is_approved"],
    }
    if "address" in row.keys():
        out["address"] = row["address"]

    return jsonify({
        "message": "Workshop creado en estado pendiente de aprobación",
        "workshop": out,
        "membership": {
            "user_id": user_id,
            "workshop_id": row["id"],
            "user_type_id": OWNER_ROLE_ID
        }
    }), 201
    
    
@workshops_bp.route("/<int:workshop_id>/approve", methods=["POST"])
async def approve_workshop(workshop_id: int):
    # acá podrías validar que g.user_id sea admin
    async with get_conn_ctx() as conn:
        result = await conn.fetchrow(
            """
            UPDATE workshop
            SET is_approved = true, updated_at = NOW()
            WHERE id = $1
            RETURNING id, is_approved
            """,
            workshop_id
        )
    if not result:
        return jsonify({"error": "Workshop no encontrado"}), 404
    return jsonify({"ok": True, "workshop_id": result["id"], "is_approved": result["is_approved"]}), 200


@workshops_bp.route("/pending", methods=["GET"])
async def list_pending_workshops():
    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number
            FROM workshop
            WHERE is_approved = false
            ORDER BY id DESC
            """
        )
    return jsonify([dict(r) for r in rows]), 200


# Crear workshop
@workshops_bp.route("/create", methods=["POST"])
async def create_workshop():
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()

    # Entrada y normalización
    name = (data.get("name") or "").strip()
    razon_social = (data.get("razonSocial") or "").strip()
    province = (data.get("province") or "").strip()
    city = (data.get("city") or "").strip()
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()
    plant_number_raw = (data.get("plantNumber") or None)
    disposition_number = (data.get("disposition_number") or "").strip()

    # Validaciones
    if len(name) < 3:
        return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400
    if len(razon_social) < 3:
        return jsonify({"error": "Ingresá una razón social válida"}), 400

    VALID_PROVINCES = {
        "Buenos Aires","CABA","Catamarca","Chaco","Chubut","Córdoba","Corrientes",
        "Entre Ríos","Formosa","Jujuy","La Pampa","La Rioja","Mendoza","Misiones",
        "Neuquén","Río Negro","Salta","San Juan","San Luis","Santa Cruz",
        "Santa Fe","Santiago del Estero","Tierra del Fuego","Tucumán"
    }
    if province not in VALID_PROVINCES:
        return jsonify({"error": "Provincia inválida"}), 400
    if not city:
        return jsonify({"error": "Falta la localidad"}), 400

    import re
    digits_only = re.compile(r"\D+")
    phone_norm = phone.strip()
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    plant_number = None
    if plant_number_raw not in (None, ""):
        try:
            plant_number = int(plant_number_raw)
            if plant_number <= 0:
                return jsonify({"error": "El número de planta debe ser mayor a cero"}), 400
        except ValueError:
            return jsonify({"error": "El número de planta debe ser numérico"}), 400

    OWNER_ROLE_ID = 2

    from asyncpg import UniqueViolationError
    async with get_conn_ctx() as conn:
        try:
            async with conn.transaction():
                # 1) crear workshop
                row = await conn.fetchrow(
                    """
                    INSERT INTO workshop (name, razon_social, province, city, phone, cuit, plant_number, disposition_number)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    RETURNING id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number
                    """,
                    name, razon_social, province, city, phone_norm, cuit_norm, plant_number, disposition_number
                )
                ws_id = row["id"]

                # 2) dar rol de owner al creador sin ON CONFLICT:
                # primero intento actualizar, si no afectó filas, inserto
                upd_status = await conn.execute(
                    """
                    UPDATE workshop_users
                    SET user_type_id = $3
                    WHERE workshop_id = $1 AND user_id = $2
                    """,
                    ws_id, user_id, OWNER_ROLE_ID
                )
                # asyncpg devuelve "UPDATE <n>"
                if upd_status.split()[-1] == "0":
                    await conn.execute(
                        """
                        INSERT INTO workshop_users (workshop_id, user_id, user_type_id)
                        VALUES ($1, $2, $3)
                        """,
                        ws_id, user_id, OWNER_ROLE_ID
                    )

                # 3) inicializar orden de pasos
                steps = await conn.fetch("SELECT id, name FROM steps ORDER BY id ASC")
                if not steps:
                    raise RuntimeError("No hay pasos base en la tabla steps")

                pairs = [(s["id"], idx + 1) for idx, s in enumerate(steps)]

                # inserto cada fila solo si no existe ya ese (workshop_id, step_id)
                for sid, num in pairs:
                    await conn.execute(
                        """
                        INSERT INTO steps_order (workshop_id, step_id, number)
                        SELECT $1, $2, $3
                        WHERE NOT EXISTS (
                            SELECT 1 FROM steps_order
                            WHERE workshop_id = $1 AND step_id = $2
                        )
                        """,
                        ws_id, sid, num
                    )

                # 4) crear 5 observaciones por defecto para cada step del workshop
                for s in steps:
                    sid = s["id"]
                    sname = (s["name"] or "").strip()
                    defaults = [
                        f"Verificación visual",
                        f"Desgaste o grietas",
                        f"Fijaciones y holguras",
                        f"Funcionamiento general",
                        f"Medidas y tolerancias",
                    ]
                    for desc in defaults:
                        await conn.execute(
                            """
                            INSERT INTO observations (workshop_id, step_id, description)
                            SELECT $1, $2, $3
                            WHERE NOT EXISTS (
                                SELECT 1 FROM observations
                                WHERE workshop_id = $1 AND step_id = $2 AND description = $3
                            )
                            """,
                            ws_id, sid, desc
                        )

        except UniqueViolationError as e:
            msg = "Ya existe un workshop con ese nombre"
            if "workshop_cuit_uidx" in str(e):
                msg = "Ya existe un workshop con ese CUIT"
            return jsonify({"error": msg}), 409

    return jsonify({
        "message": "Workshop creado",
        "workshop": dict(row),
        "membership": {
            "user_id": user_id,
            "workshop_id": row["id"],
            "user_type_id": OWNER_ROLE_ID
        }
    }), 201

# ====== Verificar membresía del usuario en un taller ======
@workshops_bp.route("/<int:workshop_id>/membership", methods=["GET"])
async def check_workshop_membership(workshop_id: int):
    """
    Devuelve si el usuario autenticado (g.user_id) pertenece al taller indicado.
    - 401 si no hay usuario autenticado.
    - 404 si el taller no existe o no está aprobado.
    - 200 con is_member=True/False si el taller existe.
    """
    
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        # 1) verificar que el taller exista y esté aprobado
        workshop = await conn.fetchrow("SELECT id, is_approved FROM workshop WHERE id = $1", workshop_id)
        print(f"workshop={workshop}")
        if not workshop:
            return jsonify({"error": "Workshop no encontrado"}), 404
        
        if not workshop["is_approved"]:
            return jsonify({"error": "Workshop no se encuentra aprobado"}), 404

        # 2) verificar membresía (y devolver rol si existe)
        row = await conn.fetchrow(
            """
            SELECT user_type_id
            FROM workshop_users
            WHERE workshop_id = $1 AND user_id = $2
            """,
            workshop_id, user_id
        )

    if not row:
        return jsonify({
            "workshop_id": workshop_id,
            "user_id": str(user_id),
            "is_member": False
        }), 200

    return jsonify({
        "workshop_id": workshop_id,
        "user_id": str(user_id),
        "is_member": True,
        "user_type_id": row["user_type_id"]  # p.ej. 2 = OWNER
    }), 200

# Cambiar el nombre de un workshop
@workshops_bp.route("/<int:workshop_id>/name", methods=["PUT", "PATCH"])
async def rename_workshop(workshop_id: int):
    data = await request.get_json()
    new_name = (data.get("name") or "").strip()

    if not new_name:
        return jsonify({"error": "Falta el nuevo nombre"}), 400

    async with get_conn_ctx() as conn:
        exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", workshop_id)
        if not exists:
            return jsonify({"error": "Workshop no encontrado"}), 404

        try:
            row = await conn.fetchrow(
                """
                UPDATE workshop
                SET name = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2
                RETURNING id, name
                """,
                new_name, workshop_id,
            )
        except UniqueViolationError:
            return jsonify({"error": "Ya existe un workshop con ese nombre"}), 409

    return jsonify({"message": "Nombre actualizado", "workshop": dict(row)}), 200


# ====== Helpers comunes ======
async def _user_belongs_to_workshop(conn, user_id: int, workshop_id: int) -> bool:
    return await conn.fetchval(
        """
        SELECT EXISTS(
          SELECT 1 FROM workshop_users
          WHERE workshop_id = $1 AND user_id = $2
        )
        """,
        workshop_id, user_id
    )

async def _step_belongs_to_workshop(conn, step_id: int, workshop_id: int) -> bool:
    return await conn.fetchval(
        """
        SELECT EXISTS(
          SELECT 1 FROM steps_order
          WHERE workshop_id = $1 AND step_id = $2
        )
        """,
        workshop_id, step_id
    )

def _camel_ws_row(row) -> dict:
    """Mapea columnas del workshop a las claves esperadas en front."""
    if not row:
        return {}
    return {
        "id": row["id"],
        "name": row["name"],
        "razonSocial": row["razon_social"],
        "phone": row["phone"],
        "cuit": row["cuit"],
        "province": row["province"],
        "city": row["city"],
        # Mantengo snake por compatibilidad, si querés camel cambiá a plantNumber
        "plant_number": row["plant_number"],
        "disposition_number": row["disposition_number"],
    }

# ====== 1) Obtener datos del taller ======
@workshops_bp.route("/<int:workshop_id>", methods=["GET"])
async def get_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        is_admin = await _is_admin(conn, user_id)
        # solo bloquear si no es admin y no pertenece
        if not is_admin:
            belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
            if not belongs:
                return jsonify({"error": "No tenés acceso a este taller"}), 403

        row = await conn.fetchrow(
            """
            SELECT id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number
            FROM workshop
            WHERE id = $1
            """,
            workshop_id
        )
        if not row:
            return jsonify({"error": "Workshop no encontrado"}), 404

    return jsonify(_camel_ws_row(row)), 200


@workshops_bp.route("/admin/<int:workshop_id>/members", methods=["GET"])
async def admin_list_workshop_members(workshop_id: int):
    # si en tu middleware pones g.user_id, podés usarlo, si no, cambialo
    from quart import g
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403

        exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", workshop_id)
        if not exists:
            return jsonify({"error": "Workshop no encontrado"}), 404

        rows = await conn.fetch(
            """
            SELECT
              u.id::text          AS user_id,
              u.first_name,
              u.last_name,
              u.email,
              u.dni,
              u.phone_number,
              ut.name             AS role,
              wu.user_type_id,
              wu.created_at
            FROM workshop_users wu
            JOIN users u      ON u.id = wu.user_id
            LEFT JOIN user_types ut ON ut.id = wu.user_type_id
            WHERE wu.workshop_id = $1
            ORDER BY wu.user_type_id NULLS LAST, u.last_name, u.first_name
            """,
            workshop_id
        )

    return jsonify([dict(r) for r in rows]), 200


@workshops_bp.route("/admin/<int:workshop_id>/members/<int:member_user_id>", methods=["DELETE"])
async def admin_unassign_member(workshop_id: int, member_user_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, user_id):
            return jsonify({"error": "Requiere admin"}), 403

        # no permitas dejar el taller sin OWNER, opcional:
        owner_left = await conn.fetchval(
            """
            SELECT (SELECT COUNT(*) FROM workshop_users WHERE workshop_id=$1 AND user_type_id=$2) = 1
                   AND EXISTS(SELECT 1 FROM workshop_users WHERE workshop_id=$1 AND user_id=$3 AND user_type_id=$2)
            """,
            workshop_id, OWNER_ROLE_ID, member_user_id
        )
        if owner_left:
            return jsonify({"error": "No se puede quitar al único OWNER del taller"}), 400

        result = await conn.execute(
            """
            DELETE FROM workshop_users
            WHERE workshop_id = $1 AND user_id = $2
            """,
            workshop_id, member_user_id
        )
    return jsonify({"ok": True, "result": result}), 200

def _as_uuid(s: str) -> UUID:
    return UUID(s)  # lanza ValueError si no es UUID


@workshops_bp.route("/admin/<int:workshop_id>/members/<user_id>", methods=["DELETE", "OPTIONS"])
async def admin_unassign_workshop_member(workshop_id: int, user_id: str):
    if request.method == "OPTIONS":
        return ("", 204)

    from quart import g
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401

    # validar UUID
    try:
        _as_uuid(user_id)
    except ValueError:
        return jsonify({"error": "user_id inválido, debe ser UUID"}), 400

    OWNER_ROLE_ID = 2

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403

        exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", workshop_id)
        if not exists:
            return jsonify({"error": "Workshop no encontrado"}), 404

        # evitar dejar el taller sin owner, opcional pero recomendado
        is_last_owner = await conn.fetchval(
            """
            SELECT
              (SELECT COUNT(*) FROM workshop_users
               WHERE workshop_id = $1 AND user_type_id = $2) = 1
              AND EXISTS(
                SELECT 1 FROM workshop_users
                WHERE workshop_id = $1 AND user_id = $3::uuid AND user_type_id = $2
              )
            """,
            workshop_id, OWNER_ROLE_ID, user_id
        )
        if is_last_owner:
            return jsonify({"error": "No se puede quitar al único OWNER del taller"}), 400

        result = await conn.execute(
            """
            DELETE FROM workshop_users
            WHERE workshop_id = $1 AND user_id = $2::uuid
            """,
            workshop_id, user_id
        )

    # asyncpg devuelve "DELETE n"
    if result.endswith("0"):
        return jsonify({"error": "Usuario no estaba asignado a este taller"}), 404

    return jsonify({"ok": True, "workshop_id": workshop_id, "user_id": user_id}), 200


@workshops_bp.route("/<int:workshop_id>", methods=["PATCH", "PUT"])
async def update_workshop(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}

    # Solo se pueden editar estos campos
    name = (data.get("name") or "").strip()
    razon_social = (data.get("razonSocial") or "").strip()
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()

    import re
    digits_only = re.compile(r"\D+")
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    sets, vals = [], []
    idx = 1
    if name:
        if len(name) < 3:
            return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400
        sets.append(f"name = ${idx}"); vals.append(name); idx += 1
    if razon_social:
        if len(razon_social) < 3:
            return jsonify({"error": "Ingresá una razón social válida"}), 400
        sets.append(f"razon_social = ${idx}"); vals.append(razon_social); idx += 1
    if phone is not None:
        sets.append(f"phone = ${idx}"); vals.append(phone); idx += 1
    if cuit_norm is not None:
        sets.append(f"cuit = ${idx}"); vals.append(cuit_norm); idx += 1

    if not sets:
        return jsonify({"error": "No hay datos para actualizar"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        try:
            row = await conn.fetchrow(
                f"""
                UPDATE workshop
                SET {", ".join(sets)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ${idx}
                RETURNING id, name, razon_social, province, city, phone, cuit, plant_number, disposition_number
                """,
                *vals, workshop_id
            )
        except UniqueViolationError:
            return jsonify({"error": "Ya existe un workshop con ese nombre o CUIT"}), 409

    return jsonify({"message": "Taller actualizado", "workshop": _camel_ws_row(row)}), 200

# ====== 3) Listar orden de pasos del taller ======
@workshops_bp.route("/<int:workshop_id>/steps-order", methods=["GET"])
async def get_steps_order(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        rows = await conn.fetch(
            """
            SELECT so.step_id, s.name, s.description, so.number
            FROM steps_order so
            JOIN steps s ON s.id = so.step_id
            WHERE so.workshop_id = $1
            ORDER BY so.number ASC
            """,
            workshop_id
        )

        if not rows:
            # inicializar desde steps base
            base_steps = await conn.fetch("SELECT id, name, description FROM steps ORDER BY id ASC")
            if not base_steps:
                return jsonify({"error": "No hay pasos base en la tabla steps"}), 500

            async with conn.transaction():
                for idx, s in enumerate(base_steps):
                    await conn.execute(
                        """
                        INSERT INTO steps_order (workshop_id, step_id, number)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                        """,
                        workshop_id, s["id"], idx + 1
                    )

            # volver a leer con el orden ya creado
            rows = await conn.fetch(
                """
                SELECT so.step_id, s.name, s.description, so.number
                FROM steps_order so
                JOIN steps s ON s.id = so.step_id
                WHERE so.workshop_id = $1
                ORDER BY so.number ASC
                """,
                workshop_id
            )

    out = [
        {
            "step_id": r["step_id"],
            "name": r["name"],
            "description": r["description"],
            "number": r["number"],
        }
        for r in rows
    ]
    return jsonify(out), 200

# ====== 4) Guardar orden de pasos del taller ======
@workshops_bp.route("/<int:workshop_id>/steps-order", methods=["PUT"])
async def save_steps_order(workshop_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    payload = await request.get_json() or {}
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"error": "Formato inválido, se espera items: [{ step_id, number }]"}), 400

    # Validación mínima
    try:
        step_ids = [int(i["step_id"]) for i in items]
        numbers = [int(i["number"]) for i in items]
    except Exception:
        return jsonify({"error": "step_id y number deben ser numéricos"}), 400

    if len(set(numbers)) != len(numbers):
        return jsonify({"error": "Hay números de orden repetidos"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        # Verifica que todos los steps pertenezcan al taller
        valid = await conn.fetch(
            """
            SELECT step_id FROM steps_order
            WHERE workshop_id = $1 AND step_id = ANY($2::int[])
            """,
            workshop_id, step_ids
        )
        valid_set = {r["step_id"] for r in valid}
        invalid = [sid for sid in step_ids if sid not in valid_set]
        if invalid:
            return jsonify({"error": f"Paso no pertenece al taller, ids: {invalid}"}), 400

        # Actualización en lote
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE steps_order AS so
                SET number = x.number
                FROM (
                  SELECT unnest($1::int[]) AS step_id, unnest($2::int[]) AS number
                ) AS x
                WHERE so.workshop_id = $3 AND so.step_id = x.step_id
                """,
                step_ids, numbers, workshop_id
            )

    return jsonify({"message": "Orden de pasos guardado"}), 200

# ====== 5) Observaciones por paso ======

# 5.1 Listar observaciones de un paso del taller
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations", methods=["GET"])
async def list_step_observations(workshop_id: int, step_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        rows = await conn.fetch(
            """
            SELECT id, description
            FROM observations
            WHERE workshop_id = $1 AND step_id = $2
            ORDER BY id
            """,
            workshop_id, step_id
        )
    return jsonify([{"id": r["id"], "description": r["description"]} for r in rows]), 200

# 5.2 Crear observación
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations", methods=["POST"])
async def create_step_observation(workshop_id: int, step_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}
    desc = (data.get("description") or "").strip()
    if not desc:
        return jsonify({"error": "Falta description"}), 400
    if len(desc) > 300:
        return jsonify({"error": "La descripción no puede superar 300 caracteres"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        row = await conn.fetchrow(
            """
            INSERT INTO observations (workshop_id, step_id, description)
            VALUES ($1, $2, $3)
            RETURNING id, description
            """,
            workshop_id, step_id, desc
        )
    return jsonify({"id": row["id"], "description": row["description"]}), 201

# 5.3 Editar observación
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations/<int:obs_id>", methods=["PUT", "PATCH"])
async def update_step_observation(workshop_id: int, step_id: int, obs_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}
    desc = (data.get("description") or "").strip()
    if not desc:
        return jsonify({"error": "Falta description"}), 400
    if len(desc) > 300:
        return jsonify({"error": "La descripción no puede superar 300 caracteres"}), 400

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        exists = await conn.fetchval(
            """
            SELECT 1 FROM observations
            WHERE id = $1 AND workshop_id = $2 AND step_id = $3
            """,
            obs_id, workshop_id, step_id
        )
        if not exists:
            return jsonify({"error": "Observación no encontrada"}), 404

        row = await conn.fetchrow(
            """
            UPDATE observations
            SET description = $1
            WHERE id = $2
            RETURNING id, description
            """,
            desc, obs_id
        )
    return jsonify({"id": row["id"], "description": row["description"]}), 200

# 5.4 Eliminar observación
@workshops_bp.route("/<int:workshop_id>/steps/<int:step_id>/observations/<int:obs_id>", methods=["DELETE"])
async def delete_step_observation(workshop_id: int, step_id: int, obs_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        belongs = await _user_belongs_to_workshop(conn, user_id, workshop_id)
        if not belongs:
            return jsonify({"error": "No tenés acceso a este taller"}), 403
        ok = await _step_belongs_to_workshop(conn, step_id, workshop_id)
        if not ok:
            return jsonify({"error": "El paso no corresponde al taller"}), 400

        # Limpia también vínculos con observation_details si existieran
        async with conn.transaction():
            await conn.execute(
                """
                DELETE FROM observation_details
                WHERE observation_id = $1
                """,
                obs_id
            )
            result = await conn.execute(
                """
                DELETE FROM observations
                WHERE id = $1 AND workshop_id = $2 AND step_id = $3
                """,
                obs_id, workshop_id, step_id
            )
    # asyncpg devuelve "DELETE n", no hace falta retornar n exacto
    return jsonify({"message": "Observación eliminada"}), 200
