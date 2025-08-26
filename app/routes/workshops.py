from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from asyncpg.exceptions import UniqueViolationError

workshops_bp = Blueprint("workshops", __name__, url_prefix="/workshops")

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
                    INSERT INTO workshop (name, razon_social, province, city, phone, cuit, plant_number)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id, name, razon_social, province, city, phone, cuit, plant_number
                    """,
                    name, razon_social, province, city, phone_norm, cuit_norm, plant_number
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
