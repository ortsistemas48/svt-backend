from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from asyncpg.exceptions import UniqueViolationError

workshops_bp = Blueprint("workshops", __name__, url_prefix="/workshops")


# Crear workshop
@workshops_bp.route("/create", methods=["POST"])
async def create_workshop():
    # requiere middleware que setee g["user_id"]
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()
    name = (data.get("name") or "").strip()
    province = (data.get("province") or "").strip()
    city = (data.get("city") or "").strip()
    phone = (data.get("phone") or "").strip()
    cuit = (data.get("cuit") or "").strip()

    if len(name) < 3:
        return jsonify({"error": "El nombre debe tener al menos 3 caracteres"}), 400

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

    # normalizaciones simples
    import re
    digits_only = re.compile(r"\D+")

    phone_norm = phone.strip()
    cuit_norm = digits_only.sub("", cuit) if cuit else None
    if cuit_norm and len(cuit_norm) != 11:
        return jsonify({"error": "CUIT inválido, deben ser 11 dígitos"}), 400

    OWNER_ROLE_ID = 2 

    async with get_conn_ctx() as conn:
        try:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO workshop (name, province, city, phone, cuit)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING id, name, province, city, phone, cuit
                    """,
                    name, province, city, phone_norm, cuit_norm
                )

                await conn.execute(
                    """
                    INSERT INTO workshop_users (workshop_id, user_id, user_type_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (workshop_id, user_id)
                    DO UPDATE SET user_type_id = EXCLUDED.user_type_id
                    """,
                    row["id"], user_id, OWNER_ROLE_ID
                )

        except UniqueViolationError as e:
            # puede saltar por name o por cuit
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
