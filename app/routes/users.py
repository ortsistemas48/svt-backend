from quart import Blueprint, request, jsonify
from app.db import get_conn

users_bp = Blueprint("users", __name__, url_prefix="/users")

# Obtener talleres de un usuario por su ID
@users_bp.route("/<int:user_id>/workshops", methods=["GET"])
async def get_user_workshops(user_id: int):
    conn = await get_conn()

    workshops = await conn.fetch("""
        SELECT wu.workshop_id, w.name AS workshop_name, ut.name AS role
        FROM workshop_users wu
        JOIN workshop w ON wu.workshop_id = w.id
        JOIN user_types ut ON wu.user_type_id = ut.id
        WHERE wu.user_id = $1
    """, user_id)

    return jsonify({
        "user_id": user_id,
        "workshops": [dict(w) for w in workshops]
    })


@users_bp.route("/<int:user_id>/workshops/<int:workshop_id>/role", methods=["GET"])
async def get_user_role_in_workshop(user_id: int, workshop_id: int):
    conn = await get_conn()

    role_row = await conn.fetchrow("""
        SELECT ut.id AS role_id, ut.name AS role_name
        FROM workshop_users wu
        JOIN user_types ut ON wu.user_type_id = ut.id
        WHERE wu.user_id = $1 AND wu.workshop_id = $2
    """, user_id, workshop_id)

    if not role_row:
        return jsonify({"error": "El usuario no tiene rol asignado en ese taller"}), 404

    return jsonify({
        "user_id": user_id,
        "workshop_id": workshop_id,
        "role": {
            "id": role_row["role_id"],
            "name": role_row["role_name"]
        }
    })


@users_bp.route("/<int:user_id>/is-garage-owner", methods=["GET"])
async def is_garage_owner(user_id: int):
    conn = await get_conn()

    result = await conn.fetchval("""
        SELECT EXISTS (
            SELECT 1
            FROM workshop_users
            WHERE user_id = $1 AND user_type_id = 2
        )
    """, user_id)

    return jsonify({
        "user_id": user_id,
        "is_garage_owner": result
    })
