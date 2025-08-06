from quart import Blueprint, request, jsonify
from app.db import get_conn

users_bp = Blueprint("users", __name__, url_prefix="/users")


@users_bp.route("/get_users/workshop/<int:workshop_id>", methods=["GET"])
async def get_users_in_workshop(workshop_id: int):
    conn = await get_conn()
    users = await conn.fetch("""
        SELECT u.id, u.first_name, u.last_name, u.email, u.dni, u.phone_number, ut.name AS role
        FROM workshop_users wu
        JOIN users u ON wu.user_id = u.id
        JOIN user_types ut ON wu.user_type_id = ut.id
        WHERE wu.workshop_id = $1;
    """, workshop_id)

    return jsonify({
        "workshop_id": workshop_id,
        "users": [dict(user) for user in users]
    })

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


@users_bp.route("/<int:user_id>/workshops/<int:work_ishopd>/role", methods=["GET"])
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
