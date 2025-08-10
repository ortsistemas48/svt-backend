from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx

users_bp = Blueprint("users", __name__, url_prefix="/users")


@users_bp.route("/by-email", methods=["GET"])
async def get_user_by_email():
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "email requerido"}), 400

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow("""
            SELECT id, first_name, last_name, email, dni, phone_number
            FROM users
            WHERE LOWER(email) = LOWER($1)
            LIMIT 1
        """, email)

    return jsonify(dict(row) if row else None)


@users_bp.route("/get_users/workshop/<int:workshop_id>", methods=["GET"])
async def get_users_in_workshop(workshop_id: int):
    async with get_conn_ctx() as conn:
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
    async with get_conn_ctx() as conn:
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
    async with get_conn_ctx() as conn:
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
    async with get_conn_ctx() as conn:
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


@users_bp.route("/user-types", methods=["GET"])
async def list_user_types():
    async with get_conn_ctx() as conn:
        rows = await conn.fetch("SELECT id, name FROM user_types ORDER BY id")
    return jsonify([dict(r) for r in rows])


@users_bp.route("/assign/<int:workshop_id>", methods=["POST"])
async def attach_user_to_workshop(workshop_id: int):
    data = await request.get_json()
    user_id = data.get("user_id")
    user_type_id = data.get("user_type_id")

    if not user_id or not user_type_id:
        return jsonify({"error": "user_id y user_type_id son requeridos"}), 400

    async with get_conn_ctx() as conn:
        await conn.execute("""
            INSERT INTO workshop_users (workshop_id, user_id, user_type_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (workshop_id, user_id) DO UPDATE SET user_type_id = EXCLUDED.user_type_id
        """, workshop_id, user_id, user_type_id)

    return jsonify({"ok": True})
