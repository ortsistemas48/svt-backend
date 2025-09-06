from quart import Blueprint, request, jsonify
from app.db import get_conn_ctx
from uuid import UUID

users_bp = Blueprint("users", __name__, url_prefix="/users")


@users_bp.route("/by-email", methods=["GET"])
async def get_user_by_email():
    email = request.args.get("email")
    workshop_id = request.args.get("workshop_id", type=int)

    if not email:
        return jsonify({"error": "email requerido"}), 400
    if workshop_id is None:
        return jsonify({"error": "workshop_id requerido"}), 400

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.email,
                u.dni,
                u.phone_number,
                u.title_name,
                u.licence_number,
                wu.user_type_id
            FROM users u
            LEFT JOIN workshop_users wu
              ON wu.user_id = u.id
             AND wu.workshop_id = $2
            WHERE LOWER(u.email) = LOWER($1)
            LIMIT 1
            """,
            email, workshop_id
        )

    # Si el usuario existe pero no está asociado a ese workshop,
    # user_type_id vendrá como null.
    return jsonify(dict(row) if row else None)


@users_bp.route("/by-email-lite", methods=["GET"])
async def get_user_by_email_lite():
    email = request.args.get("email")

    if not email:
        return jsonify({"error": "email requerido"}), 400
    print(email)
    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                u.id::text AS id,
                u.first_name,
                u.last_name,
                u.email,
                u.dni,
                u.phone_number,
                u.title_name,
                u.licence_number
            FROM users u
            WHERE LOWER(u.email) = LOWER($1)
            LIMIT 1
            """,
            email
        )
    print(dict(row))
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


@users_bp.route("/get_users/all", methods=["GET"])
async def get_all_users():
    # Parámetros de paginación opcionales: ?limit=100&offset=0
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit y offset deben ser números"}), 400

    # límites razonables
    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.email,
                u.dni,
                u.phone_number,
                COALESCE(
                  json_agg(
                    DISTINCT jsonb_build_object(
                      'workshop_id', wu.workshop_id,
                      'role', ut.name
                    )
                  ) FILTER (WHERE wu.user_id IS NOT NULL),
                  '[]'
                ) AS memberships
            FROM users u
            LEFT JOIN workshop_users wu ON wu.user_id = u.id
            LEFT JOIN user_types ut ON ut.id = wu.user_type_id
            GROUP BY u.id, u.first_name, u.last_name, u.email, u.dni, u.phone_number
            ORDER BY u.id
            LIMIT $1 OFFSET $2;
            """,
            limit,
            offset,
        )

        # total para ayudar a paginar en el frontend
        total_row = await conn.fetchrow("SELECT COUNT(*) AS total FROM users;")
        total = total_row["total"] if total_row else 0

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "users": [
            {
                "id": r["id"],
                "first_name": r["first_name"],
                "last_name": r["last_name"],
                "email": r["email"],
                "dni": r["dni"],
                "phone_number": r["phone_number"],
                "memberships": r["memberships"],  # lista de {workshop_id, role}
            }
            for r in rows
        ],
    })
    
    
@users_bp.route("/get_users/pending", methods=["GET"])
async def get_pending_users():
    # ?limit=100&offset=0
    try:
        limit = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit y offset deben ser números"}), 400

    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT
                u.id,
                u.first_name,
                u.last_name,
                u.email,
                u.dni,
                u.created_at,
                COALESCE(
                  json_agg(
                    DISTINCT jsonb_build_object(
                      'workshop_id', wu.workshop_id,
                      'role', ut.name
                    )
                  ) FILTER (WHERE wu.user_id IS NOT NULL),
                  '[]'
                ) AS memberships
            FROM users u
            LEFT JOIN workshop_users wu ON wu.user_id = u.id
            LEFT JOIN user_types ut ON ut.id = wu.user_type_id
            WHERE u.is_approved = false
            GROUP BY u.id, u.first_name, u.last_name, u.email, u.dni, u.created_at
            ORDER BY u.id
            LIMIT $1 OFFSET $2;
            """,
            limit,
            offset,
        )

        total_row = await conn.fetchrow(
            "SELECT COUNT(*) AS total FROM users WHERE is_approved = false;"
        )
        total = total_row["total"] if total_row else 0

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "users": [
            {
                "id": r["id"],
                "first_name": r["first_name"],
                "last_name": r["last_name"],
                "email": r["email"],
                "dni": r["dni"],
                "created_at": r["created_at"],
                "memberships": r["memberships"],
            }
            for r in rows
        ],
    })
    
    
def _as_uuid(s: str) -> UUID:
    return UUID(s)  # lanza ValueError si no es UUID


@users_bp.route("/approve/<user_id>", methods=["POST", "OPTIONS"])
async def approve_user(user_id: str):
    # Preflight CORS
    if request.method == "OPTIONS":
        return ("", 204)

    # Validar que tenga formato UUID
    try:
        _as_uuid(user_id)
    except ValueError:
        return jsonify({"error": "user_id inválido, debe ser UUID"}), 400

    async with get_conn_ctx() as conn:
        result = await conn.execute("""
            UPDATE users
            SET is_approved = true
            WHERE id = $1::uuid
        """, user_id)

    if result.endswith("0"):
        return jsonify({"error": "Usuario no encontrado"}), 404

    return jsonify({"ok": True, "user_id": user_id, "approved": True})


@users_bp.route("/reject/<user_id>", methods=["POST", "OPTIONS"])
async def reject_user(user_id: str):
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        _as_uuid(user_id)
    except ValueError:
        return jsonify({"error": "user_id inválido, debe ser UUID"}), 400

    async with get_conn_ctx() as conn:
        result = await conn.execute("""
            DELETE FROM users
            WHERE id = $1::uuid AND is_approved = false
        """, user_id)

    if result.endswith("0"):
        return jsonify({"error": "Usuario no encontrado o ya aprobado"}), 404

    return jsonify({"ok": True, "user_id": user_id, "rejected": True})


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
