from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from uuid import UUID
from app.email import send_assigned_to_workshop_email
import logging
import os

log = logging.getLogger(__name__)

users_bp = Blueprint("users", __name__, url_prefix="/users")

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

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
                u.license_number,
                wu.engineer_kind,
                NULL::int AS user_type_id   -- compat: ya no está en workshop_users
            FROM users u
            LEFT JOIN workshop_users wu
              ON wu.user_id = u.id
             AND wu.workshop_id = $2
            WHERE LOWER(u.email) = LOWER($1)
            LIMIT 1
            """,
            email, workshop_id
        )

    return jsonify(dict(row) if row else None)


@users_bp.route("/by-email-lite", methods=["GET"])
async def get_user_by_email_lite():
    email = request.args.get("email")

    if not email:
        return jsonify({"error": "email requerido"}), 400
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
                u.license_number
            FROM users u
            WHERE LOWER(u.email) = LOWER($1)
            LIMIT 1
            """,
            email
        )
    return jsonify(dict(row) if row else None)


@users_bp.route("/get_users/workshop/<int:workshop_id>", methods=["GET"])
async def get_users_in_workshop(workshop_id: int):
    async with get_conn_ctx() as conn:
        users = await conn.fetch("""
            SELECT 
                u.id, 
                u.first_name, 
                u.last_name, 
                u.email, 
                u.dni, 
                u.phone_number, 
                u.title_name, 
                u.license_number, 
                ut.name AS role,
                wu.engineer_kind
            FROM workshop_users wu
            JOIN users u ON wu.user_id = u.id
            JOIN user_types ut ON wu.user_type_id = ut.id
            WHERE wu.workshop_id = $1 AND u.deleted_at IS NULL;

        """, workshop_id)

    return jsonify({
        "workshop_id": workshop_id,
        "users": [dict(user) for user in users]
    })
    

@users_bp.route("/delete/<user_id>", methods=["POST", "OPTIONS"])
async def soft_delete_user(user_id: str):
    # Preflight CORS
    if request.method == "OPTIONS":
        return ("", 204)

    # Validar UUID si aplica, si tu users.id es integer podés omitir esto
    try:
        _as_uuid(user_id)
        id_value = f"{user_id}"
        cast = "::uuid"
    except ValueError:
        # Soporta también ids numéricos si tu tabla no usa UUID
        if not user_id.isdigit():
            return jsonify({"error": "user_id inválido"}), 400
        id_value = int(user_id)
        cast = "::int"

    async with get_conn_ctx() as conn:
        result = await conn.execute(f"""
            UPDATE users
               SET deleted_at = NOW()
             WHERE id = $1{cast}
               AND deleted_at IS NULL
        """, id_value)

    # asyncpg devuelve por ejemplo "UPDATE 1" o "UPDATE 0"
    if result.endswith("0"):
        return jsonify({"error": "Usuario no encontrado o ya eliminado"}), 404

    return jsonify({"ok": True, "user_id": user_id, "deleted": True}), 200


@users_bp.route("/restore/<user_id>", methods=["POST", "OPTIONS"])
async def restore_user(user_id: str):
    # Preflight CORS
    if request.method == "OPTIONS":
        return ("", 204)

    # Validar UUID si aplica
    try:
        _as_uuid(user_id)
        id_value = f"{user_id}"
        cast = "::uuid"
    except ValueError:
        if not user_id.isdigit():
            return jsonify({"error": "user_id inválido"}), 400
        id_value = int(user_id)
        cast = "::int"

    async with get_conn_ctx() as conn:
        result = await conn.execute(f"""
            UPDATE users
               SET deleted_at = NULL
             WHERE id = $1{cast}
               AND deleted_at IS NOT NULL
        """, id_value)

    if result.endswith("0"):
        return jsonify({"error": "Usuario no encontrado o no está suspendido"}), 404

    return jsonify({"ok": True, "user_id": user_id, "restored": True}), 200


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
            WHERE u.deleted_at IS NULL
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
    
    
@users_bp.route("/get_users/suspended", methods=["GET"])
async def get_suspended_users():
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
                u.phone_number,
                u.deleted_at,
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
            WHERE u.deleted_at IS NOT NULL
            GROUP BY u.id, u.first_name, u.last_name, u.email, u.dni, u.phone_number, u.deleted_at
            ORDER BY u.id
            LIMIT $1 OFFSET $2;
            """,
            limit,
            offset,
        )

        total_row = await conn.fetchrow("SELECT COUNT(*) AS total FROM users WHERE deleted_at IS NOT NULL;")
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
                "deleted_at": r["deleted_at"],
                "memberships": r["memberships"],
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
            WHERE u.is_approved = false AND u.deleted_at IS NULL
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
    """
    Asigna (o actualiza) un usuario a un taller con un rol dado.
    Si el rol es Ingeniero (id=3), requiere:
      - engineer_kind: "Titular" | "Suplente"
    Regla de negocio: a lo sumo 1 Ingeniero Titular por taller.
    """
    try:
        data = await request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"error": "Body JSON inválido"}), 400

    user_id = (data or {}).get("user_id")            # UUID string
    user_type_id = (data or {}).get("user_type_id")  # int
    engineer_kind = (data or {}).get("engineer_kind")  # "Titular" | "Suplente" | None

    if not user_id or user_type_id is None:
        return jsonify({"error": "user_id y user_type_id son requeridos"}), 400

    # valida UUID
    try:
        _as_uuid(user_id)
    except ValueError:
        return jsonify({"error": "user_id inválido, debe ser UUID"}), 400

    # valida tipo de rol
    try:
        user_type_id = int(user_type_id)
    except (TypeError, ValueError):
        return jsonify({"error": "user_type_id debe ser numérico"}), 400

    ENGINEER_ROLE_ID = 3
    if user_type_id == ENGINEER_ROLE_ID:
        if isinstance(engineer_kind, str):
            engineer_kind = engineer_kind.strip()
        if engineer_kind not in ("Titular", "Suplente"):
            return jsonify({"error": "engineer_kind debe ser 'Titular' o 'Suplente'"}), 400

    inviter_id = g.get("user_id")  # opcional, quién asigna

    async with get_conn_ctx() as conn:
        # 1) validar workshop
        ws = await conn.fetchrow(
            "SELECT id, name FROM workshop WHERE id = $1",
            workshop_id
        )
        if not ws:
            return jsonify({"error": "Workshop no encontrado"}), 404

        # 2) usuario
        u = await conn.fetchrow(
            "SELECT id, email, first_name, last_name FROM users WHERE id = $1::uuid",
            user_id
        )
        if not u or not u["email"]:
            return jsonify({"error": "Usuario no encontrado o sin email"}), 400

        # 3) rol
        role = await conn.fetchrow(
            "SELECT id, name FROM user_types WHERE id = $1",
            user_type_id
        )
        if not role:
            return jsonify({"error": "user_type_id inválido"}), 400

        # 3.1) Si es Ingeniero Titular, validar unicidad por taller
        if user_type_id == ENGINEER_ROLE_ID and engineer_kind == "Titular":
            exists_titular = await conn.fetchval(
                """
                SELECT 1
                  FROM workshop_users
                 WHERE workshop_id = $1
                   AND user_type_id = $2
                   AND engineer_kind = 'Titular'
                   AND user_id <> $3::uuid
                 LIMIT 1
                """,
                workshop_id, ENGINEER_ROLE_ID, user_id
            )
            if exists_titular:
                return jsonify({"error": "Ya existe un Ingeniero Titular asignado a este taller"}), 409

        # 4) upsert de membresía
        # Tabla workshop_users: (workshop_id int, user_id uuid, user_type_id int, engineer_kind text NULL)
        if user_type_id == ENGINEER_ROLE_ID:
            await conn.execute(
                """
                INSERT INTO workshop_users (workshop_id, user_id, user_type_id, engineer_kind)
                VALUES ($1, $2::uuid, $3, $4)
                ON CONFLICT (workshop_id, user_id)
                DO UPDATE SET
                    user_type_id  = EXCLUDED.user_type_id,
                    engineer_kind = EXCLUDED.engineer_kind
                """,
                workshop_id, user_id, user_type_id, engineer_kind
            )
        else:
            # Para roles no-ingeniero, limpiamos engineer_kind
            await conn.execute(
                """
                INSERT INTO workshop_users (workshop_id, user_id, user_type_id, engineer_kind)
                VALUES ($1, $2::uuid, $3, NULL)
                ON CONFLICT (workshop_id, user_id)
                DO UPDATE SET
                    user_type_id  = EXCLUDED.user_type_id,
                    engineer_kind = NULL
                """,
                workshop_id, user_id, user_type_id
            )

        # 5) nombre del invitador (opcional)
        inviter_name = None
        if inviter_id:
            inv = await conn.fetchrow(
                "SELECT first_name, last_name FROM users WHERE id = $1",
                inviter_id
            )
            inviter_name = f'{inv["first_name"]} {inv["last_name"]}'.strip() if inv else None

        ws_name = ws["name"]
        assignee_email = u["email"]
        role_name = role["name"]

    # 6) email fuera de la transacción
    try:
        workshop_url = f"{FRONTEND_URL}/dashboard/{workshop_id}"
        extra_role = f" ({engineer_kind})" if (user_type_id == ENGINEER_ROLE_ID and engineer_kind) else ""
        await send_assigned_to_workshop_email(
            to_email=assignee_email,
            workshop_name=ws_name,
            role_name=f"{role_name}{extra_role}",
            inviter_name=inviter_name,
            workshop_url=workshop_url,
        )
    except Exception as e:
        log.exception("No se pudo enviar email de asignación a %s, error: %s", assignee_email, e)

    return jsonify({"ok": True})


@users_bp.route("/user-type-in-workshop", methods=["GET"])
async def get_user_type_in_workshop():
    user_id = request.args.get("userId")
    workshop_id = request.args.get("workshopId", type=int)

    if not user_id or not workshop_id:
        return jsonify({"error": "userId y workshopId son requeridos"}), 400

    # valida UUID
    try:
        _as_uuid(user_id)
    except ValueError:
        return jsonify({"error": "userId inválido, debe ser UUID"}), 400

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT ut.id, ut.name
            FROM workshop_users wu
            JOIN user_types ut ON wu.user_type_id = ut.id
            WHERE wu.user_id = $1::uuid AND wu.workshop_id = $2
            """,
            user_id, workshop_id
        )

    if not row:
        return jsonify({"error": "No se encontró el usuario en el workshop"}), 404

    return jsonify(dict(row)), 200
