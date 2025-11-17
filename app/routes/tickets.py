from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
import datetime
import pytz

tickets_bp = Blueprint("tickets", __name__)


@tickets_bp.route("/workshop/<int:workshop_id>", methods=["GET"])
async def list_my_tickets(workshop_id: int):
    """
    Devuelve los tickets creados por el usuario autenticado para el workshop dado.
    Query params:
      - limit: cantidad máxima (opcional, por defecto 50, máximo 200)
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    try:
        limit = int(request.args.get("limit", 50))
        limit = max(1, min(limit, 200))
    except ValueError:
        limit = 50

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT id, workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
            FROM support_tickets
            WHERE workshop_id = $1 AND created_by_user_id = $2
            ORDER BY created_at DESC
            LIMIT $3
            """,
            workshop_id, user_id, limit
        )

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "workshop_id": r["workshop_id"],
            "created_by_user_id": r["created_by_user_id"],
            "full_name": r["full_name"],
            "phone": r["phone"],
            "subject": r["subject"],
            "description": r["description"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return jsonify({"tickets": items}), 200


@tickets_bp.route("/", methods=["POST"])
@tickets_bp.route("/create", methods=["POST"])
async def create_ticket():
    """
    Crea un nuevo ticket de soporte.
    Body esperado:
      workshop_id: number
      full_name?: string
      phone?: string
      subject: string
      description: string
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()
    workshop_id = data.get("workshop_id")
    subject = (data.get("subject") or "").strip()
    description = (data.get("description") or "").strip()
    full_name = (data.get("full_name") or "").strip() or None
    phone = (data.get("phone") or "").strip() or None

    if not workshop_id:
        return jsonify({"error": "Falta workshop_id"}), 400
    if not subject or not description:
        return jsonify({"error": "Completá asunto y descripción"}), 400

    # Hora local Argentina
    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    now_arg = datetime.datetime.now(argentina_tz)

    async with get_conn_ctx() as conn:
        # Validar que el workshop exista (evitar FK error con un mensaje legible)
        ws_exists = await conn.fetchval("SELECT 1 FROM workshop WHERE id = $1", int(workshop_id))
        if not ws_exists:
            return jsonify({"error": "Taller no encontrado"}), 404

        ticket_id = await conn.fetchval(
            """
            INSERT INTO support_tickets (
                workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, 'Pendiente', $7)
            RETURNING id
            """,
            int(workshop_id), user_id, full_name, phone, subject, description, now_arg
        )

        # Insertar mensaje inicial como enviado por el usuario (si hay descripción)
        if description:
            try:
                await conn.execute(
                    """
                    INSERT INTO support_ticket_messages (ticket_id, sender_user_id, sender_role, content, created_at)
                    VALUES ($1, $2, 'user', $3, $4)
                    """,
                    ticket_id, user_id, description, now_arg
                )
            except Exception:
                # Si la tabla no existe o hay error, no rompemos la creación del ticket
                pass

    return jsonify({"message": "Ticket creado", "ticket_id": ticket_id}), 201


@tickets_bp.route("/<int:ticket_id>", methods=["GET"])
async def get_ticket(ticket_id: int):
    """
    Devuelve el ticket si pertenece al usuario autenticado.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
            FROM support_tickets
            WHERE id = $1 AND created_by_user_id = $2
            """,
            ticket_id, user_id
        )

    if not row:
        return jsonify({"error": "Ticket no encontrado"}), 404

    return jsonify({
        "ticket": {
            "id": row["id"],
            "workshop_id": row["workshop_id"],
            "created_by_user_id": row["created_by_user_id"],
            "full_name": row["full_name"],
            "phone": row["phone"],
            "subject": row["subject"],
            "description": row["description"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        }
    }), 200


@tickets_bp.route("/<int:ticket_id>/messages", methods=["GET"])
async def list_ticket_messages(ticket_id: int):
    """
    Lista los mensajes de un ticket si pertenece al usuario autenticado.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    try:
        limit = int(request.args.get("limit", 200))
        limit = max(1, min(limit, 500))
    except ValueError:
        limit = 200

    async with get_conn_ctx() as conn:
        owner_id = await conn.fetchval(
            "SELECT created_by_user_id FROM support_tickets WHERE id = $1",
            ticket_id,
        )
        if str(owner_id) != str(user_id):
            return jsonify({"error": "No autorizado"}), 403

        rows = await conn.fetch(
            """
            SELECT id, ticket_id, sender_user_id, sender_role, content, created_at
            FROM support_ticket_messages
            WHERE ticket_id = $1
            ORDER BY created_at ASC, id ASC
            LIMIT $2
            """,
            ticket_id, limit
        )

    return jsonify({
        "messages": [
            {
                "id": r["id"],
                "ticket_id": r["ticket_id"],
                "sender_user_id": r["sender_user_id"],
                "sender_role": r["sender_role"],
                "content": r["content"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    }), 200


@tickets_bp.route("/<int:ticket_id>/messages", methods=["POST"])
async def create_ticket_message(ticket_id: int):
    """
    Crea un mensaje en el ticket del usuario autenticado.
    Body: { content: string }
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json()
    content = (data.get("content") or "").strip()
    if not content:
        return jsonify({"error": "Mensaje vacío"}), 400

    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    now_arg = datetime.datetime.now(argentina_tz)

    async with get_conn_ctx() as conn:
        owner_id = await conn.fetchval(
            "SELECT created_by_user_id FROM support_tickets WHERE id = $1",
            ticket_id,
        )
        if str(owner_id) != str(user_id):
            return jsonify({"error": "No autorizado"}), 403

        msg_id = await conn.fetchval(
            """
            INSERT INTO support_ticket_messages (ticket_id, sender_user_id, sender_role, content, created_at)
            VALUES ($1, $2, 'user', $3, $4)
            RETURNING id
            """,
            ticket_id, user_id, content, now_arg
        )

    return jsonify({"message": "Enviado", "id": msg_id}), 201

# ===================== ADMIN ENDPOINTS =====================
async def _is_admin(conn, user_id) -> bool:
    try:
        return bool(await conn.fetchval("SELECT COALESCE(is_admin,false) FROM users WHERE id = $1", user_id))
    except Exception:
        return False

@tickets_bp.route("/admin", methods=["GET"])
async def admin_list_tickets():
    """
    Lista tickets para administradores. Filtros opcionales:
    - status=open|resolved|all (default open: todo menos 'Resuelto' y 'Cerrado')
    - limit: cantidad máxima (por defecto 100, máx 500)
    """
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401

    try:
        limit = int(request.args.get("limit", 100))
        limit = max(1, min(limit, 500))
    except ValueError:
        limit = 100

    status_filter = (request.args.get("status") or "open").lower()

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403

        if status_filter == "resolved":
            rows = await conn.fetch(
                """
                SELECT id, workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
                FROM support_tickets
                WHERE status = 'Resuelto'
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit
            )
        elif status_filter == "all":
            rows = await conn.fetch(
                """
                SELECT id, workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
                FROM support_tickets
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit
            )
        else:
            # open: todo menos Resuelto y Cerrado
            rows = await conn.fetch(
                """
                SELECT id, workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
                FROM support_tickets
                WHERE COALESCE(status, '') NOT IN ('Resuelto', 'Cerrado')
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit
            )

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "workshop_id": r["workshop_id"],
            "created_by_user_id": r["created_by_user_id"],
            "full_name": r["full_name"],
            "phone": r["phone"],
            "subject": r["subject"],
            "description": r["description"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })
    return jsonify({"tickets": items}), 200

@tickets_bp.route("/admin/<int:ticket_id>", methods=["GET"])
async def admin_get_ticket(ticket_id: int):
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401
    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403
        row = await conn.fetchrow(
            """
            SELECT id, workshop_id, created_by_user_id, full_name, phone, subject, description, status, created_at
            FROM support_tickets
            WHERE id = $1
            """,
            ticket_id
        )
    if not row:
        return jsonify({"error": "Ticket no encontrado"}), 404
    return jsonify({
        "ticket": {
            "id": row["id"],
            "workshop_id": row["workshop_id"],
            "created_by_user_id": row["created_by_user_id"],
            "full_name": row["full_name"],
            "phone": row["phone"],
            "subject": row["subject"],
            "description": row["description"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        }
    }), 200

@tickets_bp.route("/admin/<int:ticket_id>/messages", methods=["GET"])
async def admin_list_ticket_messages(ticket_id: int):
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401
    try:
        limit = int(request.args.get("limit", 500))
        limit = max(1, min(limit, 1000))
    except ValueError:
        limit = 500

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403
        rows = await conn.fetch(
            """
            SELECT id, ticket_id, sender_user_id, sender_role, content, created_at
            FROM support_ticket_messages
            WHERE ticket_id = $1
            ORDER BY created_at ASC, id ASC
            LIMIT $2
            """,
            ticket_id, limit
        )
    return jsonify({
        "messages": [
            {
                "id": r["id"],
                "ticket_id": r["ticket_id"],
                "sender_user_id": r["sender_user_id"],
                "sender_role": r["sender_role"],
                "content": r["content"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            } for r in rows
        ]
    }), 200

@tickets_bp.route("/admin/<int:ticket_id>/messages", methods=["POST"])
async def admin_create_ticket_message(ticket_id: int):
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}
    content = (data.get("content") or "").strip()
    if not content:
        return jsonify({"error": "Mensaje vacío"}), 400

    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    now_arg = datetime.datetime.now(argentina_tz)

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403
        msg_id = await conn.fetchval(
            """
            INSERT INTO support_ticket_messages (ticket_id, sender_user_id, sender_role, content, created_at)
            VALUES ($1, $2, 'admin', $3, $4)
            RETURNING id
            """,
            ticket_id, admin_id, content, now_arg
        )
    return jsonify({"message": "Enviado", "id": msg_id}), 201

@tickets_bp.route("/admin/<int:ticket_id>/status", methods=["PATCH", "PUT"])
async def admin_update_ticket_status(ticket_id: int):
    admin_id = g.get("user_id")
    if not admin_id:
        return jsonify({"error": "No autorizado"}), 401
    data = await request.get_json() or {}
    new_status = (data.get("status") or "").strip()
    if not new_status:
        return jsonify({"error": "Falta status"}), 400
    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, admin_id):
            return jsonify({"error": "Requiere admin"}), 403
        row = await conn.fetchrow(
            """
            UPDATE support_tickets
            SET status = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id, status
            """,
            new_status, ticket_id
        )
    if not row:
        return jsonify({"error": "Ticket no encontrado"}), 404
    return jsonify({"ok": True, "ticket_id": row["id"], "status": row["status"]}), 200
