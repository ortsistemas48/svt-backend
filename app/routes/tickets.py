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

    return jsonify({"message": "Ticket creado", "ticket_id": ticket_id}), 201


