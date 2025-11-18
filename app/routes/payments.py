# app/blueprints/payments.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from app.email import send_payment_order_approved_email, send_admin_payment_order_created_email
import logging
import asyncio

payments_bp = Blueprint("payments", __name__, url_prefix="/payments")
log = logging.getLogger(__name__)

# Estados
PENDING = "PENDING"
IN_REVIEW = "IN_REVIEW"
APPROVED = "APPROVED"
REJECTED = "REJECTED"

# Roles
OWNER_ROLE_ID = 2

# Helpers
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

# Listar órdenes de pago por taller
@payments_bp.route("/orders", methods=["GET"])
async def list_orders():
    workshop_id = request.args.get("workshop_id", type=int)
    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400

    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _user_belongs_to_workshop(conn, user_id, workshop_id):
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        rows = await conn.fetch(
            """
            SELECT id, workshop_id, quantity, unit_price, amount, zone, status, created_at, updated_at
            FROM payment_orders
            WHERE workshop_id = $1
            ORDER BY id DESC
            """,
            workshop_id
        )

    return jsonify([dict(r) for r in rows]), 200

# Crear orden pendiente
@payments_bp.route("/orders", methods=["POST"])
async def create_order():
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    workshop_id = data.get("workshop_id")
    quantity = int(data.get("quantity") or 0)
    zone = (data.get("zone") or "").upper()
    unit_price = float(data.get("unit_price") or 0)
    amount = float(data.get("amount") or 0)
    status_param = (data.get("status") or "").upper()

    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400
    if quantity < 250 or quantity % 250 != 0:
        return jsonify({"error": "La cantidad mínima es 250 y múltiplo de 250"}), 400
    if zone not in ("SUR", "CENTRO", "NORTE"):
        return jsonify({"error": "Zona inválida"}), 400
    if unit_price <= 0 or amount <= 0:
        return jsonify({"error": "Montos inválidos"}), 400

    # Determinar el estado inicial de la orden
    # Si status no se envía o es "PENDING" → crear orden como "Pendiente de pago" (PENDING)
    # Si status es "IN_REVIEW" → crear orden como "Pendiente de acreditación" (IN_REVIEW)
    if not status_param or status_param == "PENDING":
        initial_status = PENDING
    elif status_param == "IN_REVIEW":
        initial_status = IN_REVIEW
    else:
        return jsonify({"error": "status inválido, valores permitidos: PENDING, IN_REVIEW"}), 400

    ws_name = None
    async with get_conn_ctx() as conn:
        if not await _user_belongs_to_workshop(conn, user_id, workshop_id):
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        ws = await conn.fetchrow("SELECT name FROM workshop WHERE id = $1", workshop_id)
        ws_name = ws["name"] if ws else None

        row = await conn.fetchrow(
            """
            INSERT INTO payment_orders (workshop_id, quantity, unit_price, amount, zone, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, workshop_id, quantity, unit_price, amount, zone, status, created_at
            """,
            workshop_id, quantity, unit_price, amount, zone, initial_status
        )

    # Notificar a administradores sobre nueva orden de pago
    try:
        admin_emails = []
        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                "SELECT email FROM users WHERE COALESCE(is_admin,false) = true AND COALESCE(email,'') <> ''"
            )
            admin_emails = [r["email"] for r in rows]
        for em in admin_emails:
            asyncio.create_task(
                send_admin_payment_order_created_email(
                    to_email=em,
                    workshop_name=ws_name or str(workshop_id),
                    workshop_id=workshop_id,
                    order_id=row["id"],
                    quantity=quantity,
                    amount=amount,
                    zone=zone,
                )
            )
    except Exception as e:
        log.exception("No se pudieron encolar notificaciones a admins por nueva orden %s: %s", row["id"], e)

    return jsonify({"ok": True, "order": dict(row)}), 201

# Subir comprobantes y pasar a IN_REVIEW
@payments_bp.route("/orders/<int:order_id>/documents", methods=["POST"])
async def upload_documents(order_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        order = await conn.fetchrow(
            "SELECT id, workshop_id, status FROM payment_orders WHERE id = $1",
            order_id
        )
        if not order:
            return jsonify({"error": "Orden no encontrada"}), 404
        if not await _user_belongs_to_workshop(conn, user_id, order["workshop_id"]):
            return jsonify({"error": "No tenés acceso a este taller"}), 403

    form = await request.files
    files = form.getlist("files") if form else []
    if not files:
        return jsonify({"error": "Adjuntá al menos un archivo"}), 400

    saved = []
    async with get_conn_ctx() as conn:
        async with conn.transaction():
            for f in files:
                name = f.filename or "comprobante"
                mime = f.mimetype or "application/octet-stream"
                data = await f.read()
                size = len(data)

                doc = await conn.fetchrow(
                    """
                    INSERT INTO payment_documents (order_id, file_name, mime_type, size_bytes)
                    VALUES ($1, $2, $3, $4)
                    RETURNING id, file_name, mime_type, size_bytes, created_at
                    """,
                    order_id, name, mime, size
                )
                saved.append(dict(doc))

            await conn.execute(
                "UPDATE payment_orders SET status = $1, updated_at = NOW() WHERE id = $2",
                IN_REVIEW, order_id
            )

    return jsonify({"ok": True, "saved": saved}), 201

# Admin, cambiar estado de una orden
@payments_bp.route("/admin/orders/<int:order_id>/status", methods=["PATCH"])
async def admin_set_status(order_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    new_status = (data.get("status") or "").upper()
    if new_status not in (APPROVED, REJECTED, IN_REVIEW, PENDING):
        return jsonify({"error": "Estado inválido"}), 400

    owner_emails = []
    ws_name = None
    ws_id = None
    qty = None
    should_notify = False

    async with get_conn_ctx() as conn:
        is_admin = await conn.fetchval("SELECT COALESCE(is_admin, false) FROM users WHERE id = $1", user_id)
        if not is_admin:
            return jsonify({"error": "Requiere admin"}), 403

        current = await conn.fetchrow("SELECT workshop_id, status, quantity FROM payment_orders WHERE id = $1", order_id)
        if not current:
            return jsonify({"error": "Orden no encontrada"}), 404

        row = await conn.fetchrow(
            """
            UPDATE payment_orders
            SET status = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id, workshop_id, quantity, unit_price, amount, zone, status, created_at, updated_at
            """,
            new_status, order_id
        )

        # Preparar datos para notificación por email si corresponde
        if new_status == APPROVED and (current["status"] or "").upper() != APPROVED:
            ws_id = current["workshop_id"]
            qty = int(current["quantity"] or 0)

            # Obtener emails de los OWNERS del taller
            owners = await conn.fetch(
                """
                SELECT u.email
                FROM workshop_users wu
                JOIN users u ON u.id = wu.user_id
                WHERE wu.workshop_id = $1 AND wu.user_type_id = $2 AND COALESCE(u.email, '') <> ''
                """,
                ws_id, OWNER_ROLE_ID
            )
            owner_emails = [r["email"] for r in owners]

            # Nombre del taller
            ws = await conn.fetchrow("SELECT name FROM workshop WHERE id = $1", ws_id)
            ws_name = ws["name"] if ws else None

            should_notify = bool(owner_emails and ws_name)

    # Enviar emails fuera de la transacción
    if should_notify:
        for em in owner_emails:
            try:
                await send_payment_order_approved_email(
                    to_email=em,
                    workshop_name=ws_name,
                    quantity=qty or 0,
                    workshop_id=ws_id,
                )
            except Exception as e:
                log.exception("No se pudo enviar email de pago aprobado a %s, error: %s", em, e)

    return jsonify({"ok": True, "order": dict(row)}), 200
