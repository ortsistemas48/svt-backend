# app/blueprints/payments.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx

payments_bp = Blueprint("payments", __name__, url_prefix="/payments")

# Estados
PENDING = "PENDING"
IN_REVIEW = "IN_REVIEW"
APPROVED = "APPROVED"
REJECTED = "REJECTED"

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

    if not workshop_id:
        return jsonify({"error": "workshop_id requerido"}), 400
    if quantity < 250 or quantity % 250 != 0:
        return jsonify({"error": "La cantidad mínima es 250 y múltiplo de 250"}), 400
    if zone not in ("SUR", "CENTRO", "NORTE"):
        return jsonify({"error": "Zona inválida"}), 400
    if unit_price <= 0 or amount <= 0:
        return jsonify({"error": "Montos inválidos"}), 400

    async with get_conn_ctx() as conn:
        if not await _user_belongs_to_workshop(conn, user_id, workshop_id):
            return jsonify({"error": "No tenés acceso a este taller"}), 403

        row = await conn.fetchrow(
            """
            INSERT INTO payment_orders (workshop_id, quantity, unit_price, amount, zone, status)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, workshop_id, quantity, unit_price, amount, zone, status, created_at
            """,
            workshop_id, quantity, unit_price, amount, zone, PENDING
        )

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

    async with get_conn_ctx() as conn:
        # reemplazá por tu verificación real de admin
        is_admin = await conn.fetchval("SELECT COALESCE(is_admin, false) FROM users WHERE id = $1", user_id)
        if not is_admin:
            return jsonify({"error": "Requiere admin"}), 403

        exists = await conn.fetchval("SELECT 1 FROM payment_orders WHERE id = $1", order_id)
        if not exists:
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

    return jsonify({"ok": True, "order": dict(row)}), 200
