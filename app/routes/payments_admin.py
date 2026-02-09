# app/blueprints/payments_admin.py
from quart import Blueprint, request, jsonify, g, Response
from app.db import get_conn_ctx
from supabase import create_client, Client
import os
import logging
import httpx

payments_admin_bp = Blueprint("payments_admin", __name__, url_prefix="/payments/admin")
logger = logging.getLogger("payments_admin")

PENDING = "PENDING"
IN_REVIEW = "IN_REVIEW"
APPROVED = "APPROVED"
REJECTED = "REJECTED"
VALID_STATES = (PENDING, IN_REVIEW, APPROVED, REJECTED)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

async def _is_admin(conn, user_id: int) -> bool:
    return await conn.fetchval("SELECT COALESCE(is_admin, false) FROM users WHERE id = $1", user_id)

def _parse_int(v, default=None):
    try:
        return int(v)
    except Exception:
        return default

def _log_ctx(**kw):
    """Pequeño helper para serializar contexto en logs."""
    # evita None y deja todo como clave=valor
    return ", ".join(f"{k}={v}" for k, v in kw.items() if v is not None)

@payments_admin_bp.route("/orders", methods=["GET"])
async def admin_list_orders():
    """
    Lista de órdenes con filtros opcionales y paginado.
    Query params:
      status, workshop_id, q, page, page_size
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, user_id):
            return jsonify({"error": "Requiere admin"}), 403

        status = (request.args.get("status") or "").upper()
        if status and status not in VALID_STATES:
            return jsonify({"error": "Estado inválido"}), 400

        workshop_id = _parse_int(request.args.get("workshop_id"))
        q = (request.args.get("q") or "").strip()

        page = max(1, _parse_int(request.args.get("page"), 1) or 1)
        page_size = min(200, max(1, _parse_int(request.args.get("page_size"), 20) or 20))
        offset = (page - 1) * page_size

        # build dinámico
        where = ["1=1"]
        params = []

        if status:
            where.append(f"po.status = ${len(params) + 1}")
            params.append(status)

        if workshop_id:
            where.append(f"po.workshop_id = ${len(params) + 1}")
            params.append(workshop_id)

        if q:
            like = f"%{q.lower()}%"
            # sumamos búsqueda por nombre del taller
            where.append(
                "("
                f"CAST(po.id AS TEXT) ILIKE ${len(params) + 1} OR "
                f"CAST(po.workshop_id AS TEXT) ILIKE ${len(params) + 2} OR "
                f"CAST(po.quantity AS TEXT) ILIKE ${len(params) + 3} OR "
                f"CAST(po.amount AS TEXT) ILIKE ${len(params) + 4} OR "
                f"LOWER(po.zone) LIKE ${len(params) + 5} OR "
                f"LOWER(COALESCE(w.name, '')) LIKE ${len(params) + 6}"
                ")"
            )
            params.extend([like, like, like, like, like, like])

        where_sql = " AND ".join(where)

        # total con join para que funcione el filtro por nombre
        total = await conn.fetchval(
            f"""
            SELECT COUNT(*)
            FROM payment_orders po
            LEFT JOIN workshop w ON w.id = po.workshop_id
            WHERE {where_sql}
            """,
            *params
        )

        rows = await conn.fetch(
            f"""
            SELECT
              po.id,
              po.workshop_id,
              w.name AS workshop_name,
              po.quantity,
              po.unit_price,
              po.amount,
              po.zone,
              po.status,
              po.created_at,
              po.updated_at,
              po.receipt_url,
              po.receipt_mime,
              po.receipt_size,
              po.receipt_uploaded_at,
              COALESCE(doc_counts.cnt, 0) AS document_count
            FROM payment_orders po
            LEFT JOIN workshop w ON w.id = po.workshop_id
            LEFT JOIN (
              SELECT order_id, COUNT(*) AS cnt
              FROM payment_documents
              GROUP BY order_id
            ) doc_counts ON doc_counts.order_id = po.id
            WHERE {where_sql}
            ORDER BY po.id DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """,
            *params, page_size, offset
        )

    return jsonify({
        "items": [dict(r) for r in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
    }), 200

@payments_admin_bp.route("/orders/<int:order_id>", methods=["GET"])
async def admin_get_order(order_id: int):
    """
    Devuelve la orden y sus documentos.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, user_id):
            return jsonify({"error": "Requiere admin"}), 403

        order = await conn.fetchrow(
            """
            SELECT
              id, workshop_id, quantity, unit_price, amount, zone, status,
              created_at, updated_at,
              receipt_url, receipt_mime, receipt_size, receipt_uploaded_at
            FROM payment_orders
            WHERE id = $1
            """,
            order_id
        )
        if not order:
            return jsonify({"error": "Orden no encontrada"}), 404

        docs = await conn.fetch(
            """
            SELECT id, file_name, mime_type, size_bytes, created_at
            FROM payment_documents
            WHERE order_id = $1
            ORDER BY id ASC
            """,
            order_id
        )

    return jsonify({
        "order": dict(order),
        "documents": [dict(d) for d in docs],
    }), 200

@payments_admin_bp.route("/orders/<int:order_id>/status", methods=["PATCH"])
async def admin_set_status(order_id: int):
    user_id = g.get("user_id")
    if not user_id:
        logger.warning("admin_set_status, no autorizado, %s", _log_ctx(order_id=order_id))
        return jsonify({"error": "No autorizado"}), 401

    data = await request.get_json() or {}
    new_status = (data.get("status") or "").upper()
    if new_status not in VALID_STATES:
        logger.warning("admin_set_status, estado invalido, %s", _log_ctx(user_id=user_id, order_id=order_id, new_status=new_status))
        return jsonify({"error": "Estado inválido"}), 400

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, user_id):
            logger.warning("admin_set_status, requiere admin, %s", _log_ctx(user_id=user_id, order_id=order_id))
            return jsonify({"error": "Requiere admin"}), 403

        try:
            async with conn.transaction():
                # Bloquear la orden
                order = await conn.fetchrow(
                    """
                    SELECT id, workshop_id, quantity, status
                    FROM payment_orders
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    order_id
                )
                if not order:
                    logger.warning("admin_set_status, orden no encontrada, %s", _log_ctx(user_id=user_id, order_id=order_id))
                    return jsonify({"error": "Orden no encontrada"}), 404

                prev_status = order["status"]
                workshop_id = order["workshop_id"]
                qty = int(order["quantity"] or 0)

                logger.info(
                    "admin_set_status, empezando, %s",
                    _log_ctx(user_id=user_id, order_id=order_id, workshop_id=workshop_id, prev_status=prev_status, new_status=new_status, qty=qty),
                )

                # Actualizar estado
                row = await conn.fetchrow(
                    """
                    UPDATE payment_orders
                    SET status = $1, updated_at = NOW()
                    WHERE id = $2
                    RETURNING id, workshop_id, quantity, unit_price, amount, zone, status,
                              created_at, updated_at, receipt_url, receipt_mime, receipt_size, receipt_uploaded_at
                    """,
                    new_status, order_id
                )

                # Delta para available_inspections
                delta = 0
                if new_status == APPROVED and prev_status != APPROVED:
                    delta = qty
                elif prev_status == APPROVED and new_status != APPROVED:
                    delta = -qty

                updated_workshop = None
                if delta != 0:
                    updated_workshop = await conn.fetchrow(
                        """
                        UPDATE workshop
                        SET available_inspections = COALESCE(available_inspections, 0) + $1
                        WHERE id = $2
                        RETURNING id, available_inspections
                        """,
                        delta, workshop_id
                    )
                    logger.info(
                        "admin_set_status, aplicado delta, %s",
                        _log_ctx(order_id=order_id, workshop_id=workshop_id, delta=delta, new_available=updated_workshop["available_inspections"] if updated_workshop else "NA"),
                    )
                else:
                    logger.info(
                        "admin_set_status, sin cambios en available_inspections, %s",
                        _log_ctx(order_id=order_id, prev_status=prev_status, new_status=new_status, qty=qty),
                    )

        except Exception as e:
            logger.exception("admin_set_status, error inesperado, %s", _log_ctx(user_id=user_id, order_id=order_id))
            return jsonify({"error": "Error interno"}), 500

    logger.info(
        "admin_set_status, ok, %s",
        _log_ctx(user_id=user_id, order_id=order_id, new_status=new_status),
    )

    return jsonify({
        "ok": True,
        "order": dict(row),
        "workshop": dict(updated_workshop) if updated_workshop else None,
    }), 200


@payments_admin_bp.route("/orders/<int:order_id>/receipt/download", methods=["GET"])
async def admin_download_receipt(order_id: int):
    """
    Descarga el comprobante de pago a través del backend (proxy).
    Evita problemas de CORS y permite descargar directamente.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        if not await _is_admin(conn, user_id):
            return jsonify({"error": "Requiere admin"}), 403

        order = await conn.fetchrow(
            "SELECT receipt_url, receipt_mime FROM payment_orders WHERE id = $1",
            order_id,
        )
        if not order:
            return jsonify({"error": "Orden no encontrada"}), 404

    receipt_url = order["receipt_url"]
    if not receipt_url:
        return jsonify({"error": "La orden no tiene comprobante cargado"}), 400

    mime = order["receipt_mime"] or "application/octet-stream"

    # Derivar bucket y path desde la URL pública
    try:
        prefix = "/storage/v1/object/public/"
        idx = receipt_url.index(prefix)
        bucket_and_path = receipt_url[idx + len(prefix):]
        bucket = bucket_and_path.split("/")[0]
        object_path = "/".join(bucket_and_path.split("/")[1:])
    except Exception:
        return jsonify({"error": "No se pudo parsear la URL del comprobante"}), 500

    # Descargar el archivo desde Supabase usando service role key
    try:
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        file_bytes = client.storage.from_(bucket).download(object_path)
    except Exception as e:
        logger.exception("admin_download_receipt, error descargando de Supabase, %s", _log_ctx(order_id=order_id))
        return jsonify({"error": "No se pudo descargar el archivo"}), 502

    # Extraer nombre legible del path (última parte después del uuid)
    raw_name = object_path.rsplit("/", 1)[-1] if "/" in object_path else object_path
    # Quitar prefijo UUID (32 hex chars + guión)
    if len(raw_name) > 33 and raw_name[32] == "-":
        raw_name = raw_name[33:]
    if not raw_name:
        raw_name = f"comprobante-orden-{order_id}"

    ext_map = {
        "application/pdf": ".pdf",
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/webp": ".webp",
    }
    if "." not in raw_name:
        raw_name += ext_map.get(mime, "")

    return Response(
        file_bytes,
        status=200,
        headers={
            "Content-Type": mime,
            "Content-Disposition": f'attachment; filename="{raw_name}"',
            "Content-Length": str(len(file_bytes)),
        },
    )
