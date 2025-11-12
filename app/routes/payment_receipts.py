from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from supabase import create_client, Client
import os
import uuid
import datetime as dt
import unicodedata
import re

payment_receipts_bp = Blueprint("payment_receipts", __name__, url_prefix="/payments")

# Estados de órdenes de pago
PENDING = "PENDING"
IN_REVIEW = "IN_REVIEW"

# ===== Supabase config local a este archivo =====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_DOCS = os.getenv("SUPABASE_BUCKET_DOCS", "certificados")  # usa tu bucket existente

def _get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _public_url(bucket: str, path: str) -> str:
    base = (SUPABASE_URL or "").rstrip("/")
    return f"{base}/storage/v1/object/public/{bucket}/{path}"

# ===== Helpers de acceso =====
async def _is_admin(conn, user_id: int) -> bool:
    return await conn.fetchval("SELECT COALESCE(is_admin, false) FROM users WHERE id = $1", user_id)

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

# ===== Endpoints =====

@payment_receipts_bp.route("/orders/<int:order_id>/receipt", methods=["POST"])
async def upload_payment_receipt(order_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    # validar orden y permisos
    async with get_conn_ctx() as conn:
        order = await conn.fetchrow(
            """
            SELECT id, workshop_id, status
            FROM payment_orders
            WHERE id = $1
            """,
            order_id
        )
        if not order:
            return jsonify({"error": "Orden no encontrada"}), 404

        ws_id = order["workshop_id"]
        is_admin = await _is_admin(conn, user_id)
        if not is_admin:
            belongs = await _user_belongs_to_workshop(conn, user_id, ws_id)
            if not belongs:
                return jsonify({"error": "No tenés acceso a la orden"}), 403

    # leer archivo
    form = await request.files
    files = form.getlist("file") or form.getlist("files")
    if not files:
        return jsonify({"error": "No se recibió archivo"}), 400
    if len(files) > 1:
        return jsonify({"error": "Solo se admite un archivo de comprobante"}), 400

    f = files[0]
    data = f.read()  # <- sync, devuelve bytes
    if not isinstance(data, (bytes, bytearray)):
        return jsonify({"error": f"No se pudo leer el archivo {f.filename}"}), 400

    if len(data) > 15 * 1024 * 1024:
        return jsonify({"error": "El archivo excede 15MB"}), 413

    mime = (f.mimetype or "application/octet-stream").lower()
    allowed = {"image/png", "image/jpeg", "image/webp", "application/pdf"}
    if mime not in allowed:
        return jsonify({"error": "Formato inválido, permitidos, PNG, JPG, WEBP o PDF"}), 415

    client = _get_supabase_client()
    
    # Sanitizar nombre del archivo para evitar problemas de encoding
    # Normalizar caracteres Unicode y eliminar caracteres no ASCII
    safe_name = unicodedata.normalize("NFD", (f.filename or "comprobante")).encode("ascii", "ignore").decode("ascii")
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", safe_name).strip("-.")
    if not safe_name:
        safe_name = "comprobante"
    
    dest = f"comprobantes/payments/{order_id}/{uuid.uuid4().hex}-{safe_name}"

    # file_options: usar content_type (no content-type) y x-upsert como string
    file_options = {
        "content_type": mime,
        "x-upsert": "true",
    }

    client.storage.from_(BUCKET_DOCS).upload(
        path=dest,
        file=data,
        file_options=file_options,
    )

    url = _public_url(BUCKET_DOCS, dest)
    # guardar en la orden y cambiar estado de PENDING a IN_REVIEW si corresponde
    # Reutilizamos el estado obtenido anteriormente en la validación
    current_status = order["status"]
    
    async with get_conn_ctx() as conn:
        # Si el estado actual es PENDING, cambiar a IN_REVIEW al subir el comprobante
        if current_status == PENDING:
            row = await conn.fetchrow(
                """
                UPDATE payment_orders
                SET
                  receipt_url = $1,
                  receipt_mime = $2,
                  receipt_size = $3,
                  receipt_uploaded_at = NOW(),
                  status = $4,
                  updated_at = NOW()
                WHERE id = $5
                RETURNING id, workshop_id, status, receipt_url, receipt_mime, receipt_size, receipt_uploaded_at
                """,
                url, mime, len(data), IN_REVIEW, order_id
            )
        else:
            # Si no es PENDING, solo actualizar los campos del comprobante sin cambiar el estado
            row = await conn.fetchrow(
                """
                UPDATE payment_orders
                SET
                  receipt_url = $1,
                  receipt_mime = $2,
                  receipt_size = $3,
                  receipt_uploaded_at = NOW(),
                  updated_at = NOW()
                WHERE id = $4
                RETURNING id, workshop_id, status, receipt_url, receipt_mime, receipt_size, receipt_uploaded_at
                """,
                url, mime, len(data), order_id
            )

    return jsonify({
        "message": "Comprobante subido",
        "order": {
            "id": row["id"],
            "workshop_id": row["workshop_id"],
            "status": row["status"],
            "receipt_url": row["receipt_url"],
            "receipt_mime": row["receipt_mime"],
            "receipt_size": row["receipt_size"],
            "receipt_uploaded_at": row["receipt_uploaded_at"].isoformat()
                if isinstance(row["receipt_uploaded_at"], dt.datetime)
                else row["receipt_uploaded_at"],
        }
    }), 201


@payment_receipts_bp.route("/orders/<int:order_id>/receipt", methods=["DELETE"])
async def delete_payment_receipt(order_id: int):
    """
    Elimina el comprobante de Supabase y limpia los campos de la orden.
    Requiere admin o pertenecer al taller dueño de la orden.
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT po.id, po.workshop_id, po.receipt_url, po.status
            FROM payment_orders po
            WHERE po.id = $1
            """,
            order_id
        )
        if not row:
            return jsonify({"error": "Orden no encontrada"}), 404

        is_admin = await _is_admin(conn, user_id)
        if not is_admin:
            belongs = await _user_belongs_to_workshop(conn, user_id, row["workshop_id"])
            if not belongs:
                return jsonify({"error": "No tenés acceso a la orden"}), 403

        receipt_url = row["receipt_url"]

    if not receipt_url:
        return jsonify({"error": "La orden no tiene comprobante cargado"}), 400

    # derivar bucket y path desde la url pública
    try:
      # url pública, .../storage/v1/object/public/{bucket}/{path}
      prefix = "/storage/v1/object/public/"
      idx = receipt_url.index(prefix)
      bucket_and_path = receipt_url[idx + len(prefix):]
      bucket = bucket_and_path.split("/")[0]
      object_path = "/".join(bucket_and_path.split("/")[1:])
    except Exception:
      # fallback si no se puede parsear
      bucket = BUCKET_DOCS
      # intentamos extraer a partir de 'comprobantes/'
      marker = "comprobantes/"
      object_path = receipt_url.split(marker, 1)[-1]
      object_path = f"comprobantes/{object_path}"

    client = _get_supabase_client()
    client.storage.from_(bucket).remove([object_path])

    async with get_conn_ctx() as conn:
        await conn.execute(
            """
            UPDATE payment_orders
            SET
              receipt_url = NULL,
              receipt_mime = NULL,
              receipt_size = NULL,
              receipt_uploaded_at = NULL,
              updated_at = NOW()
            WHERE id = $1
            """,
            order_id
        )

    return jsonify({"ok": True, "message": "Comprobante eliminado"}), 200
