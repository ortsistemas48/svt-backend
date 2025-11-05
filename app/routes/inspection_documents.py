# app/routes/inspection_documents.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from supabase import create_client, Client
import os
import uuid
import re, unicodedata

inspection_docs_bp = Blueprint("inspection_documents", __name__)

# ==== Supabase config local a este archivo ====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_INSPECTION_DOCS = os.getenv("SUPABASE_BUCKET_INSPECTION_DOCS", "inspections")

def _get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _public_url(bucket: str, path: str) -> str:
    base = (SUPABASE_URL or "").rstrip("/")
    return f"{base}/storage/v1/object/public/{bucket}/{path}"

# ==============================================

def _norm_role(raw: str | None) -> str:
    r = (raw or "").strip().lower()
    return r if r in {"global", "step", "owner", "driver", "car", "generic"} else "generic"

def _parse_int(raw: str | None):
    try:
        if raw is None:
            return None
        return int(raw)
    except Exception:
        return None


@inspection_docs_bp.route("/inspections/<int:inspection_id>/documents", methods=["GET"])
async def list_inspection_documents(inspection_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    role_raw = request.args.get("role")
    role = _norm_role(role_raw) if role_raw is not None else None

    step_id = _parse_int(request.args.get("step_id"))

    filters = ["inspection_id = $1"]
    params = [inspection_id]
    if role is not None:
        filters.append("role = $2")
        params.append(role)
    if step_id is not None:
        filters.append(f"step_id = ${len(params)+1}")
        params.append(step_id)

    where_sql = " AND ".join(filters)

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(f"""
            SELECT id, inspection_id, step_id, role,
                   file_name, bucket, object_path, file_url,
                   size_bytes, mime_type, created_at
            FROM inspection_documents
            WHERE {where_sql}
            ORDER BY created_at DESC
        """, *params)

    return jsonify([dict(r) for r in rows]), 200


@inspection_docs_bp.route("/inspections/<int:inspection_id>/documents", methods=["POST"])
async def upload_inspection_documents(inspection_id: int):
    """
    multipart/form-data:
      files: File[]  requerido
      role:  global, step, owner, driver, car, generic  opcional
      step_id: int  opcional, solo si role = 'step' o si querés atarlo a un paso
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    # validar inspección existente
    async with get_conn_ctx() as conn:
        exists = await conn.fetchval("SELECT 1 FROM inspections WHERE id = $1", inspection_id)
        if not exists:
            return jsonify({"error": "Inspección no encontrada"}), 404

    form_data = await request.form
    role = _norm_role(request.args.get("role") or form_data.get("role"))
    step_id = _parse_int(request.args.get("step_id") or form_data.get("step_id"))

    files_md = await request.files
    files = files_md.getlist("files")
    if not files:
        return jsonify({"error": "No se recibieron archivos"}), 400

    if role == "step" and step_id is None:
        return jsonify({"error": "Falta step_id para role = 'step'"}), 400

    client = _get_supabase_client()
    saved = []

    for f in files:
        data = f.read()
        if not isinstance(data, (bytes, bytearray)):
            return jsonify({"error": f"No se pudo leer el archivo {f.filename}"}), 400

        if len(data) > 15 * 1024 * 1024:
            return jsonify({"error": f"El archivo {f.filename} excede 15MB"}), 413

        safe_name = unicodedata.normalize("NFD", (f.filename or "file")).encode("ascii", "ignore").decode("ascii")
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "-", safe_name).strip("-.")

        # ruta en el bucket algo como: inspections/<inspection_id>/<role or step>/<uuid>-<file>
        subfolder = f"step-{step_id}" if (role == "step" and step_id is not None) else role
        dest = f"inspections/{inspection_id}/{subfolder}/{uuid.uuid4().hex}-{safe_name}"

        client.storage.from_(BUCKET_INSPECTION_DOCS).upload(
            path=dest,
            file=data,
            file_options={
                "content_type": f.mimetype or "application/octet-stream",
                "x-upsert": "true",
            },
        )

        file_url = _public_url(BUCKET_INSPECTION_DOCS, dest)

        async with get_conn_ctx() as conn:
            row = await conn.fetchrow("""
                INSERT INTO inspection_documents
                  (inspection_id, step_id, role, file_name, bucket, object_path, file_url,
                   size_bytes, mime_type)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id, inspection_id, step_id, role,
                          file_name, bucket, object_path, file_url,
                          size_bytes, mime_type, created_at
            """, inspection_id, step_id, role, safe_name,
                 BUCKET_INSPECTION_DOCS, dest, file_url, len(data), f.mimetype)
            saved.append(dict(row))

    return jsonify(saved), 201


@inspection_docs_bp.route("/inspections/<int:inspection_id>/documents/<int:doc_id>", methods=["DELETE"])
async def delete_inspection_document(inspection_id: int, doc_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        doc = await conn.fetchrow("""
            SELECT id, bucket, object_path
            FROM inspection_documents
            WHERE id = $1 AND inspection_id = $2
        """, doc_id, inspection_id)
        if not doc:
            return jsonify({"error": "Documento no encontrado"}), 404

        client = _get_supabase_client()
        client.storage.from_(doc["bucket"]).remove([doc["object_path"]])

        await conn.execute("DELETE FROM inspection_documents WHERE id = $1", doc_id)

    return jsonify({"message": "Documento eliminado"}), 200
