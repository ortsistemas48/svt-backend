# app/routes/application_documents.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from supabase import create_client, Client
import os
import uuid

docs_bp = Blueprint("application_documents", __name__)

# ==== Supabase config dentro de ESTE archivo ====
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_DOCS  = os.getenv("SUPABASE_BUCKET_DOCS", "certificados")

def _get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _public_url(bucket: str, path: str) -> str:
    base = (SUPABASE_URL or "").rstrip("/")
    return f"{base}/storage/v1/object/public/{bucket}/{path}"

# ================================================

def _norm_role(raw: str | None) -> str:
    r = (raw or "").strip().lower()
    return r if r in {"owner", "driver", "car", "generic"} else "generic"

@docs_bp.route("/applications/<int:app_id>/documents", methods=["GET"])
async def list_documents(app_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    role_raw = request.args.get("role")
    role = _norm_role(role_raw)
    filter_sql = ""
    params = [app_id]
    if role_raw is not None:
        filter_sql = " AND role = $2"
        params.append(role)

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(f"""
            SELECT id, application_id, file_name, bucket, object_path, file_url,
                   size_bytes, mime_type, role, created_at
            FROM application_documents
            WHERE application_id = $1{filter_sql}
            ORDER BY created_at DESC
        """, *params)

    return jsonify([dict(r) for r in rows]), 200


@docs_bp.route("/applications/<int:app_id>/documents", methods=["POST"])
async def upload_documents(app_id: int):
    """
    multipart/form-data:
      files: File[]  requerido
      role:  owner, driver, car, generic  opcional, form o query
    """
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    # validar aplicación
    async with get_conn_ctx() as conn:
        exists = await conn.fetchval("SELECT 1 FROM applications WHERE id = $1", app_id)
        if not exists:
            return jsonify({"error": "Trámite no encontrado"}), 404

    form_data = await request.form
    role = _norm_role(request.args.get("role") or form_data.get("role"))

    files_md = await request.files
    files = files_md.getlist("files")
    if not files:
        return jsonify({"error": "No se recibieron archivos"}), 400

    client = _get_supabase_client()
    saved = []

    for f in files:
        # leer bytes en modo síncrono
        data = f.read()
        if not isinstance(data, (bytes, bytearray)):
            return jsonify({"error": f"No se pudo leer el archivo {f.filename}"}), 400

        if len(data) > 15 * 1024 * 1024:
            return jsonify({"error": f"El archivo {f.filename} excede 15MB"}), 413

        safe_name = f.filename.replace("/", "_").replace("\\", "_")
        dest = f"apps/{app_id}/{role}/{uuid.uuid4().hex}-{safe_name}"

        client.storage.from_(BUCKET_DOCS).upload(
            path=dest,
            file=data,
            file_options={
                "content_type": f.mimetype or "application/octet-stream",
                "x-upsert": "true",
            },
        )

        file_url = _public_url(BUCKET_DOCS, dest)

        async with get_conn_ctx() as conn:
            row = await conn.fetchrow("""
                INSERT INTO application_documents
                  (application_id, file_name, bucket, object_path, file_url,
                   size_bytes, mime_type, role)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id, application_id, file_name, bucket, object_path, file_url,
                          size_bytes, mime_type, role, created_at
            """, app_id, safe_name, BUCKET_DOCS, dest, file_url, len(data), f.mimetype, role)
            saved.append(dict(row))

    return jsonify(saved), 201


@docs_bp.route("/applications/<int:app_id>/documents/<int:doc_id>", methods=["DELETE"])
async def delete_document(app_id: int, doc_id: int):
    user_id = g.get("user_id")
    if not user_id:
        return jsonify({"error": "No autorizado"}), 401

    async with get_conn_ctx() as conn:
        doc = await conn.fetchrow("""
            SELECT id, object_path, bucket
            FROM application_documents
            WHERE id = $1 AND application_id = $2
        """, doc_id, app_id)
        if not doc:
            return jsonify({"error": "Documento no encontrado"}), 404

        client = _get_supabase_client()
        client.storage.from_(doc["bucket"]).remove([doc["object_path"]])

        await conn.execute("DELETE FROM application_documents WHERE id = $1", doc_id)

    return jsonify({"message": "Documento eliminado"}), 200
