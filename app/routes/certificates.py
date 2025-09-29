# app/routes/certificates.py
from quart import Blueprint, request, jsonify, g
import os
import io
import fitz  # PyMuPDF
import qrcode
import requests
from dateutil import tz
from app.db import get_conn_ctx
from datetime import datetime, timedelta
import pytz
from supabase import create_client, Client

certificates_bp = Blueprint("certificates", __name__)

# ---------- Supabase helpers ----------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_CERTS = os.getenv("SUPABASE_BUCKET_CERTS", "certificados")

def _get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _upload_pdf_and_get_public_url(data: bytes, path: str) -> str:
    """
    Sube el PDF a Supabase Storage y devuelve la URL pública.
    path ejemplo: 'certificados/123/certificado.pdf'
    """
    sb = _get_supabase_client()
    # sube con upsert para sobrescribir si ya existía
    sb.storage.from_(BUCKET_CERTS).upload(
        file=data,
        path=path,
        file_options={"content-type": "application/pdf", "x-upsert": "true"},
    )
    # obtener URL pública
    res = sb.storage.from_(BUCKET_CERTS).get_public_url(path)
    # SDK v2 devuelve dict o string según versión, normalizamos
    if isinstance(res, dict):
        return res.get("publicUrl") or res.get("public_url") or ""
    return str(res)

# ---------- utilidades comunes ----------

def _make_qr_bytes(text: str, box_size: int = 8, border: int = 1) -> bytes:
    qr = qrcode.QRCode(version=None, box_size=box_size, border=border)
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()

def _rect_almost_equal(r1: fitz.Rect, r2: fitz.Rect, tol: float = 0.1) -> bool:
    return (
        abs(r1.x0 - r2.x0) < tol and
        abs(r1.y0 - r2.y0) < tol and
        abs(r1.x1 - r2.x1) < tol and
        abs(r1.y1 - r2.y1) < tol
    )

def _collect_placeholder_matches_with_style(page: fitz.Page, placeholder: str):
    matches = []
    try:
        d = page.get_text("dict")
        for block in d.get("blocks", []):
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    if span.get("text") == placeholder:
                        x0, y0, x1, y1 = span["bbox"]
                        matches.append({
                            "rect": fitz.Rect(x0, y0, x1, y1),
                            "font": span.get("font") or "helv",
                            "size": float(span.get("size") or 11),
                        })
    except Exception:
        pass

    try:
        rects = page.search_for(placeholder)
        for r in rects:
            if any(_rect_almost_equal(r, m["rect"]) for m in matches):
                continue
            matches.append({"rect": r, "font": "helv", "size": 11.0})
    except Exception:
        pass

    dedup = []
    out = []
    for m in matches:
        if not any(_rect_almost_equal(m["rect"], d["rect"]) for d in dedup):
            dedup.append(m)
            out.append(m)
    return out

def _square_and_scale_rect(r: fitz.Rect, scale: float, page: fitz.Page) -> fitz.Rect:
    cx = (r.x0 + r.x1) / 2
    cy = (r.y0 + r.y1) / 2
    side = max(r.width, r.height) * scale
    half = side / 2
    sr = fitz.Rect(cx - half, cy - half, cx + half, cy + half)
    pg = page.rect
    sr.x0 = max(pg.x0, sr.x0)
    sr.y0 = max(pg.y0, sr.y0)
    sr.x1 = min(pg.x1, sr.x1)
    sr.y1 = min(pg.y1, sr.y1)
    return sr

def _to_upper(val) -> str:
    if val is None:
        return ""
    s = str(val)
    return s.upper()

def _replace_placeholders_transparente(doc: fitz.Document, mapping: dict[str, str], qr_png: bytes | None):
    total_counts = {k: 0 for k in mapping.keys()}

    SIZE_MULTIPLIER = {
        "${fecha_em}": 0.75,
        "${nombre_apellido2}": 0.50,
        "${documento2}": 0.50,
        "${localidad2}": 0.50,
        "${provincia2}": 0.50,
    }

    MIN_SIZE = 6.0
    MAX_SIZE = 28.0

    for page in doc:
        page_matches = {}
        for ph in list(mapping.keys()):
            ms = _collect_placeholder_matches_with_style(page, ph)
            if ms:
                page_matches[ph] = ms
                for m in ms:
                    page.add_redact_annot(m["rect"], text="", fill=None)

        qr_matches = []
        if qr_png is not None:
            qr_matches = _collect_placeholder_matches_with_style(page, "${qr}")
            for m in qr_matches:
                page.add_redact_annot(m["rect"], text="", fill=None)

        if page_matches or qr_matches:
            page.apply_redactions()

        for ph, ms in page_matches.items():
            raw_val = mapping.get(ph, "")
            val = _to_upper(raw_val)

            for m in ms:
                fontname = m["font"] if m["font"] in (
                    "helv", "Helvetica", "Times-Roman", "Times", "Courier", "Symbol", "ZapfDingbats"
                ) else "helv"

                base_size = float(m["size"])
                mult = SIZE_MULTIPLIER.get(ph, 1.0)
                size = max(MIN_SIZE, min(MAX_SIZE, base_size * mult))

                r = m["rect"]
                padded = fitz.Rect(r.x0 + 1, r.y0 + 1, r.x1 - 1, r.y1 - 1)

                is_crt = ph in ("${crt_numero}",)
                is_vertical_slot = r.height > r.width * 2

                placed = 0
                try:
                    if is_crt and is_vertical_slot:
                        placed = page.insert_textbox(
                            padded, val or "",
                            fontname=fontname,
                            fontsize=size,
                            align=1,
                            rotate=90,
                            opacity=1.0
                        )
                    else:
                        placed = page.insert_textbox(
                            padded, val or "",
                            fontname=fontname,
                            fontsize=size,
                            align=0,
                            opacity=1.0
                        )
                except Exception:
                    placed = 0

                if not placed:
                    page.insert_text(
                        (r.x0 + 1, r.y1 - 2),
                        val or "",
                        fontname=fontname,
                        fontsize=size,
                        rotate=90 if (is_crt and is_vertical_slot) else 0
                    )
                total_counts[ph] += 1

        if qr_png is not None and qr_matches:
            for m in qr_matches:
                sq = _square_and_scale_rect(m["rect"], scale=1.5, page=page)
                page.insert_image(sq, stream=qr_png, keep_proportion=True)

    return total_counts

def _fmt_date(dt) -> str | None:
    if not dt:
        return None
    try:
        z = tz.gettz("America/Argentina/Cordoba")
        d = dt.astimezone(z) if hasattr(dt, "astimezone") else dt
    except Exception:
        d = dt
    return f"{d:%d-%m-%Y}"

# ---------- endpoint ----------

@certificates_bp.route("/certificates/application/<int:app_id>/generate", methods=["POST"])
async def certificates_generate_by_application(app_id: int):
    """
    Body JSON esperado:
    {
      "condicion": "Apto" | "Condicional" | "Rechazado",
      "output_name": "opcional, nombre del archivo"
    }
    """
    payload = await request.get_json() or {}
    condicion_raw = (payload.get("condicion") or "Apto").strip().lower()

    cond_map = {"apto": "Apto", "condicional": "Condicional", "rechazado": "Rechazado"}
    condicion = cond_map.get(condicion_raw, "Apto")

    templates_por_cond = {
        "apto": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_apto.pdf",
        "condicional": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_apto.pdf",
        "rechazado": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_apto.pdf",
    }
    template_url = templates_por_cond.get(condicion_raw, templates_por_cond["apto"])

    # bajar template como bytes
    try:
        t_resp = requests.get(template_url, timeout=20)
        t_resp.raise_for_status()
        template_bytes = t_resp.content
    except Exception as e:
        return jsonify({"error": f"No se pudo descargar el template, {e}"}), 502

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
            a.id                       AS application_id,
            a.date                     AS app_date,
            a.status                   AS app_status,
            a.result                   AS app_result,
            a.workshop_id              AS workshop_id,

            o.first_name               AS owner_first_name,
            o.last_name                AS owner_last_name,
            o.dni                      AS owner_dni,
            o.street                   AS owner_street,
            o.city                     AS owner_city,
            o.province                 AS owner_province,

            d.first_name               AS driver_first_name,
            d.last_name                AS driver_last_name,
            d.dni                      AS driver_dni,

            c.license_plate            AS car_plate,
            c.brand                    AS car_brand,
            c.model                    AS car_model,
            c.manufacture_year         AS car_year,
            c.engine_brand             AS engine_brand,
            c.engine_number            AS engine_number,
            c.chassis_brand            AS chassis_brand,
            c.chassis_number           AS chassis_number,
            c.fuel_type                AS fuel_type,
            c.insurance                AS insurance,
            c.vehicle_type             AS vehicle_type,
            c.usage_type               AS usage_type,

            ws.razon_social AS workshop_name,
            ws.plant_number            AS workshop_plant_number,

            s.sticker_number           AS sticker_number
            FROM applications a
            LEFT JOIN persons   o  ON o.id  = a.owner_id
            LEFT JOIN persons   d  ON d.id  = a.driver_id
            LEFT JOIN cars      c  ON c.id  = a.car_id
            LEFT JOIN workshop  ws ON ws.id = a.workshop_id
            LEFT JOIN stickers  s  ON s.id  = c.sticker_id
            WHERE a.id = $1
            """,
            app_id
        )
        if not row:
            return jsonify({"error": "Trámite no encontrado"}), 404

        insp = await conn.fetchrow(
            """
            SELECT i.id, i.global_observations
            FROM inspections i
            WHERE i.application_id = $1
            ORDER BY i.id DESC
            LIMIT 1
            """,
            app_id
        )
            
    owner_fullname = " ".join([x for x in [row["owner_first_name"], row["owner_last_name"]] if x])
    documento = row["owner_dni"] or row["driver_dni"]
    domicilio = row["owner_street"]
    localidad = row["owner_city"]
    provincia = row["owner_province"]

    # fecha de emisión, usa created_at de la inspección si existe
    fecha_emision_dt = insp["created_at"] if insp and "created_at" in insp else row["app_date"]
    fecha_emision = _fmt_date(fecha_emision_dt)
    # Usar fecha actual en Argentina para vencimiento
    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    now_argentina = datetime.datetime.now(argentina_tz)
    fecha_vencimiento = _fmt_date(now_argentina + timedelta(days=365)) if fecha_emision else None

    # condición final
    resultado = condicion or (row["app_result"] or row["app_status"] or "Apto")

    # clasificación en 2 líneas: tipo de vehículo y tipo de uso
    clasificacion = ""
    if row["vehicle_type"] or row["usage_type"]:
        vt = (row["vehicle_type"] or "").strip()
        ut = (row["usage_type"] or "").strip()
        clasificacion = f"{vt}\n{ut}".strip()

    # número CRT ahora es el número de oblea
    crt_numero = str(row["sticker_number"] or "")

    mapping = {
        "${fecha_emision}":         fecha_emision or "",
        "${fecha_vencimiento}":     fecha_vencimiento or "",

        "${fecha_em}":              fecha_emision or "",
        "${fecha_vto}":             fecha_vencimiento or "",
        "${taller}":                row["workshop_name"] or "",         # razón social o nombre
        "${num_reg}":               str(row["workshop_plant_number"] or ""),  # número de planta
        "${nombre_apellido}":       owner_fullname or "",
        "${nombre_apellido2}":      f"{owner_fullname} (D.N.I. {str(documento)}) - TITULAR" or "",
        "${documento}":             str(documento or ""),
        "${documento2}":            str(documento or ""),
        "${domicilio}":             domicilio or "",
        "${localidad}":             localidad or "",
        "${localidad2}":            f"{localidad} ({provincia})" or "",
        "${provincia}":             provincia or "",
        "${provincia2}":            provincia or "",
        "${dominio}":               row["car_plate"] or "",
        "${anio}":                  str(row["car_year"] or ""),
        "${marca}":                 row["car_brand"] or "",
        "${modelo}":                row["car_model"] or "",
        "${marca_motor}":           row["engine_brand"] or "",
        "${numero_motor}":          row["engine_number"] or "",
        "${combustible}":           row["fuel_type"] or "",
        "${marca_chasis}":          row["chassis_brand"] or "",
        "${numero_chasis}":         row["chassis_number"] or "",
        "${tipo_vehiculo}":         row["vehicle_type"] or "",
        "${resultado_inspeccion}":  resultado,
        "${observaciones}":         (insp["global_observations"] if insp and insp["global_observations"] else ""),
        "${clasif}":                clasificacion,
        "${resultado2}":            "",
        "${crt_numero}":            crt_numero,  # número de oblea
    }

    # abrir template desde bytes
    try:
        doc = fitz.open(stream=template_bytes, filetype="pdf")
    except Exception as e:
        return jsonify({"error": f"No se pudo abrir el template PDF, {e}"}), 500

    qr_text = f"APP,{row['application_id']},DOM,{row['car_plate'] or ''},RES,{resultado}"
    qr_png = _make_qr_bytes(qr_text)

    counts = _replace_placeholders_transparente(doc, mapping, qr_png)

    # guardar a bytes
    out_buf = io.BytesIO()
    doc.save(out_buf, garbage=4, deflate=True)
    doc.close()
    pdf_bytes = out_buf.getvalue()

    output_name = payload.get("output_name") or "certificado.pdf"
    storage_path = f"certificados/{app_id}/certificado.pdf"

    try:
        public_url = _upload_pdf_and_get_public_url(pdf_bytes, storage_path)
    except Exception as e:
        return jsonify({"error": f"No se pudo subir a Supabase Storage, {e}"}), 502

    try:
        async with get_conn_ctx() as conn:
            await conn.execute(
                """
                UPDATE applications
                SET status = $1,
                    result = $2
                WHERE id = $3
                """,
                "Completado",
                resultado,
                app_id,
            )
    except Exception as e:
        return jsonify({
            "error": f"PDF generado, pero no se pudo actualizar el estado del trámite, {e}",
            "application_id": app_id,
            "template_url": template_url,
            "storage_bucket": BUCKET_CERTS,
            "storage_path": storage_path,
            "public_url": public_url,
            "replacements": counts
        }), 207

    return jsonify({
        "message": "PDF generado y trámite actualizado",
        "application_id": app_id,
        "new_status": "Completado",
        "new_result": resultado,
        "template_url": template_url,
        "storage_bucket": BUCKET_CERTS,
        "storage_path": storage_path,
        "public_url": public_url,
        "replacements": counts
    }), 200
