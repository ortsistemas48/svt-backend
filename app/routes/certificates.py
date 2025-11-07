# app/routes/certificates.py
from quart import Blueprint, request, jsonify
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
import textwrap
from fitz import PDF_REDACT_IMAGE_NONE, PDF_REDACT_LINE_ART_NONE, PDF_REDACT_TEXT_REMOVE
import asyncio

from app.jobs import new_job, get_job, run_job

certificates_bp = Blueprint("certificates", __name__)

# ---------- Supabase helpers ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_CERTS = os.getenv("SUPABASE_BUCKET_CERTS", "certificados")

def _adjust_font_size_by_length(ph: str, base_size: float, value: str) -> float:
    s = base_size
    n = len(value or "")
    if ph in ("${domicilio}", "${modelo}"):
        if n <= 20: factor = 1.00
        elif n <= 30: factor = 0.85
        elif n <= 40: factor = 0.75
        elif n <= 50: factor = 0.65
        else: factor = 0.55
        s *= factor
    if ph == "${numero_motor}":
        s *= 0.85
    if ph == "${numero_chasis}":
        s *= 0.85
    return s

def _add_transparent_redaction(page: fitz.Page, rect: fitz.Rect):
    page.add_redact_annot(rect, text=None, fill=False, cross_out=False)

def _get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _upload_pdf_and_get_public_url(data: bytes, path: str) -> str:
    sb = _get_supabase_client()
    sb.storage.from_(BUCKET_CERTS).upload(
        file=data,
        path=path,
        file_options={"content-type": "application/pdf", "x-upsert": "true"},
    )
    res = sb.storage.from_(BUCKET_CERTS).get_public_url(path)
    if isinstance(res, dict):
        return res.get("publicUrl") or res.get("public_url") or ""
    return str(res)

# ---------- utilidades comunes ----------
def _make_qr_bytes(text: str, box_size: int = 8, border: int = 1) -> bytes:
    qr = qrcode.QRCode(box_size=box_size, border=border)
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
    return str(val).upper()

def _replace_placeholders_transparente(doc: fitz.Document, mapping: dict[str, str], qr_png: bytes | None):
    total_counts = {k: 0 for k in mapping.keys()}
    SIZE_MULTIPLIER = {
        "${fecha_em}": 0.75,
        "${nombre_apellido2}": 0.50,
        "${documento2}": 0.50,
        "${localidad2}": 0.50,
        "${provincia2}": 0.50,
        "${observaciones}": 0.1,
    }
    MIN_SIZE = 4.0
    MAX_SIZE = 28.0

    for page in doc:
        page_matches = {}
        for ph in list(mapping.keys()):
            ms = _collect_placeholder_matches_with_style(page, ph)
            if ms:
                page_matches[ph] = ms
                for m in ms:
                    _add_transparent_redaction(page, m["rect"])

        qr_matches = []
        if qr_png is not None:
            qr_matches = _collect_placeholder_matches_with_style(page, "${qr}")
            for m in qr_matches:
                _add_transparent_redaction(page, m["rect"])

        if page_matches or qr_matches:
            page.apply_redactions(
                images=PDF_REDACT_IMAGE_NONE,
                graphics=PDF_REDACT_LINE_ART_NONE,
                text=PDF_REDACT_TEXT_REMOVE
            )

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
                size = _adjust_font_size_by_length(ph, size, val)
                size = max(MIN_SIZE, min(MAX_SIZE, size))

                r = m["rect"]
                padded = fitz.Rect(r.x0 + 1, r.y0 + 1, r.x1 - 1, r.y1 - 1)

                is_crt = ph in ("${crt_numero}",)
                is_vertical_slot = r.height > r.width * 2

                placed = 0
                try:
                    if is_crt and is_vertical_slot:
                        placed = page.insert_textbox(
                            padded, val or "", fontname=fontname, fontsize=size, align=1, rotate=90, opacity=1.0
                        )
                    else:
                        placed = page.insert_textbox(
                            padded, val or "", fontname=fontname, fontsize=size, align=0, opacity=1.0
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

def _years_delta(base_dt: datetime, years: int) -> datetime:
    try:
        return base_dt.replace(year=base_dt.year + years)
    except ValueError:
        return base_dt + timedelta(days=365 * years)

def _calc_vencimiento(fecha_emision_dt: datetime | None, car_year: int | None, now_tz: pytz.BaseTzInfo) -> str | None:
    if not fecha_emision_dt:
        return None
    base = fecha_emision_dt.astimezone(now_tz) if hasattr(fecha_emision_dt, "astimezone") else fecha_emision_dt
    hoy = datetime.now(now_tz)
    try:
        cy = hoy.year
        age = None if not car_year else max(0, cy - int(car_year))
    except Exception:
        age = None
    if age is None:
        delta_years = 1
    elif age == 0:
        delta_years = 3
    elif 3 <= age <= 7:
        delta_years = 2
    elif age > 7:
        delta_years = 1
    else:
        delta_years = 1
    vto_dt = _years_delta(base, delta_years)
    return _fmt_date(vto_dt)

# ---------- mapeos completos ----------
VEHICLE_TYPE_LABELS = {
    "L":  "Vehículo automotor con menos de CUATRO (4) ruedas",
    "L1": "2 Ruedas, Menos de 50 CM3, Menos de 40 KM/H",
    "L2": "3 Ruedas, Menos de 50 CM3, Menos de 40 KM/H",
    "L3": "2 Ruedas, Más de 50 CM3, Más de 40 KM/H",
    "L4": "3 Ruedas, Más de 50 CM3, Más de 40 KM/H",
    "L5": "3 Ruedas, Más de 50 CM3, Más de 40 KM/H",
    "M":  "Vehículo automotor con por lo menos 4 ruedas, o 3 de más de 1.000 KG",
    "M1": "Hasta 8 plazas más conductor y menos de 3.500 KG",
    "M2": "Más de 8 plazas excluido conductor y hasta 5.000 KG",
    "M3": "Más de 8 plazas excluido conductor y más de 5.000 KG",
    "N":  "Vehículo automotor con por lo menos 4 ruedas, o 3 de más de 1.000 KG",
    "N1": "Hasta 3.500 KG",
    "N2": "Desde 3.500 KG hasta 12.000 KG",
    "N3": "Más de 12.000 KG",
    "O":  "Acoplados y semirremolques",
    "O1": "Acoplados, semirremolques hasta 750 KG",
    "O2": "Acoplados, semirremolques desde 750 KG hasta 3.500 KG",
    "O3": "Acoplados, semirremolques de más de 3.500 KG y hasta 10.000 KG",
    "O4": "Acoplados, semirremolques de más de 10.000 KG",
}

USAGE_TYPE_LABELS = {
    "A":  "Oficial",
    "B":  "Diplomático, Consular u Org. Internacional",
    "C":  "Particular",
    "D":  "De alquiler, alquiler con chofer, Taxi, Remis",
    "E":  "Transporte público de pasajeros",
    "E1": "Servicio internacional, regular y turismo, larga distancia y urbanos cat. M1, M2, M3",
    "E2": "Interjurisdiccional y jurisdiccional, regulares, turismo cat. M1, M2, M3",
    "F":  "Transporte escolar",
    "G":  "Cargas, generales, peligrosas, recolección, carretones, servicios industriales y trabajos sobre la vía pública",
    "H":  "Emergencia, seguridad, fúnebres, remolque, maquinaria especial o agrícola y trabajos sobre la vía pública",
}

def _vehicle_type_display(code: str | None) -> str:
    c = (code or "").strip().upper()
    if not c:
        return ""
    label = VEHICLE_TYPE_LABELS.get(c, "")
    return f"{c} - {label}" if label else c

def _usage_type_display(code: str | None) -> str:
    c = (code or "").strip().upper()
    if not c:
        return ""
    label = USAGE_TYPE_LABELS.get(c, "")
    return f"{c} - {label}" if label else c

def _wrap_to_width(text: str, width: int = 35) -> str:
    lines = []
    for part in (text or "").splitlines():
        if not part:
            lines.append("")
            continue
        wrapped = textwrap.wrap(part, width=width, break_long_words=False, break_on_hyphens=False)
        lines.extend(wrapped if wrapped else [""])
    return "\n".join(lines)

# ---------- NUEVO: endpoint async que dispara job y responde 202 ----------
@certificates_bp.route("/certificates/application/<int:app_id>/generate", methods=["POST"])
async def certificates_generate_by_application(app_id: int):
    payload = await request.get_json() or {}
    job_id = new_job()

    async def work():
        return await _do_generate_certificate(app_id, payload)

    asyncio.create_task(run_job(work(), job_id))
    return jsonify({"message": "En proceso", "job_id": job_id}), 202

@certificates_bp.route("/certificates/job/<job_id>", methods=["GET"])
async def certificates_job_status(job_id: str):
    j = get_job(job_id)
    if not j:
        return jsonify({"error": "job_id no encontrado"}), 404
    return jsonify(j), 200

# ---------- LÓGICA DE GENERACIÓN MOVIDA A FUNCIÓN REUTILIZABLE ----------
async def _do_generate_certificate(app_id: int, payload: dict):
    condicion_raw = (payload.get("condicion") or "Apto").strip().lower()
    cond_map = {"apto": "Apto", "condicional": "Condicional", "rechazado": "Rechazado"}
    condicion = cond_map.get(condicion_raw, "Apto")

    templates_por_cond = {
        "apto": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_apto.pdf",
        "condicional": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_condicional.pdf",
        "rechazado": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_rechazado.pdf",
    }
    template_url = templates_por_cond.get(condicion_raw, templates_por_cond["apto"])

    # descarga del template, bloqueante pero dentro del job, si querés pasar a httpx async mejor
    try:
        t_resp = requests.get(template_url, timeout=20)
        t_resp.raise_for_status()
        template_bytes = t_resp.content
    except Exception as e:
        raise RuntimeError(f"No se pudo descargar el template, {e}")

    async with get_conn_ctx() as conn:
        row = await conn.fetchrow(
            """
            SELECT
            a.id AS application_id,
            a.date AS app_date,
            a.status AS app_status,
            a.result AS app_result,
            a.workshop_id AS workshop_id,

            o.first_name AS owner_first_name,
            o.last_name  AS owner_last_name,
            o.dni        AS owner_dni,
            o.street     AS owner_street,
            o.city       AS owner_city,
            o.province   AS owner_province,

            d.first_name AS driver_first_name,
            d.last_name  AS driver_last_name,
            d.dni        AS driver_dni,

            c.license_plate    AS car_plate,
            c.brand            AS car_brand,
            c.model            AS car_model,
            c.manufacture_year AS car_year,
            c.engine_brand     AS engine_brand,
            c.engine_number    AS engine_number,
            c.chassis_brand    AS chassis_brand,
            c.chassis_number   AS chassis_number,
            c.fuel_type        AS fuel_type,
            c.insurance        AS insurance,
            c.vehicle_type     AS vehicle_type,
            c.usage_type       AS usage_type,

            ws.razon_social AS workshop_name,
            ws.plant_number AS workshop_plant_number,

            s.sticker_number AS sticker_number
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
            raise RuntimeError("Trámite no encontrado")

        insp = await conn.fetchrow(
            """
            SELECT
                i.id,
                i.global_observations,
                i.created_at,
                COALESCE(i.is_second, FALSE) AS is_second
            FROM inspections i
            WHERE i.application_id = $1
            ORDER BY i.id DESC
            LIMIT 1
            """,
            app_id
        )

        step_obs_rows = []
        if insp:
            step_obs_rows = await conn.fetch(
                """
                SELECT
                  COALESCE(st.name, '')    AS step_name,
                  o.description            AS obs_desc,
                  COALESCE(so.number,999)  AS step_order,
                  o.id                     AS obs_id
                FROM observation_details od
                JOIN inspection_details idet ON idet.id = od.inspection_detail_id
                JOIN observations o          ON o.id    = od.observation_id
                LEFT JOIN steps st           ON st.id   = o.step_id
                LEFT JOIN steps_order so     ON so.step_id = st.id
                                             AND so.workshop_id = $2
                WHERE idet.inspection_id = $1
                ORDER BY step_order, obs_id
                """,
                insp["id"],
                row["workshop_id"],
            )

    is_second_inspection = bool(insp and insp.get("is_second"))

    owner_fullname = " ".join([x for x in [row["owner_first_name"], row["owner_last_name"]] if x])
    documento = row["owner_dni"] or row["driver_dni"]
    domicilio = row["owner_street"]
    localidad = row["owner_city"]
    provincia = row["owner_province"]

    argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
    insp_created_at = insp.get("created_at") if insp else None
    if is_second_inspection and insp_created_at:
        fecha_emision_dt = insp_created_at
    else:
        fecha_emision_dt = insp_created_at or row["app_date"]
    print("fecha_emision_dt", fecha_emision_dt)
    fecha_emision = _fmt_date(fecha_emision_dt)
    fecha_vencimiento = _calc_vencimiento(fecha_emision_dt, row["car_year"], argentina_tz) if fecha_emision_dt else None

    resultado = condicion or (row["app_result"] or row["app_status"] or "Apto")
    resultado_primera_inspeccion = (row["app_result"] or row["app_status"] or "").strip()
    resultado_mapeo_principal = resultado if not is_second_inspection else (resultado_primera_inspeccion or resultado)
    resultado_segunda_inspeccion = resultado if is_second_inspection else ""
    tipo_puro = (row["vehicle_type"] or "").strip().upper()
    tipo_display = _vehicle_type_display((row["vehicle_type"] or "").strip())
    uso_display = _usage_type_display((row["usage_type"] or "").strip())
    clasificacion_base = "\n".join([t for t in [tipo_display, uso_display] if t])
    clasificacion = _wrap_to_width(clasificacion_base, width=40)

    resultado_final = resultado_mapeo_principal if not is_second_inspection else resultado_segunda_inspeccion
    oblea = str(row["sticker_number"] or "")
    current_year_ar = datetime.now(argentina_tz).year
    crt_numero = f"{row['application_id']}/{current_year_ar}"

    step_groups = {}
    for r in step_obs_rows or []:
        step_name = (r["step_name"] or "").strip()
        desc = (r["obs_desc"] or "").strip()
        if not step_name and not desc:
            continue
        step_groups.setdefault(step_name, []).append(desc) if desc else step_groups.setdefault(step_name, [])

    step_lines = []
    seen = set()
    for r in step_obs_rows or []:
        name = (r["step_name"] or "").strip()
        if name in seen:
            continue
        seen.add(name)
        descs = [d for d in step_groups.get(name, []) if d]
        if not name and not descs:
            continue
        if descs:
            step_lines.append(f"{name}: {', '.join(descs)}" if name else ", ".join(descs))
        else:
            if name:
                step_lines.append(f"{name}:")
    step_obs_text = "\n".join(step_lines).strip()

    global_obs_text = (insp["global_observations"] if insp and insp["global_observations"] else "").strip()
    global_obs_wrapped = textwrap.fill(global_obs_text, width=115, break_long_words=False, break_on_hyphens=False) if global_obs_text else ""
    global_obs_wrapped2 = textwrap.fill(global_obs_text, width=90, break_long_words=False, break_on_hyphens=False) if global_obs_text else ""
    observaciones_text = global_obs_wrapped
    observaciones_text2 = global_obs_wrapped2

    oblea = str(row["sticker_number"] or "").strip()
    qr_target = oblea
    qr_link = f"https://www.checkrto.com/qr/{qr_target}"
    oblea_text = oblea if oblea else "Sin Asignar"
    mapping = {
        "${fecha_emision}":         fecha_emision or "",
        "${fecha_vencimiento}":     fecha_vencimiento or "",
        "${fecha_em}":              fecha_emision or "",
        "${fecha_vto}":             fecha_vencimiento or "",
        "${taller}":                row["workshop_name"] or "",
        "${num_reg}":               str(row["workshop_plant_number"] or ""),
        "${nombre_apellido}":       owner_fullname or "",
        "${nombre_apellido2}":      f"{owner_fullname} (D.N.I. {str(documento)}) , TITULAR" or "",
        "${documento}":             str(documento or ""),
        "${documento2}":            str(documento or ""),
        "${domicilio}":             domicilio or "",
        "${localidad}":             localidad or "",
        "${localidad2}":            f"{localidad} ({provincia})" if localidad or provincia else "",
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
        "${tipo_vehiculo}":         tipo_puro,
        "${resultado_inspeccion}":  resultado_mapeo_principal,
        "${observaciones}":         observaciones_text,
        "${observaciones2}":        observaciones_text2,
        "${clasif}":                clasificacion,
        "${resultado2}":            resultado_segunda_inspeccion,
        "${crt_numero}":            crt_numero,
        "${oblea_numero}":          oblea_text,
        "${resultado_final}":       resultado_final,
    }

    # abrir, reemplazar, guardar
    try:
        doc = fitz.open(stream=template_bytes, filetype="pdf")
    except Exception as e:
        raise RuntimeError(f"No se pudo abrir el template PDF, {e}")

    qr_png = _make_qr_bytes(qr_link)
    counts = _replace_placeholders_transparente(doc, mapping, qr_png)
    out_buf = io.BytesIO()
    doc.save(out_buf, garbage=4, deflate=True)
    doc.close()
    pdf_bytes = out_buf.getvalue()

    file_name = "certificado_2.pdf" if is_second_inspection else "certificado.pdf"
    storage_path = f"certificados/{app_id}/{file_name}"
    try:
        public_url = _upload_pdf_and_get_public_url(pdf_bytes, storage_path)
    except Exception as e:
        # subido como error 207 en tu versión, acá dejamos el job en done con warning si preferís
        raise RuntimeError(f"PDF generado, no se pudo subir a Supabase Storage, {e}")

    try:
        async with get_conn_ctx() as conn:
            if is_second_inspection:
                await conn.execute(
                    """
                    UPDATE applications
                    SET status = $1,
                        result_2 = $2
                    WHERE id = $3
                    """,
                    "Completado",
                    resultado,
                    app_id,
                )
            else:
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
        # si querés tolerar esto como 207, podés no levantar excepción y devolver la data con un flag
        raise RuntimeError(f"PDF generado, no se pudo actualizar el estado del trámite, {e}")

    # devolver dict final para el job
    return {
        "message": "PDF generado y trámite actualizado",
        "application_id": app_id,
        "new_status": "Completado",
        "new_result": resultado,
        "is_second_inspection": is_second_inspection,
        "file_name": file_name,
        "template_url": template_url,
        "storage_bucket": BUCKET_CERTS,
        "storage_path": storage_path,
        "public_url": public_url,
        "replacements": counts
    }
