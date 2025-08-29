# app/routes/certificates.py
from quart import Blueprint, request, jsonify, g
import os
import io
import fitz  # PyMuPDF
import qrcode
from dateutil import tz
from app.db import get_conn_ctx
from datetime import datetime, timedelta

certificates_bp = Blueprint("certificates", __name__)

# ---------- utilidades comunes ----------

def _make_qr_bytes(text: str, box_size: int = 8, border: int = 1) -> bytes:
    """
    QR más grande, pixels por módulo subidos, borde fino para aprovechar área.
    """
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
    """
    Devuelve lista de dicts con rect, font, size.
    """
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
    """
    Hace el rectángulo cuadrado tomando el lado mayor y lo escala,
    centrado en el rect original, recortando a los límites de la página.
    """
    cx = (r.x0 + r.x1) / 2
    cy = (r.y0 + r.y1) / 2
    side = max(r.width, r.height) * scale
    half = side / 2
    sr = fitz.Rect(cx - half, cy - half, cx + half, cy + half)
    # Limitar a la página
    pg = page.rect
    sr.x0 = max(pg.x0, sr.x0)
    sr.y0 = max(pg.y0, sr.y0)
    sr.x1 = min(pg.x1, sr.x1)
    sr.y1 = min(pg.y1, sr.y1)
    return sr

def _to_upper(val) -> str:
    """
    Devuelve string en mayúscula, seguro para None y números.
    """
    if val is None:
        return ""
    s = str(val)
    return s.upper()

# 2) En _replace_placeholders_transparente, agregar un mapa de multiplicadores y aplicarlos

def _replace_placeholders_transparente(doc: fitz.Document, mapping: dict[str, str], qr_png: bytes | None):
    """
    Reemplaza placeholders por valores en MAYÚSCULA, borra el texto original,
    ajusta tamaño, maneja orientación vertical para CRT si el hueco es vertical,
    inserta QR agrandado y cuadrado.
    """
    total_counts = {k: 0 for k in mapping.keys()}

    # Tamaños relativos por placeholder
    SIZE_MULTIPLIER = {
        # Fechas originales, pedido de que la primera fecha_emision sea un poco más chica, igual que fecha_vencimiento
        "${fecha_em}": 0.75,

        # Campos que deben ser mucho más chicos
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

                # tamaño base detectado
                base_size = float(m["size"])
                # aplicar multiplicador si corresponde
                mult = SIZE_MULTIPLIER.get(ph, 1.0)
                size = max(MIN_SIZE, min(MAX_SIZE, base_size * mult))

                r = m["rect"]
                padded = fitz.Rect(r.x0 + 1, r.y0 + 1, r.x1 - 1, r.y1 - 1)

                # Detección de campo vertical para CRT
                is_crt = ph in ("${crt_numero}",)
                is_vertical_slot = r.height > r.width * 2

                placed = 0
                try:
                    if is_crt and is_vertical_slot:
                        placed = page.insert_textbox(
                            padded, val or "",
                            fontname=fontname,
                            fontsize=size,
                            color=(0, 0, 0),
                            align=1,
                            rotate=90,
                            opacity=1.0
                        )
                    else:
                        placed = page.insert_textbox(
                            padded, val or "",
                            fontname=fontname,
                            fontsize=size,
                            color=(0, 0, 0),
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
                        color=(0, 0, 0),
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
    payload = await request.get_json() or {}
    template_path = payload.get("template_path")
    if not template_path:
        return jsonify({"error": "Falta template_path"}), 400
    if not os.path.isfile(template_path):
        return jsonify({"error": f"No existe el archivo, {template_path}"}), 404

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

              ws.name                    AS workshop_name,
              ws.plant_number            AS workshop_plant_number   -- número de planta
            FROM applications a
            LEFT JOIN persons o ON o.id = a.owner_id
            LEFT JOIN persons d ON d.id = a.driver_id
            LEFT JOIN cars    c ON c.id = a.car_id
            LEFT JOIN workshop ws ON ws.id = a.workshop_id
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

    fecha_emision_dt = insp["created_at"] if insp and insp["created_at"] else row["app_date"]
    fecha_emision = _fmt_date(fecha_emision_dt)
    fecha_vencimiento = _fmt_date(datetime.utcnow() + timedelta(days=365)) if fecha_emision else None

    resultado = row["app_result"] or row["app_status"] or "Apto"

    # --- mapping actualizado ---
    mapping = {
        "${fecha_emision}":         fecha_emision or "",
        "${fecha_vencimiento}":     fecha_vencimiento or "",

        "${fecha_em}":              fecha_emision or "",
        "${fecha_vto}":             fecha_vencimiento or "",
        "${taller}":                row["workshop_name"] or "",
        "${num_reg}":               str(row["workshop_plant_number"] or ""),
        "${nombre_apellido}":       owner_fullname or "",
        "${nombre_apellido2}":      owner_fullname or "",
        "${documento}":             str(documento or ""),
        "${documento2}":            str(documento or ""),
        "${domicilio}":             domicilio or "",
        "${localidad}":             localidad or "",
        "${localidad2}":            localidad or "",
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
        "${tipo_vehiculo}":         "Automóvil" if row["car_brand"] or row["car_model"] else "",
        "${resultado_inspeccion}":  resultado,
        "${observaciones}":         (insp["global_observations"] if insp and insp["global_observations"] else ""),
        "${clasif}":                "",  # por ahora vacío
        "${resultado2}":                "",  # por ahora vacío
        "${crt_numero}":            f"CRT-{row['application_id']}",
        # ${qr} va como imagen
    }

    doc = fitz.open(template_path)

    qr_text = f"APP,{row['application_id']},DOM,{row['car_plate'] or ''},RES,{resultado}"
    qr_png = _make_qr_bytes(qr_text)

    counts = _replace_placeholders_transparente(doc, mapping, qr_png)

    base_dir = os.path.dirname(os.path.abspath(template_path))
    output_name = payload.get("output_name") or f"certificado_app_{app_id}.pdf"
    output_path = os.path.join(base_dir, output_name)

    doc.save(output_path, garbage=4, deflate=True)
    doc.close()

    return jsonify({
        "message": "PDF generado",
        "application_id": app_id,
        "template_path": template_path,
        "output_path": output_path,
        "replacements": counts
    }), 200
