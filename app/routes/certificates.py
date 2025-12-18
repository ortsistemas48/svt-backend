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
import json
import unicodedata
from calendar import monthrange
import time
from PIL import Image

try:
    fitz.TOOLS.mupdf_display_errors(False)
except Exception:
    pass

from app.jobs import new_job, get_job, run_job
from app.email import send_certificate_email
import logging

log = logging.getLogger(__name__)

certificates_bp = Blueprint("certificates", __name__)

# ---------- Supabase helpers ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_CERTS = os.getenv("SUPABASE_BUCKET_CERTS", "certificados")

def _adjust_font_size_by_length(ph: str, base_size: float, value: str) -> float:
    s = base_size
    n = len(value or "")
    if ph == "${domicilio}":
        # Para domicilio, hacer más agresivo cuando tiene más de 30 caracteres
        if n <= 20: factor = 1.00
        elif n <= 30: factor = 0.85
        elif n <= 40: factor = 0.60  # Más pequeño que antes (era 0.75)
        elif n <= 50: factor = 0.45  # Mucho más pequeño (era 0.65)
        elif n <= 60: factor = 0.35  # Muy pequeño
        else: factor = 0.28  # Extremadamente pequeño (era 0.55)
        s *= factor
    elif ph == "${modelo}":
        # Para modelo, mantener la lógica original
        if n <= 20: factor = 1.00
        elif n <= 30: factor = 0.85
        elif n <= 40: factor = 0.75
        elif n <= 50: factor = 0.65
        else: factor = 0.55
        s *= factor
    elif ph == "${nombre_apellido2}":
        # Para nombre_apellido2, reducir tamaño si supera 40 caracteres
        if n > 40:
            if n <= 50: factor = 0.85
            elif n <= 60: factor = 0.70
            elif n <= 70: factor = 0.60
            else: factor = 0.50
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

def _upload_pdf_and_get_public_url(data: bytes, path: str, max_retries: int = 3) -> str:
    """
    Sube un PDF a Supabase Storage con reintentos en caso de errores SSL/conexión.
    
    Args:
        data: Bytes del PDF a subir
        path: Ruta donde guardar el archivo
        max_retries: Número máximo de reintentos (default: 3)
    
    Returns:
        URL pública del archivo subido
    
    Raises:
        RuntimeError: Si no se pudo subir después de todos los reintentos
    """
    file_size_mb = len(data) / (1024 * 1024)
    
    last_error = None
    # Optimización: reutilizar cliente Supabase
    sb = None
    
    for attempt in range(max_retries):
        try:
            if sb is None:
                sb = _get_supabase_client()
            
            upload_result = sb.storage.from_(BUCKET_CERTS).upload(
                file=data,
                path=path,
                file_options={"content-type": "application/pdf", "x-upsert": "true"},
            )
            
            res = sb.storage.from_(BUCKET_CERTS).get_public_url(path)
            if isinstance(res, dict):
                url = res.get("publicUrl") or res.get("public_url") or ""
            else:
                url = str(res)
            
            if url:
                return url
            else:
                raise RuntimeError("No se pudo obtener la URL pública del archivo subido")
            
        except Exception as e:
            last_error = e
            error_msg = str(e).lower()
            error_type = type(e).__name__
            
            is_ssl_error = any(keyword in error_msg for keyword in [
                "ssl", "eof", "protocol", "connection", "timeout", "broken pipe",
                "_ssl.c", "ssl3", "tls", "socket", "errno", "closed", "reset"
            ]) or "SSLError" in error_type or "ConnectionError" in error_type
            
            if is_ssl_error and attempt < max_retries - 1:
                wait_time = (2 ** attempt) + 0.3  # Optimización: reducir tiempos de espera (1.3s, 2.3s, 4.3s)
                time.sleep(wait_time)
                # Optimización: recrear cliente en caso de error de conexión
                sb = None
                continue
            else:
                raise
    
    raise RuntimeError(f"No se pudo subir el PDF después de {max_retries} intentos. Último error: {last_error}")

async def _upload_pdf_and_get_public_url_async(data: bytes, path: str) -> str:
    try:
        return await asyncio.to_thread(_upload_pdf_and_get_public_url, data, path)
    except Exception as e:
        error_msg = str(e)
        raise RuntimeError(f"Error al subir PDF a Supabase Storage: {error_msg}")

# ---------- utilidades comunes ----------
def _make_qr_bytes(text: str, box_size: int = 8, border: int = 1) -> bytes:
    # Optimización: usar versión más rápida sin fit para mejor rendimiento
    qr = qrcode.QRCode(box_size=box_size, border=border, error_correction=qrcode.constants.ERROR_CORRECT_L)
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    # Optimización: usar optimize=False para guardar más rápido
    img.save(buf, format="PNG", optimize=False)
    buf.seek(0)
    return buf.read()

async def _download_and_resize_image_async(image_url: str, target_width: int, target_height: int) -> bytes | None:
    """Descarga una imagen desde una URL y la redimensiona al tamaño especificado"""
    def _download_and_resize() -> bytes | None:
        try:
            # Optimización: usar stream=True y timeout más corto
            resp = requests.get(image_url, timeout=15, stream=True)
            resp.raise_for_status()
            
            img = Image.open(io.BytesIO(resp.content))
            
            # Optimización: usar Resampling.NEAREST para imágenes pequeñas (más rápido)
            # o mantener LANCZOS si se necesita mejor calidad
            img_resized = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
            
            if img_resized.mode in ('RGBA', 'LA', 'P'):
                rgb_img = Image.new('RGB', img_resized.size, (255, 255, 255))
                if img_resized.mode == 'P':
                    img_resized = img_resized.convert('RGBA')
                rgb_img.paste(img_resized, mask=img_resized.split()[-1] if img_resized.mode == 'RGBA' else None)
                img_resized = rgb_img
            
            buf = io.BytesIO()
            # Optimización: usar optimize=False para guardar más rápido
            img_resized.save(buf, format="PNG", optimize=False)
            buf.seek(0)
            return buf.read()
        except Exception as e:
            return None
    
    return await asyncio.to_thread(_download_and_resize)

def _rect_almost_equal(r1: fitz.Rect, r2: fitz.Rect, tol: float = 0.1) -> bool:
    return (
        abs(r1.x0 - r2.x0) < tol and
        abs(r1.y0 - r2.y0) < tol and
        abs(r1.x1 - r2.x1) < tol and
        abs(r1.y1 - r2.y1) < tol
    )

def _rects_overlap_significantly(r1: fitz.Rect, r2: fitz.Rect, overlap_threshold: float = 0.8) -> bool:
    """Verifica si dos rectángulos se superponen significativamente (más del 80% del área)"""
    # Calcular intersección
    x0 = max(r1.x0, r2.x0)
    y0 = max(r1.y0, r2.y0)
    x1 = min(r1.x1, r2.x1)
    y1 = min(r1.y1, r2.y1)
    
    if x0 >= x1 or y0 >= y1:
        return False  # No hay intersección
    
    # Área de intersección
    intersection_area = (x1 - x0) * (y1 - y0)
    
    # Área del rectángulo más pequeño
    area1 = (r1.x1 - r1.x0) * (r1.y1 - r1.y0)
    area2 = (r2.x1 - r2.x0) * (r2.y1 - r2.y0)
    min_area = min(area1, area2)
    
    if min_area == 0:
        return False
    
    # Si la intersección es más del 80% del área más pequeña, son el mismo placeholder
    return (intersection_area / min_area) >= overlap_threshold

def _collect_all_placeholder_matches_with_style(page: fitz.Page, placeholders: set[str]):
    result = {ph: [] for ph in placeholders}
    # Optimización: usar un set para búsqueda rápida
    ph_set = set(placeholders)
    # Pre-calcular set de placeholders múltiples para búsqueda rápida
    multi_occurrence_ph_set = {"${numero_motor}", "${numero_chasis}", "${fecha_emision}", 
                               "${fecha_em2}", "${fecha_vto}", "${fecha_vencimiento}"}
    # Para placeholders múltiples, usar un dict separado con mayor precisión
    seen_rects_multi = {}  # Para placeholders que aparecen múltiples veces (4 decimales)
    seen_rects_normal = {}  # Para otros placeholders (1 decimal)
    
    try:
        d = page.get_text("dict")
        for block in d.get("blocks", []):
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    txt = span.get("text")
                    if txt in ph_set:
                        # IMPORTANTE: Verificar que el texto es exactamente el placeholder, no una subcadena
                        # Por ejemplo, si encontramos "${fecha_em2}" pero estamos buscando "${fecha_emision}",
                        # no deberíamos agregarlo a ${fecha_emision}
                        # Solo agregar si el texto coincide exactamente con algún placeholder en el set
                        if txt in placeholders:  # Verificar que es exactamente uno de los placeholders buscados
                            x0, y0, x1, y1 = span["bbox"]
                            # Para placeholders que aparecen múltiples veces, usar más precisión (4 decimales)
                            is_multi = txt in multi_occurrence_ph_set
                            if is_multi:
                                # Usar 4 decimales para evitar eliminar instancias legítimas
                                rect_key = (round(x0, 4), round(y0, 4), round(x1, 4), round(y1, 4))
                                if rect_key not in seen_rects_multi:
                                    seen_rects_multi[rect_key] = True
                                    result[txt].append({
                                        "rect": fitz.Rect(x0, y0, x1, y1),
                                        "font": span.get("font") or "helv",
                                        "size": float(span.get("size") or 11),
                                    })
                            else:
                                rect_key = (round(x0, 1), round(y0, 1), round(x1, 1), round(y1, 1))
                                if rect_key not in seen_rects_normal:
                                    seen_rects_normal[rect_key] = True
                                    result[txt].append({
                                        "rect": fitz.Rect(x0, y0, x1, y1),
                                        "font": span.get("font") or "helv",
                                        "size": float(span.get("size") or 11),
                                    })
    except Exception:
        pass

    # Optimización: para placeholders múltiples, SIEMPRE usar search_for() para encontrar todas las instancias
    # incluso si get_text() ya encontró algunas
    multi_occurrence_ph_set = {"${numero_motor}", "${numero_chasis}", "${fecha_emision}", 
                               "${fecha_em2}", "${fecha_vto}", "${fecha_vencimiento}"}
    missing_ph = [ph for ph in placeholders if not result[ph]]
    # Agregar placeholders múltiples que ya se encontraron para buscar más instancias con search_for
    ph_to_search = set(missing_ph) | {ph for ph in multi_occurrence_ph_set if ph in placeholders and result[ph]}
    
    # Ordenar placeholders por longitud (más largos primero) para evitar que placeholders cortos
    # sean encontrados cuando se busca uno más largo (ej: ${fecha_em2} dentro de ${fecha_emision})
    ph_to_search_sorted = sorted(ph_to_search, key=len, reverse=True)
    
    if ph_to_search_sorted:
        for ph in ph_to_search_sorted:
            try:
                # Determinar si es un placeholder que aparece múltiples veces (usar set para búsqueda rápida)
                is_multi = ph in multi_occurrence_ph_set
                
                # Buscar el placeholder exacto con search_for (siempre para placeholders múltiples)
                # IMPORTANTE: Filtrar resultados para que coincidan exactamente, no subcadenas
                # Esto evita que ${fecha_em2} sea encontrado cuando se busca ${fecha_emision}
                rects = page.search_for(ph)
                if rects:
                    for r in rects:
                        # Verificar que el texto en este rectángulo coincide EXACTAMENTE con el placeholder buscado
                        # Esto es crítico para evitar que placeholders más cortos sean encontrados cuando se busca uno más largo
                        # Por ejemplo, evitar que ${fecha_em2} sea encontrado cuando se busca ${fecha_emision}
                        matches_exactly = False
                        try:
                            # Obtener el texto en este rectángulo específico usando get_text con clip
                            text_in_rect = page.get_text("text", clip=r).strip()
                            text_clean = text_in_rect.strip()
                            
                            # Verificar coincidencia EXACTA (sin espacios extra, sin caracteres adicionales)
                            if text_clean == ph:
                                matches_exactly = True
                            else:
                                # Si no coincide exactamente, verificar que no sea un placeholder diferente
                                # que es subcadena o supercadena del buscado
                                # Caso 1: Si el texto encontrado es un placeholder más corto que es subcadena del buscado
                                # (ej: buscamos "${fecha_emision}" pero encontramos "${fecha_em2}")
                                if text_clean in placeholders and text_clean != ph:
                                    # Es otro placeholder, verificar si es subcadena o supercadena
                                    if text_clean in ph:
                                        # El texto encontrado es más corto y subcadena del buscado, NO agregar
                                        matches_exactly = False
                                    elif ph in text_clean:
                                        # El texto encontrado es más largo y contiene el buscado, podría ser válido
                                        # pero solo si el texto es exactamente el placeholder más largo
                                        matches_exactly = (text_clean == ph)
                                    else:
                                        # Son placeholders diferentes sin relación de subcadena, NO agregar
                                        matches_exactly = False
                                else:
                                    # El texto no es un placeholder conocido, podría ser válido si contiene el placeholder
                                    # Pero para ser seguro, solo aceptar si coincide exactamente
                                    matches_exactly = False
                        except Exception:
                            # Si no se puede obtener el texto, ser conservador y NO agregar
                            # Esto evita agregar resultados incorrectos
                            matches_exactly = False
                        
                        # Solo agregar si coincide exactamente
                        if not matches_exactly:
                            continue
                        
                        # Usar 4 decimales para placeholders múltiples, 1 decimal para otros
                        if is_multi:
                            rect_key = (round(r.x0, 4), round(r.y0, 4), round(r.x1, 4), round(r.y1, 4))
                            if rect_key not in seen_rects_multi:
                                seen_rects_multi[rect_key] = True
                                result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                        else:
                            rect_key = (round(r.x0, 1), round(r.y0, 1), round(r.x1, 1), round(r.y1, 1))
                            if rect_key not in seen_rects_normal:
                                seen_rects_normal[rect_key] = True
                                result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                
                # Para placeholders múltiples que ya se encontraron con get_text, 
                # search_for puede encontrar instancias adicionales, así que no saltamos variaciones
                # Si no se encontró con search_for, intentar variaciones
                if (not rects or is_multi) and ph.startswith("${") and ph.endswith("}"):
                    # Variación 1: sin ${}
                    ph_clean = ph[2:-1]  # Remover ${ y }
                    rects_clean = page.search_for(ph_clean)
                    if rects_clean:
                        for r in rects_clean:
                            # Usar 4 decimales para placeholders múltiples, 1 decimal para otros
                            if is_multi:
                                rect_key = (round(r.x0, 4), round(r.y0, 4), round(r.x1, 4), round(r.y1, 4))
                                if rect_key not in seen_rects_multi:
                                    seen_rects_multi[rect_key] = True
                                    result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                            else:
                                rect_key = (round(r.x0, 1), round(r.y0, 1), round(r.x1, 1), round(r.y1, 1))
                                if rect_key not in seen_rects_normal:
                                    seen_rects_normal[rect_key] = True
                                    result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                    
                    # Variación 2: con espacios alrededor
                    if not rects_clean:
                        ph_with_spaces = f" {ph_clean} "
                        rects_spaces = page.search_for(ph_with_spaces)
                        if rects_spaces:
                            for r in rects_spaces:
                                # Usar 4 decimales para placeholders múltiples, 1 decimal para otros
                                if is_multi:
                                    rect_key = (round(r.x0, 4), round(r.y0, 4), round(r.x1, 4), round(r.y1, 4))
                                    if rect_key not in seen_rects_multi:
                                        seen_rects_multi[rect_key] = True
                                        result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                                else:
                                    rect_key = (round(r.x0, 1), round(r.y0, 1), round(r.x1, 1), round(r.y1, 1))
                                    if rect_key not in seen_rects_normal:
                                        seen_rects_normal[rect_key] = True
                                        result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                    
                    # Variación 3: buscar palabra por palabra para placeholders compuestos
                    if not rects_clean and not rects_spaces and "_" in ph_clean:
                        # Para fecha_emision, numero_motor, numero_chasis, etc.
                        words = ph_clean.split("_")
                        if len(words) >= 2:
                            # Buscar la primera palabra seguida de la segunda
                            search_term = f"{words[0]} {words[1]}"
                            rects_words = page.search_for(search_term)
                            if rects_words:
                                for r in rects_words:
                                    # Usar 4 decimales para placeholders múltiples, 1 decimal para otros
                                    if is_multi:
                                        rect_key = (round(r.x0, 4), round(r.y0, 4), round(r.x1, 4), round(r.y1, 4))
                                        if rect_key not in seen_rects_multi:
                                            seen_rects_multi[rect_key] = True
                                            result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                                    else:
                                        rect_key = (round(r.x0, 1), round(r.y0, 1), round(r.x1, 1), round(r.y1, 1))
                                        if rect_key not in seen_rects_normal:
                                            seen_rects_normal[rect_key] = True
                                            result[ph].append({"rect": r, "font": "helv", "size": 11.0})
                
                # Debug: si aún no se encontró, reportar
                if not result[ph]:
                    pass
            except Exception as e:
                pass

    # Optimización: deduplicación más eficiente usando dict con verificación de superposición
    # Para placeholders que aparecen múltiples veces, eliminar duplicados que se superponen significativamente
    for ph in placeholders:
        if result[ph]:
            # Para placeholders que sabemos que aparecen múltiples veces
            is_multi_occurrence = ph in multi_occurrence_ph_set
            
            if is_multi_occurrence:
                # Para placeholders múltiples: eliminar duplicados que se superponen significativamente
                # Esto maneja el caso donde get_text() y search_for() encuentran la misma instancia
                dedup_list = []
                for m in result[ph]:
                    r = m["rect"]
                    is_duplicate = False
                    # Verificar si este rectángulo se superpone significativamente con alguno ya agregado
                    for existing in dedup_list:
                        if _rects_overlap_significantly(r, existing["rect"], overlap_threshold=0.8):
                            is_duplicate = True
                            break
                    if not is_duplicate:
                        dedup_list.append(m)
                result[ph] = dedup_list
            else:
                # Para otros placeholders: deduplicación normal (1 decimal)
                dedup_dict = {}
                for m in result[ph]:
                    r = m["rect"]
                    key = (round(r.x0, 1), round(r.y0, 1), round(r.x1, 1), round(r.y1, 1))
                    if key not in dedup_dict:
                        dedup_dict[key] = m
                result[ph] = list(dedup_dict.values())
    return result

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

def _replace_placeholders_transparente(doc: fitz.Document, mapping: dict[str, str], qr_png: bytes | None, photo_png: bytes | None = None, usage_type: str | None = None):
    total_counts = {k: 0 for k in mapping.keys()}
    SIZE_MULTIPLIER = {
        "${fecha_em2}": 0.75,
        # "${nombre_apellido2}": 0.50,
        "${documento2}": 0.50,
        # "${localidad2}": 0.50,
        "${provincia2}": 0.50,
        "${ced_tipo2}": 0.50,
        "${patente2}": 0.50,
        "${observaciones}": 0.1,
    }

    if usage_type and (usage_type.strip().upper() == "D"):
        SIZE_MULTIPLIER["${observaciones2}"] = 0.7
    MIN_SIZE = 4.0
    MAX_SIZE = 28.0

    # Optimización: pre-calcular ph_set una sola vez
    ph_set = set(list(mapping.keys()) + ["${qr}", "${photo}"])
    has_photo_placeholder = "${photo}" in mapping

    for page_num, page in enumerate(doc):
        matches_map = _collect_all_placeholder_matches_with_style(page, ph_set)
        page_matches = {ph: matches_map.get(ph, []) for ph in mapping.keys() if matches_map.get(ph)}
        qr_matches = matches_map.get("${qr}", []) if qr_png is not None else []
        photo_matches = matches_map.get("${photo}", []) if has_photo_placeholder else []
        
        # Debug: verificar si placeholders problemáticos están en el mapping pero no se encontraron
        for ph_debug in ("${numero_motor}", "${numero_chasis}", "${fecha_emision}"):
            if ph_debug in mapping and ph_debug not in page_matches:
                # Intentar búsqueda manual para debug
                try:
                    debug_rects = page.search_for(ph_debug)
                    if not debug_rects and ph_debug.startswith("${") and ph_debug.endswith("}"):
                        ph_clean_debug = ph_debug[2:-1]
                        debug_rects_clean = page.search_for(ph_clean_debug)
                except Exception as e:
                    pass

        # Optimización: acumular todas las redacciones antes de aplicar
        has_redactions = False
        for ms in page_matches.values():
            for m in ms:
                _add_transparent_redaction(page, m["rect"])
                has_redactions = True
        if qr_png is not None and qr_matches:
            for m in qr_matches:
                _add_transparent_redaction(page, m["rect"])
                has_redactions = True
        if photo_png is not None and photo_matches:
            for m in photo_matches:
                _add_transparent_redaction(page, m["rect"])
                has_redactions = True

        # Optimización: aplicar redacciones solo una vez por página
        if has_redactions:
            page.apply_redactions(
                images=PDF_REDACT_IMAGE_NONE,
                graphics=PDF_REDACT_LINE_ART_NONE,
                text=PDF_REDACT_TEXT_REMOVE
            )

        for ph, ms in page_matches.items():
            raw_val = mapping.get(ph, "")
            val = _to_upper(raw_val)
            
            # Debug para placeholders problemáticos
            if ph in ("${numero_motor}", "${numero_chasis}", "${fecha_emision}"):
                pass

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
                text_inserted = False
                try:
                    if is_crt and is_vertical_slot:
                        placed = page.insert_textbox(
                            padded, val or "", fontname=fontname, fontsize=size, align=1, rotate=90
                        )
                    else:
                        placed = page.insert_textbox(
                            padded, val or "", fontname=fontname, fontsize=size, align=0
                        )
                    # insert_textbox retorna el número de caracteres insertados
                    # Si retorna >= longitud del texto, se insertó completamente
                    # Si retorna > 0 pero < longitud, se insertó parcialmente
                    # Si retorna 0, no se insertó nada
                    val_len = len(val) if val else 0
                    if placed >= val_len:
                        text_inserted = True  # Se insertó completamente
                    elif placed > 0:
                        text_inserted = True  # Se insertó parcialmente, no usar fallback para evitar duplicados
                    else:
                        text_inserted = False  # No se insertó nada
                except Exception as e:
                    placed = 0
                    text_inserted = False

                # Solo usar insert_text como fallback si NO se insertó NADA con insert_textbox
                # Esto evita duplicar el texto cuando insert_textbox inserta (aunque sea parcialmente)
                if not text_inserted and val:
                    try:
                        page.insert_text(
                            (r.x0 + 1, r.y1 - 2),
                            val or "",
                            fontname=fontname,
                            fontsize=size,
                            rotate=90 if (is_crt and is_vertical_slot) else 0
                        )
                    except Exception as e:
                        pass
                total_counts[ph] += 1

        if qr_png is not None and qr_matches:
            for m in qr_matches:
                sq = _square_and_scale_rect(m["rect"], scale=1.5, page=page)
                page.insert_image(sq, stream=qr_png, keep_proportion=True)

        if photo_matches:
            if photo_png is not None:
                for m in photo_matches:
                    r_placeholder = m["rect"]
                    
                    photo_width_pts = 246.0  
                    photo_height_pts = 170.0  
                    
                    center_x = (r_placeholder.x0 + r_placeholder.x1) / 2
                    center_y = (r_placeholder.y0 + r_placeholder.y1) / 2
                    
                    r_exact = fitz.Rect(
                        center_x - photo_width_pts / 2,
                        center_y - photo_height_pts / 2,
                        center_x + photo_width_pts / 2,
                        center_y + photo_height_pts / 2
                    )
                    
                    try:
                        page.insert_image(r_exact, stream=photo_png, keep_proportion=False)
                    except Exception as e:
                        pass

    return total_counts

_TEMPLATE_CACHE: dict[str, tuple[float, bytes]] = {}
_TEMPLATE_TTL_SECONDS = 3600  # Optimización: aumentar TTL a 1 hora (era 10 minutos)

async def _get_template_bytes_async(template_url: str) -> bytes:
    now = time.time()
    cached = _TEMPLATE_CACHE.get(template_url)
    if cached and (now - cached[0] < _TEMPLATE_TTL_SECONDS):
        return cached[1]
    def _download() -> bytes:
        # Optimización: usar stream=True y timeout más corto para descargas más rápidas
        resp = requests.get(template_url, timeout=15, stream=True)
        resp.raise_for_status()
        return resp.content
    data = await asyncio.to_thread(_download)
    _TEMPLATE_CACHE[template_url] = (now, data)
    return data

def _render_certificate_pdf_sync(template_bytes: bytes, mapping: dict[str, str], qr_link: str, photo_png: bytes | None = None, usage_type: str | None = None) -> tuple[bytes, dict]:
    doc = fitz.open(stream=template_bytes, filetype="pdf")
    try:
        # Optimización: generar QR antes de abrir el documento para paralelizar mejor
        qr_png = _make_qr_bytes(qr_link)
        counts = _replace_placeholders_transparente(doc, mapping, qr_png, photo_png, usage_type)
        # Optimización: usar garbage=2 en lugar de 4 para mejor rendimiento (4 es muy agresivo)
        out_buf = io.BytesIO()
        doc.save(out_buf, garbage=2, deflate=True)
        pdf_bytes = out_buf.getvalue()
        return pdf_bytes, counts
    finally:
        try:
            doc.close()
        except Exception:
            pass

async def _render_certificate_pdf_async(template_bytes: bytes, mapping: dict[str, str], qr_link: str, photo_png: bytes | None = None, usage_type: str | None = None) -> tuple[bytes, dict]:
    return await asyncio.to_thread(_render_certificate_pdf_sync, template_bytes, mapping, qr_link, photo_png, usage_type)

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

_LOCALIDADES_INDEX = None 

def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    s = unicodedata.normalize("NFKD", str(value))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    out = []
    for ch in s:
        if ch.isalnum() or ch.isspace():
            out.append(ch)
    s = "".join(out)
    return " ".join(s.split())

def _build_localidades_index():
    global _LOCALIDADES_INDEX
    if _LOCALIDADES_INDEX is not None:
        return _LOCALIDADES_INDEX

    try:
        json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "utils", "localidades.json")
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        localidades = data.get("localidades", [])
        index = {}
        for item in localidades:
            prov_nombre = (item.get("provincia") or {}).get("nombre")
            prov_id = (item.get("provincia") or {}).get("id")
            loc_full_id = item.get("id")
            loc_censal = item.get("localidad_censal") or {}
            loc_censal_nombre = loc_censal.get("nombre")
            loc_censal_id = loc_censal.get("id")

            nombre = item.get("nombre")
            municipio_nombre = (item.get("municipio") or {}).get("nombre")
            dpto_nombre = (item.get("departamento") or {}).get("nombre")

            prov_norm = _normalize_name(prov_nombre)

            def register_candidate(city_raw, priority: int):
                key_city = _normalize_name(city_raw)
                if not (prov_norm and key_city and prov_id and (loc_full_id or loc_censal_id)):
                    return
                k = (prov_norm, key_city)
                candidate = (prov_id, (loc_full_id or loc_censal_id), priority)
                if k not in index:
                    index[k] = candidate
                else:
                    _, _, current_pr = index[k]
                    if priority < current_pr:
                        index[k] = candidate

            register_candidate(nombre, 1)
            register_candidate(municipio_nombre, 2)
            register_candidate(loc_censal_nombre, 3)
            register_candidate(dpto_nombre, 4)
        _LOCALIDADES_INDEX = index
    except Exception:
        _LOCALIDADES_INDEX = {}
    return _LOCALIDADES_INDEX

def _find_localidad_codes(province_name: str | None, city_name: str | None) -> tuple[str | None, str | None]:
    index = _build_localidades_index()
    prov_norm = _normalize_name(province_name)
    city_norm = _normalize_name(city_name)
    if not prov_norm or not city_norm:
        return None, None
    if (prov_norm, city_norm) in index:
        val = index[(prov_norm, city_norm)]
        return val[0], val[1]
    parts = city_norm.split()
    for i in range(len(parts), 0, -1):
        cand = " ".join(parts[:i])
        if (prov_norm, cand) in index:
            val = index[(prov_norm, cand)]
            return val[0], val[1]
    return None, None

def _add_months(base_dt: datetime, months: int) -> datetime:
    if months is None:
        return base_dt
    y = base_dt.year + (base_dt.month - 1 + months) // 12
    m = (base_dt.month - 1 + months) % 12 + 1
    last_day = monthrange(y, m)[1]
    d = min(base_dt.day, last_day)
    try:
        return base_dt.replace(year=y, month=m, day=d)
    except ValueError:
        return base_dt + timedelta(days=30 * months)

def _parse_spanish_month(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        if 1 <= value <= 12:
            return value
        return None
    s = _normalize_name(str(value))
    mapping = {
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12
        }
    return mapping.get(s, None)

async def _calc_vencimiento_from_rules(
    fecha_emision_dt: datetime | None,
    province_name: str | None,
    city_name: str | None,
    usage_code: str | None,
    registration_year: int | None,
    now_tz: pytz.BaseTzInfo,
) -> datetime | None:
    if not fecha_emision_dt:
        return None

    base = fecha_emision_dt.astimezone(now_tz) if hasattr(fecha_emision_dt, "astimezone") else fecha_emision_dt
    prov_code, loc_key = _find_localidad_codes(province_name, city_name)
    usage = (usage_code or "").strip().upper() or None

    if not usage or not registration_year:
        return None

    rule = None
    try:
        if prov_code and loc_key:
            async with get_conn_ctx() as conn:
                rule = await conn.fetchrow(
                    """
                    SELECT localidad_key, usage_code, up_to_36_months, from_3_to_7_years, over_7_years
                    FROM inspection_validity_rules
                    WHERE (localidad_key = $1 OR localidad_key LIKE ($1 || '%'))
                    ORDER BY 
                      CASE WHEN usage_code = $2 THEN 0
                           WHEN usage_code = 'C' THEN 1
                           ELSE 2 END,
                      CASE WHEN localidad_key = $1 THEN 0 ELSE 1 END,
                      localidad_key
                    LIMIT 1
                    """,
                    str(loc_key),
                    usage,
                )
        else:
            pass
    except Exception as e:
        rule = None

    try:
        elapsed_months = (base.year - int(registration_year)) * 12
        elapsed_months = max(0, elapsed_months)
    except Exception:
        return None
    elapsed_years = elapsed_months // 12

    default_up_to_36 = 36  
    default_from_3_to_7 = 24 
    default_over_7 = 12

    if rule:
        up_to_36 = rule["up_to_36_months"]
        from_3_to_7 = rule["from_3_to_7_years"]
        over_7 = rule["over_7_years"]
    else:
        up_to_36 = default_up_to_36
        from_3_to_7 = default_from_3_to_7
        over_7 = default_over_7

    if elapsed_months <= 36 and up_to_36:
        vto_dt = _add_months(base, int(up_to_36))
        return vto_dt

    if 3 <= elapsed_years <= 7 and from_3_to_7:
        vto_dt = _add_months(base, int(from_3_to_7))
        return vto_dt

    if elapsed_years > 7 and over_7:
        vto_dt = _add_months(base, int(over_7))
        return vto_dt

    return None

def _calc_vencimiento_fallback_dt(fecha_emision_dt: datetime | None, car_year: int | None, now_tz: pytz.BaseTzInfo) -> datetime | None:
    if not fecha_emision_dt:
        return None
    base = fecha_emision_dt.astimezone(now_tz) if hasattr(fecha_emision_dt, "astimezone") else fecha_emision_dt
    try:
        cy = datetime.now(now_tz).year
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
    return _years_delta(base, delta_years)


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
    templates_por_cond_with_photo = {
        "apto": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/crts-with-images/certificado_base_apto_photo.pdf",
        "condicional": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/crts-with-images/certificado_base_condicional_photo.pdf",
        "rechazado": "https://uedevplogwlaueyuofft.supabase.co/storage/v1/object/public/certificados/certificado_base_rechazado.pdf",
    }
    
    usage_type = None
    needs_photo_template = False
    photo_png = None
    template_url = None  
    insp = None  
    step_obs_rows = []
    
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
            o.cuit       AS owner_cuit,
            o.razon_social AS owner_razon_social,
            o.street     AS owner_street,
            o.city       AS owner_city,
            o.province   AS owner_province,
            o.email      AS owner_email,
            d.first_name AS driver_first_name,
            d.last_name  AS driver_last_name,
            d.dni        AS driver_dni,
            d.cuit       AS driver_cuit,

            c.license_plate    AS car_plate,
            c.brand            AS car_brand,
            c.model            AS car_model,
            c.manufacture_year AS car_year,
            c.registration_year AS car_registration_year,
            c.engine_brand     AS engine_brand,
            c.engine_number    AS engine_number,
            c.chassis_brand    AS chassis_brand,
            c.chassis_number   AS chassis_number,
            c.fuel_type        AS fuel_type,
            c.insurance        AS insurance,
            c.vehicle_type     AS vehicle_type,
            c.usage_type       AS usage_type,
            c.type_ced         AS car_type_ced,

            ws.razon_social AS workshop_name,
            ws.plant_number AS workshop_plant_number,
            ws.province      AS workshop_province,
            ws.city          AS workshop_city,

            s.id AS sticker_id,
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
        
        usage_type = (row.get("usage_type") or "").strip().upper()
        needs_photo_template = (usage_type == "D" and condicion_raw in ("apto", "condicional"))
        
        if needs_photo_template:
            template_url = templates_por_cond_with_photo.get(condicion_raw, templates_por_cond_with_photo["apto"])
        else:
            template_url = templates_por_cond.get(condicion_raw, templates_por_cond["apto"])
    
        if not template_url:
            template_url = templates_por_cond.get(condicion_raw, templates_por_cond["apto"])

        # Optimización: actualizar sticker solo una vez si es necesario
        if condicion == "Rechazado" and row.get("sticker_id"):
            await conn.execute(
                "UPDATE stickers SET status = 'No Disponible' WHERE id = $1",
                row["sticker_id"]
            )
        
        # Optimización: ejecutar consultas en paralelo cuando sea posible
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

        # Actualizar created_at de la inspección a la fecha actual (zona horaria Argentina)
        if insp and insp.get("id"):
            await conn.execute(
                """
                UPDATE inspections
                SET created_at = NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires'
                WHERE id = $1
                """,
                insp["id"],
            )

        # Ejecutar consultas secuencialmente (asyncpg no permite paralelismo en la misma conexión)
        step_obs_rows = []
        photo_doc = None
        
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
            
            # Consulta de photo_doc si es necesario
            if needs_photo_template and insp.get("id"):
                photo_doc = await conn.fetchrow(
                    """
                    SELECT file_url
                    FROM inspection_documents
                    WHERE inspection_id = $1
                      AND is_front = true
                    LIMIT 1
                    """,
                    insp["id"]
                )
        
        # Optimización: descargar template y foto en paralelo si es necesario
        template_task = None
        photo_task = None
        
        if needs_photo_template and photo_doc and photo_doc.get("file_url"):
            photo_url = photo_doc["file_url"]
            photo_task = _download_and_resize_image_async(photo_url, 246, 170)
        elif needs_photo_template:
            pass

    # Optimización: descargar template y foto en paralelo
    try:
        tasks = [_get_template_bytes_async(template_url)]
        if photo_task:
            tasks.append(photo_task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        template_bytes = results[0]
        if isinstance(template_bytes, Exception):
            raise RuntimeError(f"No se pudo descargar el template, {template_bytes}")
        
        if photo_task:
            photo_png_result = results[1]
            if isinstance(photo_png_result, Exception):
                photo_png = None
            else:
                photo_png = photo_png_result
        else:
            photo_png = None
    except Exception as e:
        raise RuntimeError(f"No se pudo descargar el template, {e}")

    is_second_inspection = bool(insp and insp.get("is_second"))

    base_owner_fullname = " ".join([x for x in [row["owner_first_name"], row["owner_last_name"]] if x])

    documento = None
    documento_label = "D.N.I."
    using_cuit = False
    if row.get("owner_cuit"):
        documento = row["owner_cuit"]
        documento_label = "CUIT"
        using_cuit = True
    elif row.get("owner_dni"):
        documento = row["owner_dni"]
        documento_label = "D.N.I."
        using_cuit = False
    elif row.get("driver_dni"):
        documento = row["driver_dni"]
        documento_label = "D.N.I."
        using_cuit = False
    elif row.get("driver_cuit"):
        documento = row["driver_cuit"]
        documento_label = "CUIT"
        using_cuit = True

    if using_cuit and (row.get("owner_razon_social")):
        owner_fullname = row["owner_razon_social"]
    else:
        owner_fullname = base_owner_fullname

    domicilio = row["owner_street"]
    localidad = row["owner_city"]
    provincia = row["owner_province"]

    argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
    insp_created_at = insp.get("created_at") if insp else None
    if is_second_inspection and insp_created_at:
        fecha_emision_dt = insp_created_at
    else:
        fecha_emision_dt = insp_created_at or row["app_date"]
    fecha_emision = _fmt_date(fecha_emision_dt) if fecha_emision_dt else ""
    
    # Para calcular la fecha de vencimiento, siempre usar la fecha de la aplicación (app_date)
    fecha_base_vencimiento_dt = row["app_date"]
    fecha_vencimiento = None
    vto_dt_for_db = None
    if fecha_base_vencimiento_dt:
        if condicion == "Condicional":
            vto_dt_for_db = fecha_base_vencimiento_dt + timedelta(days=59)
        elif condicion == "Rechazado":
            vto_dt_for_db = None
        else:
            vto_dt_for_db = await _calc_vencimiento_from_rules(
                fecha_emision_dt=fecha_base_vencimiento_dt,
                province_name=row["workshop_province"],
                city_name=row["workshop_city"],
                usage_code=row["usage_type"],
                registration_year=row["car_registration_year"],
                now_tz=argentina_tz,
            )
            if not vto_dt_for_db:
                vto_dt_for_db = _calc_vencimiento_fallback_dt(fecha_base_vencimiento_dt, row["car_year"], argentina_tz)
        fecha_vencimiento = _fmt_date(vto_dt_for_db) if vto_dt_for_db else None
    email_owner = row["owner_email"]
    
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
    crt_numero = f"{row['application_id']}"

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
    obs2_width = 45 if (usage_type and usage_type.strip().upper() == "D") else 90
    global_obs_wrapped2 = textwrap.fill(global_obs_text, width=obs2_width, break_long_words=False, break_on_hyphens=False) if global_obs_text else ""
    observaciones_text = global_obs_wrapped
    observaciones_text2 = global_obs_wrapped2

    oblea = str(row["sticker_number"] or "").strip()
    qr_target = oblea
    qr_link = f"https://www.checkrto.com/qr/{qr_target}"
    oblea_text = oblea if oblea else "Sin Asignar"

    if insp and insp.get("id") and vto_dt_for_db:
        try:
            async with get_conn_ctx() as conn:
                await conn.execute(
                    """
                    UPDATE inspections
                    SET expiration_date = $2
                    WHERE id = $1
                    """,
                    insp["id"],
                    vto_dt_for_db.date(),
                )
        except Exception:
            pass

    dominio_value_for_mapping = row["car_plate"] or ""
    
    mapping = {
        "${fecha_emision}":         fecha_emision or "",
        "${fecha_vencimiento}":     fecha_vencimiento or "",
        "${fecha_em2}":              fecha_emision or "",
        "${fecha_vto}":             fecha_vencimiento or "",
        "${taller}":                row["workshop_name"] or "",
        "${num_reg}":               str(row["workshop_plant_number"] or ""),
        "${nombre_apellido}":       owner_fullname or "",
        "${nombre_apellido2}":      f"{owner_fullname} ({documento_label} {str(documento)}) - TITULAR" or "",
        "${documento}":             str(documento or ""),
        "${documento2}":            str(documento or ""),
        "${domicilio}":             domicilio or "",
        "${f_localidad}":             localidad or "",
        "${localidad2}":            f"{localidad} ({provincia})" if localidad or provincia else "",
        "${provincia}":             provincia or "",
        "${provincia2}":            provincia or "",
        "${patente}":               dominio_value_for_mapping,
        "${patente2}":              dominio_value_for_mapping,
        "${anio}":                  str(row["car_registration_year"] or ""),
        "${marca}":                 row["car_brand"] or "",
        "${modelo}":                row["car_model"] or "",
        "${marca_motor}":           row["engine_brand"] or "",
        "${numero_motor}":          str(row["engine_number"] or ""),
        "${combustible}":           row["fuel_type"] or "",
        "${marca_chasis}":          row["chassis_brand"] or "",
        "${numero_chasis}":         str(row["chassis_number"] or ""),
        "${ced_tipo}":              str(row["car_type_ced"] or ""),
        "${ced_tipo2}":             str(row["car_type_ced"] or ""),
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

    if needs_photo_template:
        mapping["${photo}"] = ""  
    
    try:
        pdf_bytes, counts = await _render_certificate_pdf_async(template_bytes, mapping, qr_link, photo_png, usage_type)
    except Exception as e:
        raise RuntimeError(f"No se pudo renderizar el PDF, {e}")

    file_name = "certificado_2.pdf" if is_second_inspection else "certificado.pdf"
    storage_path = f"certificados/{app_id}/{file_name}"
    try:
        public_url = await _upload_pdf_and_get_public_url_async(pdf_bytes, storage_path)
    except Exception as e:
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
        raise RuntimeError(f"PDF generado, no se pudo actualizar el estado del trámite, {e}")

    # ----- DEBUG RESUMEN FINAL (opcional por env var CRT_DEBUG=1) -----
    if os.getenv("CRT_DEBUG") == "1":
        try:
            prov_dbg = row.get("workshop_province")
            city_dbg = row.get("workshop_city")
            usage_dbg = (row.get("usage_type") or "").strip().upper()
            reg_year_dbg = row.get("car_registration_year")
            reg_mon_dbg = None

            prov_code_dbg, loc_key_dbg = _find_localidad_codes(prov_dbg, city_dbg)

            base_dbg = fecha_emision_dt.astimezone(argentina_tz) if hasattr(fecha_emision_dt, "astimezone") else fecha_emision_dt
            elapsed_months_dbg = None
            elapsed_years_dbg = None
            try:
                if reg_year_dbg:
                    elapsed_months_dbg = (base_dbg.year - int(reg_year_dbg)) * 12
                    elapsed_months_dbg = max(0, elapsed_months_dbg)
                    elapsed_years_dbg = elapsed_months_dbg // 12
                    reg_mon_dbg = base_dbg.month
            except Exception:
                pass

            up36_dbg = 36
            a3_7_dbg = 24
            o7_dbg = 12
            used_lkey_dbg = None
            used_usage_dbg = None
            if prov_code_dbg and loc_key_dbg and usage_dbg:
                try:
                    async with get_conn_ctx() as conn:
                        rule_dbg = await conn.fetchrow(
                            """
                            SELECT localidad_key, usage_code, up_to_36_months, from_3_to_7_years, over_7_years
                            FROM inspection_validity_rules
                            WHERE (localidad_key = $1 OR localidad_key LIKE ($1 || '%'))
                            ORDER BY 
                              CASE WHEN usage_code = $2 THEN 0
                                   WHEN usage_code = 'C' THEN 1
                                   ELSE 2 END,
                              CASE WHEN localidad_key = $1 THEN 0 ELSE 1 END,
                              localidad_key
                            LIMIT 1
                            """,
                            str(loc_key_dbg),
                            usage_dbg,
                        )
                    if rule_dbg:
                        used_lkey_dbg = rule_dbg["localidad_key"]
                        used_usage_dbg = rule_dbg["usage_code"]
                        up36_dbg = rule_dbg["up_to_36_months"] or up36_dbg
                        a3_7_dbg = rule_dbg["from_3_to_7_years"] or a3_7_dbg
                        o7_dbg = rule_dbg["over_7_years"] or o7_dbg
                except Exception as e:
                    print(f"[CRT][RESUMEN] Error consultando regla para debug: {e}")

            bucket_dbg = None
            if elapsed_months_dbg is not None:
                if elapsed_months_dbg <= 36:
                    bucket_dbg = "up_to_36"
                elif elapsed_years_dbg is not None and 3 <= elapsed_years_dbg <= 7:
                    bucket_dbg = "3_to_7_years"
                elif elapsed_years_dbg is not None and elapsed_years_dbg > 7:
                    bucket_dbg = "over_7_years"

        except Exception as e:
            print(f"[CRT][RESUMEN] Error generando resumen final: {e}")

    # Enviar certificado por email si el owner tiene email
    if email_owner and email_owner.strip():
        try:
            await send_certificate_email(
                to_email=email_owner.strip(),
                pdf_bytes=pdf_bytes,
                pdf_filename=file_name,
                owner_name=owner_fullname,
                sticker_number=oblea,
                car_plate=row["car_plate"],
                fecha_emision=fecha_emision,
                fecha_vencimiento=fecha_vencimiento,
                resultado=resultado_final,
                certificate_number=crt_numero,
                workshop_name=row["workshop_name"],
            )
            log.info("Certificado enviado por email a %s para aplicación %s", email_owner, app_id)
        except Exception as e:
            # No fallar la generación del certificado si falla el envío del email
            log.exception("Error enviando certificado por email a %s para aplicación %s: %s", email_owner, app_id, e)

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
