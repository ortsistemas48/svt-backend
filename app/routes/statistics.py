# app/routes/statistics.py
from quart import Blueprint, request, jsonify, g
from app.db import get_conn_ctx
from dateutil import parser
import datetime
import pytz

statistics_bp = Blueprint("statistics", __name__, url_prefix="/statistics")

def _arg_date(name: str, default: datetime.date) -> datetime.date:
    v = (request.args.get(name) or "").strip()
    if not v:
        return default
    try:
        return parser.parse(v).date()
    except Exception:
        raise ValueError(f"Parametro {name} inválido, usa YYYY-MM-DD")

def _range() -> tuple[datetime.date, datetime.date]:
    ar = pytz.timezone("America/Argentina/Buenos_Aires")
    today_ar = datetime.datetime.now(ar).date()
    date_to = _arg_date("to", today_ar)
    date_from = _arg_date("from", date_to - datetime.timedelta(days=13))
    if date_from > date_to:
        date_from, date_to = date_to, date_from
    return date_from, date_to

async def _auth() -> int:
    user_id = g.get("user_id")
    if not user_id:
        raise PermissionError("No autorizado")
    return user_id

# ==============================
# 1) Overview
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/overview", methods=["GET"])
async def statistics_overview(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()

        async with get_conn_ctx() as conn:
            row = await conn.fetchrow(
                """
                WITH base AS (
                    SELECT *
                    FROM applications a
                    WHERE a.workshop_id = $1
                      AND a.is_deleted IS NOT TRUE
                      AND a.owner_id IS NOT NULL
                      AND a.car_id IS NOT NULL
                      AND a.date::date BETWEEN $2 AND $3
                )
                SELECT
                  COUNT(*)                                   AS created,
                  COUNT(CASE WHEN status = 'Completado' THEN 1 END) AS completed,
                  COUNT(CASE WHEN status = 'En Cola' THEN 1 END)    AS in_queue,
                  COUNT(CASE WHEN status = 'Completado' AND result = 'Apto' THEN 1 END) AS approved
                FROM base
                """,
                workshop_id, date_from, date_to
            )
            
            # Obtener cantidad de usuarios activos del taller
            active_users_count = await conn.fetchval(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM workshop_users
                WHERE workshop_id = $1
                """,
                workshop_id
            )

        created = row["created"] or 0
        completed = row["completed"] or 0
        in_queue = row["in_queue"] or 0
        approved = row["approved"] or 0
        approval_rate = round((approved / completed) * 100, 2) if completed > 0 else 0.0
        active_users = active_users_count or 0

        return jsonify({
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "workshop_id": workshop_id,
            "totals": {
                "created": created,
                "completed": completed,
                "in_queue": in_queue,
                "approved": approved,
                "approval_rate": approval_rate,
                "active_users": active_users
            }
        }), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 2) Serie diaria
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/daily", methods=["GET"])
async def statistics_daily(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()

        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                WITH base AS (
                  SELECT a.id, a.date::date AS d, a.status, a.result
                  FROM applications a
                  WHERE a.workshop_id = $1
                    AND a.is_deleted IS NOT TRUE
                    AND a.owner_id IS NOT NULL
                    AND a.car_id IS NOT NULL
                    AND a.date::date BETWEEN $2 AND $3
                )
                SELECT d,
                  COUNT(*)                                  AS created,
                  COUNT(CASE WHEN status = 'Completado' THEN 1 END) AS completed,
                  COUNT(CASE WHEN status = 'Completado' AND result = 'Apto' THEN 1 END) AS approved
                FROM base
                GROUP BY d
                ORDER BY d
                """,
                workshop_id, date_from, date_to
            )

        # llenar días faltantes
        by_date = {r["d"]: r for r in rows}
        items = []
        cur = date_from
        while cur <= date_to:
            r = by_date.get(cur)
            items.append({
                "date": cur.isoformat(),
                "created": int(r["created"]) if r else 0,
                "completed": int(r["completed"]) if r else 0,
                "approved": int(r["approved"]) if r else 0,
            })
            cur += datetime.timedelta(days=1)

        return jsonify({"items": items, "total_days": len(items)}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 3) Breakdown por status
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/status-breakdown", methods=["GET"])
async def statistics_status_breakdown(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()
        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT status, COUNT(*) AS c
                FROM applications a
                WHERE a.workshop_id = $1
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND a.date::date BETWEEN $2 AND $3
                GROUP BY status
                ORDER BY c DESC NULLS LAST
                """,
                workshop_id, date_from, date_to
            )
        items = [{"status": r["status"] or "Sin dato", "count": int(r["c"])} for r in rows]
        total = sum(i["count"] for i in items)
        return jsonify({"items": items, "total": total}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 4) Breakdown por resultado
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/results-breakdown", methods=["GET"])
async def statistics_results_breakdown(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()
        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT result, COUNT(*) AS c
                FROM applications a
                WHERE a.workshop_id = $1
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND a.status = 'Completado'
                  AND a.date::date BETWEEN $2 AND $3
                GROUP BY result
                ORDER BY c DESC NULLS LAST
                """,
                workshop_id, date_from, date_to
            )
        items = [{"result": r["result"] or "Sin dato", "count": int(r["c"])} for r in rows]
        total = sum(i["count"] for i in items)
        return jsonify({"items": items, "total": total}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 5) Top modelos
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/top-models", methods=["GET"])
async def statistics_top_models(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()
        limit = request.args.get("limit", 8, type=int)
        limit = max(1, min(limit, 50))

        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT c.brand, c.model, COUNT(*) AS c
                FROM applications a
                JOIN cars c ON c.id = a.car_id
                WHERE a.workshop_id = $1
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND a.date::date BETWEEN $2 AND $3
                  AND (NULLIF(trim(c.model), '') IS NOT NULL OR NULLIF(trim(c.brand), '') IS NOT NULL)
                GROUP BY c.brand, c.model
                ORDER BY c DESC NULLS LAST
                LIMIT $4
                """,
                workshop_id, date_from, date_to, limit
            )

        items = [{"brand": r["brand"], "model": r["model"], "count": int(r["c"])} for r in rows]
        return jsonify({"items": items, "total_models": len(items)}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 6) Top marcas
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/top-brands", methods=["GET"])
async def statistics_top_brands(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()
        limit = request.args.get("limit", 5, type=int)
        limit = max(1, min(limit, 50))

        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT c.brand, COUNT(*) AS c
                FROM applications a
                JOIN cars c ON c.id = a.car_id
                WHERE a.workshop_id = $1
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND a.date::date BETWEEN $2 AND $3
                  AND NULLIF(trim(c.brand), '') IS NOT NULL
                GROUP BY c.brand
                ORDER BY c DESC NULLS LAST
                LIMIT $4
                """,
                workshop_id, date_from, date_to, limit
            )

        items = [{"brand": r["brand"], "count": int(r["c"])} for r in rows]
        return jsonify({"items": items, "total": len(items)}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

@statistics_bp.route("/workshop/<int:workshop_id>/usage-types", methods=["GET"])
async def statistics_usage_types(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()

        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT c.usage_type, COUNT(*) AS c
                FROM applications a
                JOIN cars c ON c.id = a.car_id
                WHERE a.workshop_id = $1
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND a.date::date BETWEEN $2 AND $3
                  AND NULLIF(trim(c.usage_type), '') IS NOT NULL
                GROUP BY c.usage_type
                ORDER BY c DESC NULLS LAST
                """,
                workshop_id, date_from, date_to
            )

        items = [{"usage_type": r["usage_type"] or "Sin dato", "count": int(r["c"])} for r in rows]
        total = sum(i["count"] for i in items)
        return jsonify({"items": items, "total": total}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 8) Errores más comunes (pasos con Condicional o Rechazado)
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/common-errors", methods=["GET"])
async def statistics_common_errors(workshop_id: int):
    try:
        await _auth()
        date_from, date_to = _range()
        limit = request.args.get("limit", 3, type=int)
        limit = max(1, min(limit, 20))

        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT s.name AS step_name, COUNT(*) AS c
                FROM applications a
                JOIN inspections i ON i.application_id = a.id
                JOIN inspection_details idet ON idet.inspection_id = i.id
                JOIN steps s ON s.id = idet.step_id
                WHERE a.workshop_id = $1
                  AND a.is_deleted IS NOT TRUE
                  AND a.owner_id IS NOT NULL
                  AND a.car_id IS NOT NULL
                  AND a.date::date BETWEEN $2 AND $3
                  AND idet.status IN ('Condicional', 'Rechazado')
                  AND NULLIF(trim(s.name), '') IS NOT NULL
                GROUP BY s.name
                ORDER BY c DESC NULLS LAST
                LIMIT $4
                """,
                workshop_id, date_from, date_to, limit
            )
        total_errors = sum(int(r["c"]) for r in rows)
        items = []
        for r in rows:
            count = int(r["c"])
            percentage = round((count / total_errors) * 100) if total_errors > 0 else 0
            items.append({
                "step_name": r["step_name"] or "Sin dato",
                "count": count,
                "percentage": percentage
            })

        return jsonify({"items": items, "total": total_errors}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500

# ==============================
# 9) Últimos vencimientos
# ==============================
@statistics_bp.route("/workshop/<int:workshop_id>/upcoming-expirations", methods=["GET"])
async def statistics_upcoming_expirations(workshop_id: int):
    try:
        await _auth()
        limit = request.args.get("limit", 3, type=int)
        limit = max(1, min(limit, 20))

        ar = pytz.timezone("America/Argentina/Buenos_Aires")
        today_ar = datetime.datetime.now(ar).date()
        max_date = today_ar + datetime.timedelta(days=90)

        async with get_conn_ctx() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    ii.expiration_date,
                    c.license_plate,
                    p.phone_number,
                    p.email,
                    p.first_name || ' ' || p.last_name AS name
                    FROM applications a
                    JOIN (
                    -- Elegimos una sola inspección por aplicación, priorizando las secundarias
                    SELECT DISTINCT ON (i.application_id)
                        i.application_id,
                        i.expiration_date
                    FROM inspections i
                    WHERE i.expiration_date IS NOT NULL
                    ORDER BY i.application_id, i.is_second DESC, i.expiration_date DESC
                    ) AS ii
                    ON ii.application_id = a.id
                    JOIN cars c
                    ON c.id = a.car_id
                    LEFT JOIN persons p
                    ON p.id = a.owner_id
                    WHERE a.workshop_id = $1
                    AND a.is_deleted IS NOT TRUE
                    AND a.owner_id IS NOT NULL
                    AND a.car_id IS NOT NULL
                    AND ii.expiration_date::date >= $2
                    AND ii.expiration_date::date <= $3
                    ORDER BY ii.expiration_date::date ASC
                    LIMIT $4;
                """,
                workshop_id, today_ar, max_date, limit
            )
        items = []
        for r in rows:
            exp_date = r["expiration_date"]
            if exp_date:
                exp_date_obj = exp_date if isinstance(exp_date, datetime.date) else parser.parse(str(exp_date)).date()
                days_until = (exp_date_obj - today_ar).days
                
                contact = r["phone_number"] or r["email"] or r["name"] or "Sin contacto"
                contact_type = "Tel" if r["phone_number"] else ("Email" if r["email"] else "Nombre")
                if contact_type == "Tel" and not contact.startswith("Tel:"):
                    contact = f"Tel: {contact}"
                
                items.append({
                    "license_plate": r["license_plate"] or "N/D",
                    "contact": contact,
                    "days_until": days_until,
                    "expiration_date": exp_date_obj.isoformat()
                })

        return jsonify({"items": items, "total": len(items)}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 401
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error interno, {e}"}), 500