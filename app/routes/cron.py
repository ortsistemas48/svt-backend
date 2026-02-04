"""
Cron endpoints para ser llamados desde servicios externos (ej: cron-job.org).
Protegidos por API key via header X-Api-Key.
"""
from quart import Blueprint, request, jsonify, current_app

from app.db import get_conn_ctx

cron_bp = Blueprint("cron", __name__)


def _validate_api_key() -> bool:
    """Valida que el header X-Api-Key coincida con CRON_API_KEY."""
    api_key = request.headers.get("X-Api-Key")
    expected = current_app.config.get("CRON_API_KEY")
    if not expected:
        return False
    return api_key == expected and bool(api_key)


@cron_bp.route("/condicional-expired", methods=["GET"])
async def get_condicional_expired():
    """
    Busca en la tabla applications las revisiones que:
    - result = 'Condicional'
    - result_2 IS NULL (sin segunda inspección)
    - Pasaron más de 60 días desde el campo date
    """
    if not _validate_api_key():
        return jsonify({"error": "API key inválida o faltante"}), 401

    async with get_conn_ctx() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, workshop_id, date, status, result, result_2
            FROM applications
            WHERE result = 'Condicional'
              AND result_2 IS NULL
              AND date + INTERVAL '60 days' < NOW()
            ORDER BY date ASC
            """
        )

    items = [
        {
            "id": r["id"],
            "user_id": r["user_id"],
            "workshop_id": r["workshop_id"],
            "date": r["date"].isoformat() if r["date"] else None,
            "status": r["status"],
            "result": r["result"],
            "result_2": r["result_2"],
        }
        for r in rows
    ]

    return jsonify({"count": len(items), "applications": items}), 200
