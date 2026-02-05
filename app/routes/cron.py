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
    - is_expired no es TRUE (aún no procesadas por este cron)
    - Pasaron más de 60 días desde el campo date

    Para cada aplicación con car_id:
    - Si es la primera vez del vehículo (única application con ese car_id): se desasigna
      la oblea (sticker_id = NULL en cars), el sticker se marca 'No Disponible' e
      is_expired_application TRUE.
    - Si no es la primera pero es la última (sin posteriores): no se toca la oblea.
    En ambos casos se actualiza result a 'Condicional Vencido' e is_expired = TRUE,
    para que no se vuelvan a procesar en futuras ejecuciones.
    """
    if not _validate_api_key():
        return jsonify({"error": "API key inválida o faltante"}), 401

    async with get_conn_ctx() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT id, user_id, workshop_id, date, status, result, result_2, car_id
                FROM applications
                WHERE result = 'Condicional'
                  AND result_2 IS NULL
                  AND (is_expired IS NOT TRUE OR is_expired IS NULL)
                  AND date + INTERVAL '60 days' < NOW()
                ORDER BY date ASC
                """
            )

            stickers_updated = []
            applications_updated = []  # app_ids con result actualizado a Condicional Vencido

            for r in rows:
                car_id = r["car_id"]
                app_id = r["id"]
                app_date = r["date"]
                if not car_id:
                    continue

                # Obtener sticker_id del car
                car_row = await conn.fetchrow(
                    "SELECT sticker_id FROM cars WHERE id = $1",
                    car_id,
                )
                if not car_row or not car_row["sticker_id"]:
                    continue

                sticker_id = car_row["sticker_id"]

                # Contar applications del car en el historial
                app_count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM applications
                    WHERE car_id = $1 AND (is_deleted IS NOT TRUE OR is_deleted IS NULL)
                    """,
                    car_id,
                )
                app_count = app_count or 0

                # Hay applications posteriores a esta?
                apps_after = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM applications
                    WHERE car_id = $1 AND id != $2
                      AND (is_deleted IS NOT TRUE OR is_deleted IS NULL)
                      AND date > $3
                    """,
                    car_id, app_id, app_date,
                )
                has_apps_after = (apps_after or 0) > 0

                # Solo actuar si: (a) es la única app del car, o (b) tuvo anteriores pero no hay posteriores
                if app_count == 1 or (app_count > 1 and not has_apps_after):
                    # En ambos casos: actualizar result de la application a Condicional Vencido
                    await conn.execute(
                        """
                        UPDATE applications
                        SET result = 'Condicional Vencido', is_expired = TRUE
                        WHERE id = $1
                        """,
                        app_id,
                    )
                    applications_updated.append(app_id)
                    if app_count == 1:
                        # Primera vez del vehículo: desasignar oblea y marcar sticker
                        await conn.execute(
                            "UPDATE cars SET sticker_id = NULL WHERE id = $1",
                            car_id,
                        )
                        await conn.execute(
                            """
                            UPDATE stickers
                            SET status = 'No Disponible', is_expired_application = TRUE
                            WHERE id = $1
                            """,
                            sticker_id,
                        )
                        stickers_updated.append(sticker_id)
                    # Si app_count > 1 y no hay posteriores: no se toca la oblea

    items = [
        {
            "id": r["id"],
            "user_id": r["user_id"],
            "workshop_id": r["workshop_id"],
            "date": r["date"].isoformat() if r["date"] else None,
            "status": r["status"],
            "result": "Condicional Vencido" if r["id"] in applications_updated else r["result"],
            "result_2": r["result_2"],
            "car_id": r["car_id"],
        }
        for r in rows
    ]

    return jsonify({
        "count": len(items),
        "applications": items,
        "stickers_updated": stickers_updated,
    }), 200
