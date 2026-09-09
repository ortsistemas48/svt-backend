# app/sticker_events.py
"""
Historial de obleas.

La tabla `stickers` solo guarda el estado actual, y varios flujos (abandono,
condicional vencido) rompen el vínculo `cars.sticker_id` antes de marcar la oblea
como 'No Disponible'. Sin un registro del evento es imposible saber por qué ni
cuándo se perdió una oblea, así que cada transición relevante se persiste acá.

Siempre se recibe la conexión del llamador para que el evento quede dentro de la
misma transacción que el UPDATE sobre stickers, y estado e historial no diverjan.
"""
import logging

log = logging.getLogger(__name__)

# Tipos de evento
EVENT_LOST = "Perdida"
EVENT_RELEASED = "Liberada"
EVENT_ASSIGNED = "Asignada"

# Motivos de pérdida
LOSS_ABANDONO = "Abandono"
LOSS_CONDICIONAL = "Condicional Vencido"
LOSS_RECHAZADO = "Rechazado"
LOSS_MANUAL = "Baja Manual"

LOSS_REASONS = (LOSS_ABANDONO, LOSS_CONDICIONAL, LOSS_RECHAZADO, LOSS_MANUAL)


async def record_sticker_event(
    conn,
    *,
    sticker_id: int,
    event: str,
    reason: str | None = None,
    workshop_id: int | None = None,
    application_id: int | None = None,
    car_id: int | None = None,
    status_before: str | None = None,
    status_after: str | None = None,
    created_by: int | None = None,
) -> int | None:
    """
    Registra un evento de oblea. Devuelve el id del evento, o None si no se pudo
    registrar (por ejemplo, si la oblea no tiene orden asociada).

    Si no se pasa workshop_id se resuelve desde la orden de la oblea.
    """
    if not sticker_id:
        return None

    if workshop_id is None:
        workshop_id = await conn.fetchval(
            """
            SELECT so.workshop_id
            FROM stickers s
            JOIN sticker_orders so ON so.id = s.sticker_order_id
            WHERE s.id = $1
            """,
            sticker_id,
        )

    if workshop_id is None:
        log.warning(
            "No se pudo registrar evento '%s' de la oblea %s: sin workshop_id",
            event, sticker_id,
        )
        return None

    return await conn.fetchval(
        """
        INSERT INTO sticker_events (
            sticker_id, workshop_id, application_id, car_id,
            event, reason, status_before, status_after, created_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        """,
        sticker_id, workshop_id, application_id, car_id,
        event, reason, status_before, status_after, created_by,
    )


async def record_sticker_loss(conn, *, sticker_id: int, reason: str, **kwargs) -> int | None:
    """Atajo para registrar una pérdida de oblea (status pasa a 'No Disponible')."""
    kwargs.setdefault("status_after", "No Disponible")
    return await record_sticker_event(
        conn, sticker_id=sticker_id, event=EVENT_LOST, reason=reason, **kwargs
    )


async def record_sticker_release(conn, *, sticker_id: int, **kwargs) -> int | None:
    """Atajo para registrar que una oblea vuelve a estar disponible."""
    kwargs.setdefault("status_after", "Disponible")
    return await record_sticker_event(
        conn, sticker_id=sticker_id, event=EVENT_RELEASED, **kwargs
    )
