# app/sticker_events.py
"""
Dominio de obleas: historial de eventos y la regla de "oblea pegada".

La tabla `stickers` solo guarda el estado actual, y varios flujos (abandono,
condicional vencido) rompen el vínculo `cars.sticker_id` antes de marcar la oblea
como 'No Disponible'. Sin un registro del evento es imposible saber por qué ni
cuándo se perdió una oblea, así que cada transición relevante se persiste acá.

Siempre se recibe la conexión del llamador para que el evento quede dentro de la
misma transacción que el UPDATE sobre stickers, y estado e historial no diverjan.

Acá vive también `sticker_was_applied`, que es la pregunta que los tres flujos de
pérdida necesitan antes de dar de baja una oblea. Está centralizada porque la
respondían por separado y con la regla equivocada.
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


async def sticker_was_applied(conn, *, car_id: int, before_application_id: int) -> bool:
    """
    ¿Alguna revisión previa del auto dejó la oblea pegada en el parabrisas?

    La oblea se pega cuando la revisión da Apto, o cuando al Condicional se le
    efectúa la segunda revisión. No alcanza con que exista una revisión previa
    completada, que es lo que preguntaban antes los flujos de pérdida: un
    'Condicional Vencido' está Completado pero nunca llegó a pegar la oblea, y
    contarlo como pegada hacía que la oblea de la revisión siguiente no se diera
    de baja y quedara 'En Uso' para siempre.

    Solo mira revisiones anteriores a `before_application_id`, que es la que se
    está por resolver.
    """
    if not car_id or not before_application_id:
        return False

    return await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM applications a
            WHERE a.car_id = $1
              AND a.id < $2
              AND a.status = 'Completado'
              AND a.is_deleted IS NOT TRUE
              AND (
                TRIM(a.result) = 'Apto'
                OR (
                  TRIM(a.result) = 'Condicional'
                  AND EXISTS (
                    SELECT 1 FROM inspections i
                    WHERE i.application_id = a.id
                      AND COALESCE(i.is_second, FALSE) = TRUE
                  )
                )
              )
        )
        """,
        car_id, before_application_id,
    )


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
    created_by: str | None = None,
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::uuid)
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
