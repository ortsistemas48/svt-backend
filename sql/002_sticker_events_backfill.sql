-- Backfill best-effort del historial de obleas. Correr UNA sola vez, después de 001.
--
-- Solo se puede reconstruir lo que dejó rastro en la base:
--   * Condicional vencido -> stickers.is_expired_application = TRUE
--   * Rechazado           -> la oblea sigue vinculada por cars.sticker_id a un auto
--                            cuya última revisión completada dio Rechazado
-- Los abandonos y las bajas manuales viejas NO son reconstruibles: el flujo rompía el
-- vínculo con el auto y no guardaba nada más. Esas obleas quedan fuera del desglose
-- histórico (la query 3 al final dice cuántas son).

BEGIN;

-- 1) Condicional vencido: fecha estimada = fecha de la revisión + 60 días, que es el
--    umbral con el que el cron las da de baja.
INSERT INTO sticker_events (
  sticker_id, workshop_id, application_id, car_id,
  event, reason, status_after, is_backfill, created_at
)
SELECT
  s.id,
  so.workshop_id,
  a.id,
  a.car_id,
  'Perdida',
  'Condicional Vencido',
  'No Disponible',
  true,
  a.date + INTERVAL '60 days'
FROM stickers s
JOIN sticker_orders so ON so.id = s.sticker_order_id
LEFT JOIN LATERAL (
  SELECT a.id, a.car_id, a.date
  FROM applications a
  JOIN cars c ON c.id = a.car_id
  WHERE a.result = 'Condicional Vencido'
    AND a.workshop_id = so.workshop_id
    AND a.is_deleted IS NOT TRUE
  ORDER BY a.date DESC
  LIMIT 1
) a ON TRUE
WHERE s.status = 'No Disponible'
  AND s.is_expired_application IS TRUE
  AND a.id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM sticker_events e WHERE e.sticker_id = s.id);

-- 2) Rechazado: la oblea conserva el vínculo con el auto, así que se puede datar con
--    la fecha de la revisión que la dio de baja.
INSERT INTO sticker_events (
  sticker_id, workshop_id, application_id, car_id,
  event, reason, status_after, is_backfill, created_at
)
SELECT
  s.id,
  so.workshop_id,
  a.id,
  a.car_id,
  'Perdida',
  'Rechazado',
  'No Disponible',
  true,
  a.date
FROM stickers s
JOIN sticker_orders so ON so.id = s.sticker_order_id
JOIN cars c ON c.sticker_id = s.id
JOIN LATERAL (
  SELECT a.id, a.car_id, a.date
  FROM applications a
  WHERE a.car_id = c.id
    AND a.status = 'Completado'
    AND a.is_deleted IS NOT TRUE
    AND COALESCE(NULLIF(TRIM(a.result_2), ''), NULLIF(TRIM(a.result), '')) = 'Rechazado'
  ORDER BY a.date DESC
  LIMIT 1
) a ON TRUE
WHERE s.status = 'No Disponible'
  AND NOT EXISTS (SELECT 1 FROM sticker_events e WHERE e.sticker_id = s.id);

COMMIT;

-- 3) Control: obleas perdidas que quedaron sin atribuir (abandonos y bajas manuales
--    previas al deploy). No son reconstruibles; el número es solo informativo.
SELECT so.workshop_id, COUNT(*) AS sin_atribuir
FROM stickers s
JOIN sticker_orders so ON so.id = s.sticker_order_id
WHERE s.status = 'No Disponible'
  AND NOT EXISTS (SELECT 1 FROM sticker_events e WHERE e.sticker_id = s.id)
GROUP BY so.workshop_id
ORDER BY sin_atribuir DESC;
