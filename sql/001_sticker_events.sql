-- Historial de obleas.
-- Correr en Supabase (SQL Editor) antes de deployar el backend.
--
-- Motivo: `stickers` solo guarda el estado actual y los flujos de abandono y de
-- condicional vencido rompen el vínculo `cars.sticker_id` antes de marcar la oblea como
-- 'No Disponible'. Sin este historial es imposible saber por qué ni cuándo se perdió.

CREATE TABLE IF NOT EXISTS sticker_events (
  id             bigserial   PRIMARY KEY,
  sticker_id     bigint      NOT NULL REFERENCES stickers(id) ON DELETE CASCADE,
  workshop_id    bigint      NOT NULL,
  application_id bigint      REFERENCES applications(id) ON DELETE SET NULL,
  car_id         bigint      REFERENCES cars(id) ON DELETE SET NULL,
  event          text        NOT NULL,   -- 'Perdida' | 'Liberada' | 'Asignada'
  reason         text,                   -- 'Abandono' | 'Condicional Vencido' | 'Rechazado' | 'Baja Manual'
  status_before  text,
  status_after   text,
  created_by     bigint,                 -- user_id; NULL cuando lo dispara el cron
  is_backfill    boolean     NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now()
);

-- Sin índice único sobre (sticker_id, event): una oblea puede perderse, recuperarse
-- manualmente y volver a perderse.
CREATE INDEX IF NOT EXISTS sticker_events_workshop_idx
  ON sticker_events (workshop_id, event, created_at);
CREATE INDEX IF NOT EXISTS sticker_events_sticker_idx
  ON sticker_events (sticker_id, created_at DESC);
