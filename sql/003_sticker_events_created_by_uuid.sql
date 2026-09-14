-- Corrige el tipo de sticker_events.created_by.
-- Correr en Supabase (SQL Editor) ANTES de deployar el backend con este fix.
--
-- 001 la declaró `bigint`, pero `users.id` es uuid (todo el resto del código bindea
-- los ids de usuario con ::uuid). Como consecuencia, cada evento disparado por un
-- usuario logueado explotaba con:
--   DataError: invalid input for query argument $9: '<uuid>' ('str' object cannot be
--   interpreted as an integer)
-- y, al estar dentro de la transacción, se revertía también el UPDATE sobre stickers.
-- Solo funcionaban los eventos del cron, que pasan created_by = NULL.

BEGIN;

-- No puede haber valores no nulos: todo INSERT con created_by no nulo falló antes de
-- llegar a la base, y el backfill de 002 nunca setea la columna. Verificación previa:
--   SELECT count(*) FROM sticker_events WHERE created_by IS NOT NULL;  -- debe dar 0
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM sticker_events WHERE created_by IS NOT NULL) THEN
    RAISE EXCEPTION 'sticker_events.created_by tiene valores no nulos, revisar a mano';
  END IF;
END $$;

ALTER TABLE sticker_events
  ALTER COLUMN created_by TYPE uuid USING NULL::uuid;

COMMIT;
