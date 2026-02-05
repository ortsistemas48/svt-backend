-- Agrega la columna is_expired_application a la tabla stickers.
-- Se usa cuando un sticker se marca como 'No Disponible' por aplicación condicional vencida
-- (60+ días sin segunda inspección) en un vehículo que nunca volvió al sistema.
ALTER TABLE stickers
ADD COLUMN IF NOT EXISTS is_expired_application BOOLEAN DEFAULT FALSE;
