CREATE TABLE IF NOT EXISTS kafka_eventos_procesados (
    event_id CHAR(36) NOT NULL PRIMARY KEY,
    flujo VARCHAR(32) NOT NULL,
    entidad_id BIGINT NOT NULL,
    tipo_cuenta VARCHAR(255) NOT NULL,
    ocurrido_en DATETIME(6) NOT NULL
);
