-- MVP: Alerta de Conflicto de Andén en Tiempo Real — arquitectura Event-Driven
-- Esquema de auditoría para PostgreSQL 16.
--
-- IMPORTANTE: la detección crítica NO usa la base de datos.
-- El stream vive en Redis + ventanas deslizantes en memoria.
-- Esta tabla es únicamente historial/auditoría de alertas.

-- Migración MVP → Event-Driven: si existe la tabla antigua (sin uid),
-- se recrea porque es sólo auditoría (la detección vive en memoria).
DO $$
BEGIN
    IF EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'conflictos'
        )
        AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'conflictos' AND column_name = 'uid'
        ) THEN
        DROP TABLE conflictos;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS conflictos (
    uid                     VARCHAR(160) PRIMARY KEY,
    estacion_id             VARCHAR(10)  NOT NULL,
    nombre_estacion         VARCHAR(120) NOT NULL DEFAULT '',
    region                  VARCHAR(60)  NOT NULL DEFAULT '',
    ciudad                  VARCHAR(120) NOT NULL DEFAULT '',
    gleis                   VARCHAR(10)  NOT NULL,
    tren_1_id               VARCHAR(40)  NOT NULL,
    tren_2_id               VARCHAR(40)  NOT NULL,
    minutos_diferencia      FLOAT        NOT NULL,
    causa                   VARCHAR(40)  NOT NULL DEFAULT 'solapamiento_programado',
    retraso_min             FLOAT        NOT NULL DEFAULT 0,
    sugerencia_gleiswechsel BOOLEAN      NOT NULL DEFAULT FALSE,
    gleis_alternativo       VARCHAR(10),
    estado                  VARCHAR(12)  NOT NULL DEFAULT 'activo',
    primera_deteccion       TIMESTAMP    NOT NULL DEFAULT NOW(),
    actualizado_en          TIMESTAMP    NOT NULL DEFAULT NOW(),
    detecciones             INTEGER      NOT NULL DEFAULT 1
);

-- Nivel de señal: 'conflicto' = ocupaciones solapadas de verdad (dos trenes
-- en el mismo Gleis a la vez, típicamente por retraso); 'riesgo' = headway
-- ajustado (< umbral) sin solape (p. ej. cadencia S-Bahn).
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS nivel VARCHAR(12) NOT NULL DEFAULT 'conflicto';
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS solape_min FLOAT NOT NULL DEFAULT 0;
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS margen_min FLOAT NOT NULL DEFAULT 0;
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS region VARCHAR(60) NOT NULL DEFAULT '';
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS ciudad VARCHAR(120) NOT NULL DEFAULT '';
-- Número de tren real (Zugnummer) para identificar cada servicio; la ruta/
-- línea se descarta a propósito (el usuario pide el nº de tren, no el de ruta).
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS tren_1_numero VARCHAR(20) NOT NULL DEFAULT '';
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS tren_1_categoria VARCHAR(12) NOT NULL DEFAULT '';
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS tren_2_numero VARCHAR(20) NOT NULL DEFAULT '';
ALTER TABLE conflictos ADD COLUMN IF NOT EXISTS tren_2_categoria VARCHAR(12) NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_conflictos_region
    ON conflictos (region, ciudad);

CREATE INDEX IF NOT EXISTS idx_conflictos_estado
    ON conflictos (estado, actualizado_en DESC);

CREATE INDEX IF NOT EXISTS idx_conflictos_timestamp
    ON conflictos (primera_deteccion DESC);

-- Auditoría de ocupaciones (historial, no usado por la detección).
CREATE TABLE IF NOT EXISTS ocupacion_andenes (
    id              SERIAL PRIMARY KEY,
    estacion_id     VARCHAR(10)  NOT NULL,
    nombre_estacion VARCHAR(120) NOT NULL,
    gleis           VARCHAR(10)  NOT NULL,
    tren_id         VARCHAR(40)  NOT NULL,
    hora_llegada    TIMESTAMP    NOT NULL,
    hora_salida     TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ocupacion_estacion_gleis_tren
        UNIQUE (estacion_id, gleis, tren_id)
);

CREATE INDEX IF NOT EXISTS idx_ocupacion_estacion_gleis
    ON ocupacion_andenes (estacion_id, gleis);
