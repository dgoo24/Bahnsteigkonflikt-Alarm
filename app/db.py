"""Capa de acceso a datos (PostgreSQL) para el MVP de conflictos de andén."""

import logging
import os
import time
from contextlib import contextmanager

try:  # PostgreSQL es opcional (Streamlit Cloud no tiene servidor de BD)
    import psycopg2
    from psycopg2.extras import RealDictCursor

    PSYCOPG2_DISPONIBLE = True
except Exception:  # pragma: no cover - depende del entorno
    psycopg2 = None
    RealDictCursor = None
    PSYCOPG2_DISPONIBLE = False

logger = logging.getLogger(__name__)

# La auditoría en PostgreSQL es un sink opcional: en Streamlit Cloud se
# desactiva (AUDIT_ENABLED=0) porque no hay servidor de base de datos.
AUDIT_ENABLED = (
    os.getenv("AUDIT_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)


def get_conn_params() -> dict:
    """Parámetros de conexión tomados de variables de entorno."""
    return {
        "host": os.getenv("DB_HOST", "db"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "db_andenes"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }


def get_connection(retries: int = 10, delay: float = 2.0):
    """Abre una conexión a PostgreSQL con reintentos."""
    if not PSYCOPG2_DISPONIBLE:
        raise RuntimeError("psycopg2 no está instalado: auditoría no disponible")
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**get_conn_params())
        except psycopg2.OperationalError as exc:  # pragma: no cover
            last_error = exc
            logger.warning("Intento %s/%s de conexión a la DB falló: %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"No se pudo conectar a PostgreSQL: {last_error}")


@contextmanager
def get_cursor(commit: bool = False, cursor_factory=RealDictCursor):
    """Context manager que entrega un cursor y gestiona commit/rollback."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()


def upsert_conflicto_auditoria(conflicto: dict, action: str) -> None:
    """Sink de auditoría: persiste el estado del conflicto en PostgreSQL.

    Se ejecuta fuera del camino crítico del stream (asyncio.to_thread).
    La tabla es historial/auditoría — la detección vive en memoria.
    """
    if not (AUDIT_ENABLED and PSYCOPG2_DISPONIBLE):
        return
    sql = """
        INSERT INTO conflictos
            (uid, estacion_id, nombre_estacion, region, ciudad, gleis,
             tren_1_id, tren_2_id,
             tren_1_numero, tren_1_categoria, tren_2_numero, tren_2_categoria,
             nivel, solape_min, margen_min, minutos_diferencia, causa,
             retraso_min, sugerencia_gleiswechsel,
             gleis_alternativo, estado, primera_deteccion, actualizado_en)
        VALUES
            (%(uid)s, %(estacion_id)s, %(nombre_estacion)s, %(region)s,
             %(ciudad)s, %(gleis)s,
             %(tren_1_id)s, %(tren_2_id)s,
             %(tren_1_numero)s, %(tren_1_categoria)s,
             %(tren_2_numero)s, %(tren_2_categoria)s,
             %(nivel)s, %(solape_min)s,
             %(margen_min)s, %(minutos_diferencia)s, %(causa)s,
             %(retraso_min)s, %(sugerencia_gleiswechsel)s,
             %(gleis_alternativo)s, %(estado)s, %(primera_deteccion)s,
             %(actualizado_en)s)
        ON CONFLICT (uid) DO UPDATE SET
            region              = EXCLUDED.region,
            ciudad              = EXCLUDED.ciudad,
            tren_1_numero       = EXCLUDED.tren_1_numero,
            tren_1_categoria    = EXCLUDED.tren_1_categoria,
            tren_2_numero       = EXCLUDED.tren_2_numero,
            tren_2_categoria    = EXCLUDED.tren_2_categoria,
            nivel               = EXCLUDED.nivel,
            solape_min          = EXCLUDED.solape_min,
            margen_min          = EXCLUDED.margen_min,
            minutos_diferencia  = EXCLUDED.minutos_diferencia,
            causa               = EXCLUDED.causa,
            retraso_min         = EXCLUDED.retraso_min,
            sugerencia_gleiswechsel = EXCLUDED.sugerencia_gleiswechsel,
            gleis_alternativo   = EXCLUDED.gleis_alternativo,
            estado              = EXCLUDED.estado,
            actualizado_en      = EXCLUDED.actualizado_en,
            detecciones         = conflictos.detecciones + 1
    """
    row = {**conflicto}
    row["estado"] = {"resuelto": "resuelto"}.get(action, "activo")
    # timestamp_alerta no se mapea: la columna es primera_deteccion.
    row.setdefault("primera_deteccion", row.get("timestamp_alerta"))
    try:
        with get_cursor(commit=True) as cur:
            cur.execute(sql, row)
    except Exception:  # pragma: no cover - la auditoría nunca rompe el stream
        logger.exception("Auditoría de conflicto falló (uid=%s)", row.get("uid"))
