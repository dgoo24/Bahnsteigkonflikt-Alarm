"""API Backend FastAPI — arquitectura Event-Driven con SSE.

- **Lifespan**: arranca las tasks de ingesta (productor con datos REALES de
  la DB API → Redis Pub/Sub) y detección (stream processor con ventanas
  deslizantes en memoria).
- **SSE**: ``GET /api/stream/conflictos`` empuja alertas en tiempo real
  (sustituye al polling HTTP de 5 s del frontend).
- **PostgreSQL**: sólo auditoría/historial de alertas, desacoplado del
  camino crítico de la detección.

Este módulo se despliega con Docker (``uvicorn app.api:app``). La versión de
Streamlit Cloud vive en ``app/main.py`` y reutiliza el mismo pipeline
(productor + detector) con un bus de eventos local.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.db import get_cursor
from app.detector import detector, run_detector
from app.events import CH_CONFLICTOS, REDIS_URL
from app.ingest import producer, run_ingestor

import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("app.main")

STATIC_DIR = Path(__file__).parent / "static"
SSE_HEARTBEAT_SEG = float(os.getenv("SSE_HEARTBEAT_SEG", "15"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tasks Event-Driven: productor y consumidor del stream.
    ingest_task = asyncio.create_task(run_ingestor())
    detect_task = asyncio.create_task(run_detector())
    logger.info("Tasks Event-Driven iniciadas (ingest + detector)")
    try:
        yield
    finally:
        ingest_task.cancel()
        detect_task.cancel()
        await asyncio.gather(ingest_task, detect_task, return_exceptions=True)
        await producer.stop()
        await detector.stop()
        logger.info("Tasks Event-Driven detenidas")


app = FastAPI(
    title="MVP - Alerta de Conflicto de Andén en Tiempo Real (Event-Driven)",
    version="2.0.0",
    lifespan=lifespan,
)


# ── SSE: push de conflictos en tiempo real ──────────────────────────────

@app.get("/api/stream/conflictos")
async def stream_conflictos(request: Request):
    """Server-Sent Events: empuja cada alerta del detector (nuevo /
    actualizado / resuelto) sin que el cliente tenga que hacer polling."""

    async def event_stream():
        # Estado inicial: snapshot de los conflictos activos en memoria.
        snapshot = detector.snapshot()
        yield (
            "event: snapshot\n"
            f"data: {json.dumps({'conflictos': snapshot}, ensure_ascii=False)}\n\n"
        )

        client = aioredis.from_url(REDIS_URL, decode_responses=True)
        pubsub = client.pubsub()
        await pubsub.subscribe(CH_CONFLICTOS)
        try:
            while True:
                if await request.is_disconnected():
                    break
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=SSE_HEARTBEAT_SEG
                )
                if msg is None:
                    # Heartbeat para mantener viva la conexión.
                    yield f": heartbeat {detector.metrics()['conflicts_active']}\n\n"
                    continue
                yield f"data: {msg['data']}\n\n"
        finally:
            await pubsub.aclose()
            await client.aclose()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ── REST: auditoría + métricas ──────────────────────────────────────────

@app.get("/api/conflictos")
def get_conflictos(
    minutes: int = Query(15, ge=1, le=1440),
    estado: str | None = Query(None, pattern="^(activo|resuelto)$"),
):
    """Historial de alertas desde PostgreSQL (auditoría, no el camino crítico)."""
    sql = """
        SELECT uid, estacion_id, nombre_estacion, region, ciudad, gleis,
               tren_1_id, tren_2_id,
               tren_1_numero, tren_1_categoria, tren_2_numero, tren_2_categoria,
               nivel, solape_min, margen_min, minutos_diferencia, causa,
               retraso_min, sugerencia_gleiswechsel,
               gleis_alternativo, estado, primera_deteccion, actualizado_en,
               detecciones
        FROM conflictos
        WHERE primera_deteccion >= NOW() - %(window)s
    """
    params: dict = {"window": timedelta(minutes=minutes)}
    if estado:
        sql += " AND estado = %(estado)s"
        params["estado"] = estado
    sql += " ORDER BY actualizado_en DESC LIMIT 500"

    with get_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    conflictos = []
    for r in rows:
        c = {k: r[k] for k in r.keys()}
        c["primera_deteccion"] = r["primera_deteccion"].isoformat()
        c["actualizado_en"] = r["actualizado_en"].isoformat()
        conflictos.append(c)

    return {"total": len(conflictos), "conflictos": conflictos}


@app.get("/api/estado")
def get_estado():
    """Estado en vivo del stream desde la memoria del detector (sin DB)."""
    return {
        "conflictos_activos": len(detector.active),
        "conflictos": detector.snapshot(),
        "metricas": detector.metrics(),
    }


@app.get("/api/estaciones")
def get_estaciones():
    """Catálogo StaDa completo: regiones (Bundesland) y sus ciudades.

    Alimenta los filtros del panel, de modo que aparezcan TODAS las ciudades
    de Alemania aunque ahora mismo no tengan ninguna alerta activa.
    """
    por_region: dict[str, set[str]] = {}
    estaciones_por_ciudad: dict[str, int] = {}
    estaciones_por_region: dict[str, int] = {}
    for st in producer.stations:
        region = st.region or "Sin región"
        por_region.setdefault(region, set())
        estaciones_por_region[region] = estaciones_por_region.get(region, 0) + 1
        if st.ciudad:
            por_region[region].add(st.ciudad)
            estaciones_por_ciudad[st.ciudad] = estaciones_por_ciudad.get(st.ciudad, 0) + 1

    return {
        "total_estaciones": len(producer.stations),
        "regiones": sorted(por_region.keys(), key=str.lower),
        "ciudades": {
            r: sorted(ciudades, key=str.lower) for r, ciudades in por_region.items()
        },
        "estaciones_por_ciudad": estaciones_por_ciudad,
        "estaciones_por_region": estaciones_por_region,
    }


@app.get("/api/metrics")
def get_metrics():
    """Métricas del pipeline Event-Driven (productor + detector)."""
    return {
        "producer": producer.metrics(),
        "detector": detector.metrics(),
    }


@app.get("/api/health")
def health():
    import concurrent.futures

    checks = {"status": "ok", "db": "unknown", "redis": "unknown"}
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
        checks["db"] = "up"
    except Exception as exc:  # pragma: no cover
        checks["db"] = str(exc)
        checks["status"] = "degraded"

    def _ping_redis() -> str:
        import redis as sync_redis
        client = sync_redis.from_url(REDIS_URL, decode_responses=True)
        try:
            client.ping()
            return "up"
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        try:
            checks["redis"] = pool.submit(_ping_redis).result(timeout=3)
        except Exception as exc:
            checks["redis"] = str(exc)
            checks["status"] = "degraded"

    # Fuente de datos real: credenciales de la DB API Marketplace.
    checks["modo"] = "datos-reales"
    checks["api_db"] = "ok" if producer.metrics()["credenciales"] else "sin-credenciales"
    if checks["api_db"] != "ok":
        checks["status"] = "degraded"

    code = 200 if checks["status"] == "ok" else 503
    return JSONResponse(status_code=code, content=checks)


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
