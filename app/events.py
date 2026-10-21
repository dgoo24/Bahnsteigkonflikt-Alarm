"""Esquema de eventos y claves del bus de mensajes (Redis Pub/Sub).

Este módulo define el contrato de eventos que circulan por el pipeline
Event-Driven:

- ``events:ocupacion``   → eventos de ocupación de andén (Gleis)
- ``events:retraso``     → eventos de retraso de tren (Verspätung real)
- ``events:cancelacion`` → cancelaciones de tren (Ausfall real)
- ``events:conflictos``  → alertas de conflicto emitidas por el detector
                           (las consume el endpoint SSE del frontend)

Todos los eventos llevan ``event_time`` (tiempo de evento real, event time)
para poder reordenar eventos fuera de orden y calcular ventanas deslizantes
basadas en el tiempo de evento y no en el tiempo de procesamiento.

Los eventos los genera el productor a partir de la **DB Timetables API**
(datos reales: Sollfahrplan + cambios en vivo).
"""

import json
import os
import uuid
from datetime import datetime, timezone

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

CH_OCUPACION = "events:ocupacion"
CH_RETRASO = "events:retraso"
CH_CANCELACION = "events:cancelacion"
CH_CONFLICTOS = "events:conflictos"

TIPO_OCUPACION = "ocupacion"
TIPO_RETRASO = "retraso"
TIPO_CANCELACION = "cancelacion"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    return utcnow().isoformat()


def new_event_id() -> str:
    return uuid.uuid4().hex


def parse_event_time(value: str) -> datetime:
    """Parsea ISO-8601 y garantiza timezone-aware UTC."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ocupacion_event(
    *,
    estacion_id: str,
    nombre_estacion: str,
    gleis: str,
    tren_id: str,
    hora_llegada: datetime,
    hora_salida: datetime,
    retraso_min: float = 0.0,
    event_time: datetime | None = None,
    fuente: str = "db-timetables",
    estimada: bool = False,
    region: str = "",
    ciudad: str = "",
    tren_categoria: str = "",
    tren_numero: str = "",
) -> dict:
    """Evento de ocupación de andén.

    ``estimada=True`` cuando el plan solo trae llegada o solo salida y el
    intervalo se ha inferido (margen sintético): es una ocupación no
    confirmada y no debe elevar una alerta a "conflicto".

    ``tren_categoria``/``tren_numero`` son el número de tren real (Zugnummer,
    p. ej. ``ICE 599``) — NO la línea/ruta (``<tl n>``, no ``<ar l>``).
    """
    et = event_time or utcnow()
    return {
        "event_id": new_event_id(),
        "tipo": TIPO_OCUPACION,
        "event_time": et.isoformat(),
        "estacion_id": estacion_id,
        "nombre_estacion": nombre_estacion,
        "region": region,
        "ciudad": ciudad,
        "gleis": str(gleis),
        "tren_id": tren_id,
        "tren_categoria": tren_categoria,
        "tren_numero": tren_numero,
        "hora_llegada": hora_llegada.isoformat(),
        "hora_salida": hora_salida.isoformat(),
        "retraso_min": round(float(retraso_min), 2),
        "fuente": fuente,
        "estimada": bool(estimada),
    }


def retraso_event(
    *,
    estacion_id: str,
    tren_id: str,
    retraso_min: float,
    motivo: str = "Verspätung",
    event_time: datetime | None = None,
) -> dict:
    """Evento de retraso de tren (Verspätung) que desplaza su ocupación."""
    et = event_time or utcnow()
    return {
        "event_id": new_event_id(),
        "tipo": TIPO_RETRASO,
        "event_time": et.isoformat(),
        "estacion_id": estacion_id,
        "tren_id": tren_id,
        "retraso_min": round(float(retraso_min), 2),
        "motivo": motivo,
    }


def cancelacion_event(
    *,
    estacion_id: str,
    tren_id: str,
    motivo: str = "Ausfall",
    event_time: datetime | None = None,
) -> dict:
    """Cancelación real de un tren (Ausfall): libera su ocupación de andén."""
    et = event_time or utcnow()
    return {
        "event_id": new_event_id(),
        "tipo": TIPO_CANCELACION,
        "event_time": et.isoformat(),
        "estacion_id": estacion_id,
        "tren_id": tren_id,
        "motivo": motivo,
    }


def dumps(event: dict) -> str:
    return json.dumps(event, ensure_ascii=False)


def loads(payload) -> dict:
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")
    return json.loads(payload)
