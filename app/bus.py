"""Bus de eventos: Redis Pub/Sub (producción) o bus local en memoria.

En el despliegue con Docker el bus es **Redis** (proceso separado). En
Streamlit Cloud no hay Redis ni Docker, así que el pipeline corre dentro del
mismo proceso del dashboard y el bus es **local** (en memoria), con la misma
API que usa el productor y el detector:

- ``bus.pipeline(transaction=False).publish(canal, payload)`` + ``execute()``
- ``bus.pubsub().subscribe(canales)`` + ``async for msg in listen()``
- ``bus.publish(canal, payload)``

El bus local es un **singleton** (igual que el servidor Redis lo es para
todos los clientes del despliegue): productor y detector deben compartirlo.
"""

from __future__ import annotations

import asyncio
import os
from typing import AsyncIterator

from app.events import REDIS_URL

try:  # Redis es opcional (Streamlit Cloud no lo tiene)
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover - depende del entorno
    aioredis = None

BUS_LOCAL = os.getenv("BUS_LOCAL", "0").strip().lower() in ("1", "true", "yes")


class LocalPipeline:
    """Equivalente de ``redis.asyncio.client.Pipeline`` para el bus local."""

    def __init__(self, bus: "LocalBus") -> None:
        self._bus = bus
        self._msgs: list[tuple[str, str]] = []

    def publish(self, channel: str, payload: str) -> None:
        self._msgs.append((channel, payload))

    async def execute(self) -> None:
        for channel, payload in self._msgs:
            self._bus.publish_nowait(channel, payload)
        self._msgs.clear()


class LocalPubSub:
    """Equivalente de ``redis.asyncio.client.PubSub`` para el bus local."""

    def __init__(self, bus: "LocalBus") -> None:
        self._bus = bus
        self._channels: set[str] = set()
        self._queue: asyncio.Queue = asyncio.Queue()

    async def subscribe(self, *channels: str) -> None:
        for ch in channels:
            self._channels.add(ch)
            self._bus._subscribe(self, ch)

    async def listen(self) -> AsyncIterator[dict]:
        while True:
            yield await self._queue.get()

    async def aclose(self) -> None:
        self._bus._unsubscribe(self)
        self._channels.clear()

    def _push(self, channel: str, payload: str) -> None:
        self._queue.put_nowait(
            {"type": "message", "channel": channel, "data": payload}
        )


class LocalBus:
    """Bus de eventos en memoria (un único proceso)."""

    _instance: "LocalBus | None" = None

    def __init__(self) -> None:
        self._subs: dict[str, list[LocalPubSub]] = {}

    @classmethod
    def instance(cls) -> "LocalBus":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ── API "redis-like" ────────────────────────────────────────────────
    def pipeline(self, transaction: bool = True) -> LocalPipeline:
        return LocalPipeline(self)

    def pubsub(self) -> LocalPubSub:
        return LocalPubSub(self)

    def publish_nowait(self, channel: str, payload: str) -> None:
        for ps in list(self._subs.get(channel, [])):
            ps._push(channel, payload)

    async def publish(self, channel: str, payload: str) -> None:
        self.publish_nowait(channel, payload)

    async def aclose(self) -> None:
        # El bus vive mientras viva el proceso: no se cierran suscripciones
        # compartidas (productor y detector usan la misma instancia).
        return None

    # ── Interno ─────────────────────────────────────────────────────────
    def _subscribe(self, ps: LocalPubSub, channel: str) -> None:
        self._subs.setdefault(channel, []).append(ps)

    def _unsubscribe(self, ps: LocalPubSub) -> None:
        for subs in self._subs.values():
            if ps in subs:
                subs.remove(ps)


def create_bus():
    """Devuelve el bus de eventos: Redis si está disponible; si no, local."""
    if BUS_LOCAL or aioredis is None:
        return LocalBus.instance()
    return aioredis.from_url(REDIS_URL, decode_responses=True)
