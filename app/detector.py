"""Detección de conflictos de andén como Stream Processor en memoria.

Rediseño Event-Driven del antiguo ``detector.py`` (que consultaba PostgreSQL
cada 30 s). Ahora:

- **Consume** los canales Redis ``events:ocupacion``, ``events:retraso``
  y ``events:cancelacion`` (Pub/Sub) — no hay polling a la base de datos.
- **Sliding Window de 15 min** por (estación, Gleis) mantenida en memoria,
  indexada por **event time** real: soporta eventos desordenados y tardíos
  (*late events*) con una tolerancia configurable (``LATE_TOLERANCE_SEG``).
- **Efecto dominó (Verspätung)**: un ``retraso`` desplaza la ocupación
  efectiva del tren (sube o baja según el dato real de la DB API); si con
  las horas programadas no había conflicto pero con las horas efectivas sí
  lo hay, la alerta se marca con ``causa="efecto_dominó"``.
- **Cancelación (Ausfall)**: un tren cancelado libera su ocupación de
  andén y sus conflictos se resuelven de inmediato.
- **Gleiswechsel**: si un conflicto persiste más de
  ``GLEISWECHEL_UMBRAL_SEG`` segundos, se marca con
  ``sugerencia_gleiswechsel=True`` y se calcula un ``gleis_alternativo``
  libre en la misma estación.
- **Emite** las alertas al canal ``events:conflictos`` (las consume el
  endpoint SSE ``/api/stream/conflictos`` del frontend).
- **Persistencia como auditoría desacoplada**: escribe en PostgreSQL vía
  ``asyncio.to_thread`` — nunca en el camino crítico del stream.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from app.bus import create_bus
from app.db import upsert_conflicto_auditoria
from app.events import (
    CH_CANCELACION,
    CH_CONFLICTOS,
    CH_OCUPACION,
    CH_RETRASO,
    loads,
    parse_event_time,
    utcnow,
)

logger = logging.getLogger(__name__)

# ── Configuración de la ventana deslizante ──────────────────────────────
VENTANA_SEG = int(os.getenv("VENTANA_SEG", "900"))          # 15 min
LATE_TOLERANCE_SEG = int(os.getenv("LATE_TOLERANCE_SEG", "120"))
UMBRAL_SEGUNDOS = int(os.getenv("UMBRAL_SEGUNDOS", "180"))  # 3 min
GLEISWECHEL_UMBRAL_SEG = int(os.getenv("GLEISWECHEL_UMBRAL_SEG", "45"))
SWEEP_INTERVAL = float(os.getenv("SWEEP_INTERVAL", "5"))
# Expiración lógica: una ocupación se retira al pasar su salida efectiva
# más este margen (o por seguridad al superar el tiempo de evento máximo).
OCUPACION_TTL_SEG = int(os.getenv("OCUPACION_TTL_SEG", "600"))
EVENTO_TTL_SEG = int(os.getenv("EVENTO_TTL_SEG", "7200"))
# Horizonte de predicción: solo se alerta de conflictos cuya ocupación está
# a punto de empezar (<= 30 min). Evita acumular miles de "programados"
# lejanos cuando se ingiere el plan completo de la hora en toda Alemania.
HORIZONTE_SEG = int(os.getenv("HORIZONTE_SEG", "1800"))
# Tolerancia para considerar "material" un cambio de minutos_diferencia
DIF_MATERIAL = 0.1


@dataclass(order=True)
class Ocupacion:
    """Ocupación de andén con horas programadas + Verspätung aplicada.

    El orden es por ``event_time`` (tiempo de evento) para mantener la
    ventana deslizante ordenada aunque lleguen eventos fuera de orden.
    """

    event_time: datetime
    estacion_id: str = field(compare=False)
    nombre_estacion: str = field(compare=False)
    gleis: str = field(compare=False)
    tren_id: str = field(compare=False)
    hora_llegada: datetime = field(compare=False)
    hora_salida: datetime = field(compare=False)
    region: str = field(compare=False, default="")
    ciudad: str = field(compare=False, default="")
    tren_categoria: str = field(compare=False, default="")
    tren_numero: str = field(compare=False, default="")
    retraso_min: float = field(compare=False, default=0.0)
    estimada: bool = field(compare=False, default=False)

    @property
    def llegada_eff(self) -> datetime:
        return self.hora_llegada + timedelta(minutes=self.retraso_min)

    @property
    def salida_eff(self) -> datetime:
        return self.hora_salida + timedelta(minutes=self.retraso_min)


def _pair_diff_seconds(a: Ocupacion, b: Ocupacion, effective: bool = True) -> float:
    """Diferencia mínima (s) entre salida de uno y llegada del otro."""
    sa = a.salida_eff if effective else a.hora_salida
    lb = b.llegada_eff if effective else b.hora_llegada
    sb = b.salida_eff if effective else b.hora_salida
    la = a.llegada_eff if effective else a.hora_llegada
    d1 = (sa - lb).total_seconds()
    d2 = (sb - la).total_seconds()
    return d1 if abs(d1) <= abs(d2) else d2


class StreamDetector:
    """Procesador de stream con ventanas deslizantes en memoria."""

    def __init__(self) -> None:
        # (estacion_id, gleis) -> {tren_id: Ocupacion}  (ventana deslizante)
        self.windows: dict[tuple[str, str], dict[str, Ocupacion]] = {}
        # (estacion_id, tren_id) -> (estacion_id, gleis) para localizar retardos
        self.train_index: dict[tuple[str, str], tuple[str, str]] = {}
        # uid -> conflicto activo
        self.active: dict[str, dict] = {}

        self._redis = None  # bus de eventos (Redis o local)
        self._sweep_task: asyncio.Task | None = None

        # Métricas de stream
        self.events_in = 0
        self.late_events = 0
        self.out_of_order = 0
        self.updates = 0
        self.dropped_old = 0
        self.delays_applied = 0
        self.cancelaciones = 0
        self.conflicts_detected = 0
        self.detected_conflicto = 0
        self.detected_riesgo = 0
        self.conflicts_resolved = 0
        self.groups = 0
        self.pruned_por_salida = 0
        self.pruned_por_evento = 0

    # ── Ciclo de vida ───────────────────────────────────────────────────

    async def start(self) -> None:
        self._redis = create_bus()
        self._sweep_task = asyncio.create_task(self._sweep_loop())
        logger.info(
            "StreamDetector iniciado: ventana=%ss, lateness=%ss, umbral=%ss, "
            "gleiswechsel tras=%ss",
            VENTANA_SEG, LATE_TOLERANCE_SEG, UMBRAL_SEGUNDOS,
            GLEISWECHEL_UMBRAL_SEG,
        )
        await self._consume()

    async def stop(self) -> None:
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def _consume(self) -> None:
        """Loop de consumo del stream (Pub/Sub). Nunca se detiene."""
        assert self._redis is not None
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(CH_OCUPACION, CH_RETRASO, CH_CANCELACION)
        logger.info(
            "Suscrito a %s, %s y %s", CH_OCUPACION, CH_RETRASO, CH_CANCELACION
        )
        try:
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    event = loads(msg["data"])
                    channel = msg["channel"]
                    if channel == CH_RETRASO:
                        await self._on_retraso(event)
                    elif channel == CH_CANCELACION:
                        await self._on_cancelacion(event)
                    else:
                        await self._on_ocupacion(event)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Error procesando evento del stream")
        finally:
            await pubsub.aclose()

    # ── Ingesta de eventos en la ventana ────────────────────────────────

    async def _on_ocupacion(self, ev: dict) -> None:
        self.events_in += 1
        event_time = parse_event_time(ev["event_time"])
        now = utcnow()

        # Late / out-of-order según el tiempo de evento real.
        if event_time < now - timedelta(seconds=VENTANA_SEG + LATE_TOLERANCE_SEG):
            self.dropped_old += 1
            return
        if event_time < now - timedelta(seconds=LATE_TOLERANCE_SEG):
            self.late_events += 1

        ocu = Ocupacion(
            event_time=event_time,
            estacion_id=ev["estacion_id"],
            nombre_estacion=ev.get("nombre_estacion", ev["estacion_id"]),
            region=ev.get("region", ""),
            ciudad=ev.get("ciudad", ""),
            tren_categoria=ev.get("tren_categoria", ""),
            tren_numero=ev.get("tren_numero", ""),
            gleis=str(ev["gleis"]),
            tren_id=ev["tren_id"],
            hora_llegada=parse_event_time(ev["hora_llegada"]),
            hora_salida=parse_event_time(ev["hora_salida"]),
            retraso_min=float(ev.get("retraso_min", 0.0)),
            estimada=bool(ev.get("estimada", False)),
        )

        # El tren puede haberse re-programado a otro Gleis: limpiar la
        # posición anterior del índice para no duplicarlo.
        old_key = self.train_index.get((ocu.estacion_id, ocu.tren_id))
        new_key = (ocu.estacion_id, ocu.gleis)
        if old_key is not None and old_key != new_key:
            self.windows.get(old_key, {}).pop(ocu.tren_id, None)

        group = self.windows.setdefault(new_key, {})
        existing = group.get(ocu.tren_id)
        if existing is not None:
            # Upsert por event_time: un evento más viejo no pisa uno más nuevo
            # (manejo de out-of-order).
            if event_time < existing.event_time:
                self.out_of_order += 1
                return
            if event_time > existing.event_time:
                self.updates += 1
            # Conservar el Verspätung ya aplicado si el nuevo evento no lo trae.
            if ocu.retraso_min == 0.0 and existing.retraso_min > 0.0:
                ocu.retraso_min = existing.retraso_min
        else:
            if group and event_time < max(o.event_time for o in group.values()):
                self.out_of_order += 1

        # Insertar manteniendo la ventana ordenada por event_time (bisect).
        group[ocu.tren_id] = ocu
        self.train_index[(ocu.estacion_id, ocu.tren_id)] = new_key

        await self._evaluate_group(*new_key)

    async def _on_retraso(self, ev: dict) -> None:
        """Verspätung: desplaza la ocupación efectiva del tren (efecto dominó)."""
        self.events_in += 1
        key = self.train_index.get((ev["estacion_id"], ev["tren_id"]))
        if key is None:
            return
        group = self.windows.get(key)
        ocu = group.get(ev["tren_id"]) if group else None
        if ocu is None:
            return

        retraso = float(ev.get("retraso_min", 0.0))
        if retraso == ocu.retraso_min:
            return
        # Asignación bidireccional: el dato real puede subir o bajar
        # (tren recuperado de la Verspätung → retraso 0).
        ocu.retraso_min = retraso
        self.delays_applied += 1
        logger.info(
            "RETRASO %s: %s → %.0f min → Gleis %s (re-evaluando)",
            ev.get("motivo", "Verspätung"), ev["tren_id"], retraso,
            ocu.gleis,
        )
        await self._evaluate_group(*key)

    async def _on_cancelacion(self, ev: dict) -> None:
        """Ausfall real: libera la ocupación y resuelve sus conflictos."""
        self.events_in += 1
        self.cancelaciones += 1
        estacion_id = ev["estacion_id"]
        tren_id = ev["tren_id"]
        key = self.train_index.pop((estacion_id, tren_id), None)
        if key is None:
            return
        group = self.windows.get(key)
        if group is not None and group.pop(tren_id, None) is not None:
            logger.info(
                "CANCELADO %s: ocupación liberada en %s Gleis %s",
                tren_id, estacion_id, key[1],
            )
            await self._evaluate_group(*key)

    # ── Evaluación de conflictos en la ventana ──────────────────────────

    async def _evaluate_group(self, estacion_id: str, gleis: str) -> None:
        """Recalcula los conflictos de un (estación, Gleis) y emite cambios."""
        key = (estacion_id, gleis)
        group = self.windows.get(key) or {}
        trains = list(group.values())

        now = utcnow()
        found: dict[str, dict] = {}

        for i in range(len(trains)):
            for j in range(i + 1, len(trains)):
                a, b = trains[i], trains[j]
                if a.tren_id == b.tren_id:
                    continue

                # Solape real de ocupaciones: ambos trenes en el andén a la vez.
                ini = max(a.llegada_eff, b.llegada_eff)
                fin = min(a.salida_eff, b.salida_eff)
                # Fuera del horizonte de predicción: todavía no accionable
                # (entrará en el panel cuando la ocupación se acerque).
                if ini > now + timedelta(seconds=HORIZONTE_SEG):
                    continue
                overlap_seg = (fin - ini).total_seconds()
                # Un solape sólo es "conflicto" si ambas ocupaciones tienen
                # llegada y salida reales; si alguna es inferida (cabecera:
                # el plan trae sólo llegada o sólo salida) no se puede
                # confirmar → se degrada a "riesgo".
                confirmado = not (a.estimada or b.estimada)
                if overlap_seg > 0 and confirmado:
                    nivel = "conflicto"
                else:
                    # Sin solape confirmado: sólo relevante si el headway es
                    # corto (o si el solape es estimado, con margen 0).
                    gap_seg = -overlap_seg
                    if gap_seg >= UMBRAL_SEGUNDOS:
                        continue
                    nivel = "riesgo"
                diff = overlap_seg if overlap_seg > 0 else -(gap_seg)

                t1, t2 = sorted([a.tren_id, b.tren_id])
                uid = f"{estacion_id}:{gleis}:{t1}:{t2}"
                # Número de tren (Zugnummer) para identificar cada servicio.
                por_id = {a.tren_id: a, b.tren_id: b}
                o1, o2 = por_id[t1], por_id[t2]

                # ¿El problema existe solo por el retraso? → efecto dominó.
                base_diff = _pair_diff_seconds(a, b, effective=False)
                retraso_max = max(a.retraso_min, b.retraso_min)
                if abs(base_diff) >= UMBRAL_SEGUNDOS and retraso_max > 0:
                    causa = "efecto_dominó"
                elif retraso_max > 0:
                    causa = "retraso_contribuyente"
                else:
                    causa = "solapamiento_programado"

                minutos = round(abs(diff) / 60.0, 2)
                previo = self.active.get(uid)

                conflicto = {
                    "uid": uid,
                    "estacion_id": estacion_id,
                    "nombre_estacion": a.nombre_estacion,
                    "region": a.region,
                    "ciudad": a.ciudad,
                    "gleis": gleis,
                    "tren_1_id": t1,
                    "tren_2_id": t2,
                    "tren_1_numero": o1.tren_numero,
                    "tren_1_categoria": o1.tren_categoria,
                    "tren_2_numero": o2.tren_numero,
                    "tren_2_categoria": o2.tren_categoria,
                    "nivel": nivel,
                    "solape_min": round(max(overlap_seg, 0.0) / 60.0, 2),
                    "margen_min": round(max(-overlap_seg, 0.0) / 60.0, 2),
                    "minutos_diferencia": minutos,
                    "causa": causa,
                    "retraso_min": round(retraso_max, 2),
                    "estado": "activo",
                    "sugerencia_gleiswechsel": False,
                    "gleis_alternativo": None,
                    "timestamp_alerta": (
                        previo["timestamp_alerta"] if previo else now.isoformat()
                    ),
                    "actualizado_en": now.isoformat(),
                }

                if previo is None:
                    conflicto["primera_deteccion"] = now.isoformat()
                    conflicto["detecciones"] = 1
                    found[uid] = conflicto
                    self.active[uid] = conflicto
                    self.conflicts_detected += 1
                    if nivel == "conflicto":
                        self.detected_conflicto += 1
                    else:
                        self.detected_riesgo += 1
                    logger.warning(
                        "ALERTA [%s/%s] %s Gleis %s: %s vs %s (%.2f min)",
                        nivel, causa, estacion_id, gleis, t1, t2, minutos,
                    )
                    await self._emit("nuevo", conflicto)
                    self._persist(conflicto, "nuevo")
                else:
                    minutos_previo = previo["minutos_diferencia"]
                    material = abs(minutos - minutos_previo) >= DIF_MATERIAL
                    previo["nivel"] = nivel
                    previo["solape_min"] = round(max(overlap_seg, 0.0) / 60.0, 2)
                    previo["margen_min"] = round(max(-overlap_seg, 0.0) / 60.0, 2)
                    previo["minutos_diferencia"] = minutos
                    previo["causa"] = causa
                    previo["retraso_min"] = round(retraso_max, 2)
                    previo["tren_1_numero"] = o1.tren_numero
                    previo["tren_1_categoria"] = o1.tren_categoria
                    previo["tren_2_numero"] = o2.tren_numero
                    previo["tren_2_categoria"] = o2.tren_categoria
                    previo["actualizado_en"] = now.isoformat()
                    previo["detecciones"] = previo.get("detecciones", 1) + 1

                    # Gleiswechsel: el conflicto persiste demasiado tiempo.
                    gwNuevo = False
                    if not previo.get("sugerencia_gleiswechsel"):
                        primera = parse_event_time(previo["primera_deteccion"])
                        if (now - primera).total_seconds() >= GLEISWECHEL_UMBRAL_SEG:
                            alt = self._gleis_alternativo(estacion_id, gleis)
                            previo["sugerencia_gleiswechsel"] = True
                            previo["gleis_alternativo"] = alt
                            gwNuevo = True
                            logger.warning(
                                "GLEISWECHEL %s Gleis %s → sugerido Gleis %s "
                                "(conflicto persiste ≥%ss)",
                                estacion_id, gleis, alt, GLEISWECHEL_UMBRAL_SEG,
                            )

                    if gwNuevo or material:
                        await self._emit("actualizado", previo)
                        self._persist(previo, "actualizado")
                    found[uid] = previo

        # Resolver los conflictos de este grupo que ya no existen.
        for uid in [
            u for u, c in self.active.items()
            if c["estacion_id"] == estacion_id and c["gleis"] == gleis
            and u not in found
        ]:
            conflicto = self.active.pop(uid)
            conflicto["actualizado_en"] = now.isoformat()
            self.conflicts_resolved += 1
            logger.info("RESUELTO %s", uid)
            await self._emit("resuelto", conflicto)
            self._persist(conflicto, "resuelto")

    def _gleis_alternativo(self, estacion_id: str, gleis_actual: str) -> str | None:
        """Propone un Gleis de la misma estación sin conflictos activos."""
        conflicted = {
            c["gleis"] for c in self.active.values()
            if c["estacion_id"] == estacion_id
        }
        candidatos = [
            (est, g) for (est, g) in self.windows
            if est == estacion_id and g != gleis_actual and g not in conflicted
        ]
        if not candidatos:
            return None
        # Elige el Gleis con menos ocupación en la ventana (menos saturado).
        _, best = min(candidatos, key=lambda k: len(self.windows.get(k, {})))
        return best

    # ── Sweep periódico: prune de la ventana + resolución de estancados ─

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(SWEEP_INTERVAL)
                await self._sweep()
        except asyncio.CancelledError:
            raise

    async def _sweep(self) -> None:
        now = utcnow()
        cutoff_evento = now - timedelta(seconds=EVENTO_TTL_SEG)

        # 1. Expiración lógica: la ocupación termina con su salida efectiva
        #    (+ margen) y, por seguridad, nunca vive más de EVENTO_TTL_SEG.
        popped: list[tuple[str, str]] = []
        for key, group in list(self.windows.items()):
            for tid, ocu in list(group.items()):
                expira_salida = (
                    ocu.salida_eff + timedelta(seconds=OCUPACION_TTL_SEG) < now
                )
                expira_evento = ocu.event_time < cutoff_evento
                if expira_salida or expira_evento:
                    group.pop(tid, None)
                    self.train_index.pop((key[0], tid), None)
                    if expira_salida:
                        self.pruned_por_salida += 1
                    else:
                        self.pruned_por_evento += 1
            if not group:
                self.windows.pop(key, None)
                popped.append(key)

        # 2. Re-evaluar todos los grupos (incl. los vacíados: resuelve sus
        #    conflictos) y marcar Gleiswechsel en los que siguen activos.
        self.groups = len(self.windows)
        for estacion_id, gleis in list(self.windows.keys()) + popped:
            await self._evaluate_group(estacion_id, gleis)

    # ── Emisión (SSE) y auditoría (PostgreSQL) ──────────────────────────

    async def _emit(self, action: str, conflicto: dict) -> None:
        """Publica la alerta para los suscriptores SSE."""
        if self._redis is None:
            return
        payload = json.dumps(
            {"action": action, "conflicto": conflicto}, ensure_ascii=False
        )
        try:
            await self._redis.publish(CH_CONFLICTOS, payload)
        except Exception:
            logger.exception("No se pudo publicar la alerta en el bus")

    def _persist(self, conflicto: dict, action: str) -> None:
        """Sink de auditoría desacoplado (no bloquea el stream)."""
        try:
            asyncio.get_running_loop().create_task(
                asyncio.to_thread(upsert_conflicto_auditoria, conflicto, action)
            )
        except RuntimeError:
            # Sin loop activo (uso CLI): escritura síncrona.
            upsert_conflicto_auditoria(conflicto, action)

    # ── API para el frontend / endpoints ────────────────────────────────

    def snapshot(self) -> list[dict]:
        """Conflictos activos en memoria (estado del stream)."""
        return sorted(
            self.active.values(),
            key=lambda c: c["timestamp_alerta"],
            reverse=True,
        )

    def metrics(self) -> dict:
        return {
            "events_in": self.events_in,
            "late_events": self.late_events,
            "out_of_order": self.out_of_order,
            "updates": self.updates,
            "dropped_old": self.dropped_old,
            "delays_applied": self.delays_applied,
            "cancelaciones": self.cancelaciones,
            "conflicts_detected": self.conflicts_detected,
            "detected_conflicto": self.detected_conflicto,
            "detected_riesgo": self.detected_riesgo,
            "conflicts_resolved": self.conflicts_resolved,
            "conflicts_active": len(self.active),
            "conflicts_activos_conflicto": sum(
                1 for c in self.active.values()
                if c.get("nivel") == "conflicto"
            ),
            "conflicts_activos_riesgo": sum(
                1 for c in self.active.values() if c.get("nivel") == "riesgo"
            ),
            "window_groups": len(self.windows),
            "window_occupations": sum(len(g) for g in self.windows.values()),
            "ocupaciones_por_salida": self.pruned_por_salida,
            "ocupaciones_por_evento": self.pruned_por_evento,
            "window_seg": VENTANA_SEG,
            "lateness_seg": LATE_TOLERANCE_SEG,
            "umbral_seg": UMBRAL_SEGUNDOS,
            "horizonte_seg": HORIZONTE_SEG,
        }


# Instancia global compartida con main.py
detector = StreamDetector()


async def run_detector() -> None:
    """Entry point para la task de detección en el lifespan de FastAPI."""
    await detector.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("StreamDetector: ejecutar dentro de la app FastAPI (lifespan).")
