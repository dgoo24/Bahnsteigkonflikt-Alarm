"""Ingesta Event-Driven con datos REALES de la Deutsche Bahn.

Fuentes (DB API Marketplace, `developers.deutschebahn.com`, licencia CC BY 4.0):

- **Timetables API** (plan Free, 60 req/min):
    - ``GET /plan/{eva}/{fecha}/{hora}`` → Sollfahrplan (plan por hora) con
      **Gleis real** (atributo ``pp``).
    - ``GET /fchg/{eva}`` → cambios vigentes: **Verspätung real** (``ct`` vs
      ``pt``), **cancelaciones** (``cs``/``ps`` = "c") y **cambios de Gleis**
      (``cp``).
- **StaDa API** (plan Free4All): ``GET /stations`` → catálogo de todas las
  estaciones de Alemania (1 llamada/día por condiciones de uso).

No existe simulación: todo el tráfico sale de datos reales en vivo.

Gestión de rate limit (60 req/min en plan Free):

- Token bucket global (``DB_RATE_PER_MIN``).
- Rotación con prioridad por *score*: estaciones con actividad reciente y
  categorías altas (Hbf) se atienden primero; el resto rota en segundo plano.
- El productor publica eventos en Redis Pub/Sub (``events:ocupacion``,
  ``events:retraso``, ``events:cancelacion``); la persistencia en PostgreSQL
  la sigue haciendo el detector como sink de auditoría.
"""

import asyncio
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from app.bus import create_bus
from app.events import (
    CH_CANCELACION,
    CH_OCUPACION,
    CH_RETRASO,
    cancelacion_event,
    ocupacion_event,
    retraso_event,
    utcnow,
)

logger = logging.getLogger(__name__)

TIMETABLES_BASE = (
    "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1"
)
STADA_BASE = "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2"
DB_TZ = ZoneInfo("Europe/Berlin")

# Credenciales (DB API Marketplace → aplicación → suscripción a las APIs).
DB_CLIENT_ID = os.getenv("DB_CLIENT_ID", "").strip()
DB_API_KEY = os.getenv("DB_API_KEY", "").strip()

# Rate limit: plan Free = 60 req/min; usamos 55 por seguridad.
DB_RATE_PER_MIN = float(os.getenv("DB_RATE_PER_MIN", "55"))
# TTLs de rotación: activas (con actividad reciente) vs. fondo.
FCHG_TTL_ACTIVA = float(os.getenv("FCHG_TTL_ACTIVA", "60"))
FCHG_TTL_FONDO = float(os.getenv("FCHG_TTL_FONDO", "900"))
PLAN_TTL_SEG = float(os.getenv("PLAN_TTL_SEG", "3600"))
CATALOG_TTL_SEG = float(os.getenv("CATALOG_TTL_SEG", "86400"))
STOP_STATE_TTL_SEG = float(os.getenv("STOP_STATE_TTL_SEG", "7200"))
# 0 = todas las estaciones del catálogo; >0 = límite para pruebas.
STADA_MAX_ESTACIONES = int(os.getenv("STADA_MAX_ESTACIONES", "0"))

# Fallback temporal si StaDa no está suscrito / no responde: EVAs REALES de
# los principales nudos. No es simulación (los horarios y Gleise siguen
# viniendo de Timetables); se sustituye por el catálogo completo en cuanto
# StaDa vuelve a estar disponible (reintento cada 5 min).
SEED_EVAS = [
    ("8000105", "Frankfurt(Main) Hbf"),
    ("8002549", "Hamburg Hbf"),
    ("8011160", "Berlin Hbf"),
    ("8000261", "München Hbf"),
    ("8000207", "Köln Hbf"),
    ("8000096", "Stuttgart Hbf"),
]

# Ocupación estimada cuando el plan sólo trae llegada o sólo salida.
MARGEN_ANTES_SALIDA = timedelta(minutes=3)
MARGEN_DESPUES_LLEGADA = timedelta(minutes=4)
# Retraso mínimo (min) para emitir un evento Verspätung.
RETRASO_MINIMO_MIN = 1
# Mismo margen de caducidad que el detector (no publicar ocupaciones ya
# terminadas: solo genera churn de eventos y conflictos transitorios).
TTL_SALIDA_SEG = int(os.getenv("OCUPACION_TTL_SEG", "600"))

# Bonus de prioridad por categoría StaDa (1 = gran Hbf … 6 = parada pequeña).
CAT_BONUS = {1: 0.25, 2: 0.15, 3: 0.08, 4: 0.03}
# Score mínimo para considerar un job vencido (age/ttl >= 1).
SCORE_VENCIDO = 1.0


class DbAuthError(RuntimeError):
    """Credenciales inválidas o API no suscrita (401/403)."""


class DbRateLimitError(RuntimeError):
    """Rate limit de la DB API alcanzado (429)."""


# ── Cliente HTTP de la DB API ────────────────────────────────────────────


class DbApi:
    """Cliente mínimo para Timetables (XML) y StaDa (JSON)."""

    def __init__(self, client_id: str, api_key: str, timeout: int = 15):
        self._session = requests.Session()
        self._session.headers.update(
            {
                "DB-Client-Id": client_id,
                "DB-Api-Key": api_key,
                "Accept": "application/xml, application/json",
            }
        )
        self._timeout = timeout
        self.bad_requests = 0

    @property
    def credenciales(self) -> bool:
        return bool(DB_CLIENT_ID and DB_API_KEY)

    def _get(self, url: str, params: dict | None = None) -> requests.Response:
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:
            raise RuntimeError(f"Error de red: {exc}") from exc
        if resp.status_code in (401, 403):
            raise DbAuthError(
                "DB API rechazó las credenciales (401/403): revisa DB_CLIENT_ID/"
                "DB_API_KEY y la suscripción a Timetables/StaDa"
            )
        if resp.status_code == 429:
            raise DbRateLimitError("Rate limit DB API (429)")
        if resp.status_code == 404:
            return resp  # sin plan para esa hora / recurso vacío
        if resp.status_code == 400:
            # Algunos EVAs del catálogo no tienen datos en Timetables.
            self.bad_requests += 1
            return resp
        if not resp.ok:
            raise RuntimeError(
                f"DB API {resp.status_code} en {url}: {resp.text[:200]}"
            )
        return resp

    def get_xml(self, path: str) -> ET.Element | None:
        """Devuelve el árbol XML o ``None`` si la API responde 400/404."""
        resp = self._get(TIMETABLES_BASE + path)
        if resp.status_code in (400, 404):
            return None
        try:
            return ET.fromstring(resp.content)
        except ET.ParseError as exc:
            raise RuntimeError(f"XML inválido en {path}: {exc}") from exc

    def get_stations_page(self, offset: int, limit: int = 10000) -> dict:
        resp = self._get(
            f"{STADA_BASE}/stations", params={"offset": offset, "limit": limit}
        )
        return resp.json()


class RateLimiter:
    """Token bucket global: ``per_min`` peticiones por minuto."""

    def __init__(self, per_min: float):
        self._interval = 60.0 / max(per_min, 1.0)
        self._next = 0.0

    async def acquire(self) -> None:
        now = time.monotonic()
        wait = self._next - now
        if wait > 0:
            await asyncio.sleep(wait)
            now = time.monotonic()
        self._next = max(self._next, now) + self._interval

    def penalizar(self, segundos: float) -> None:
        """Bloquea el bucket tras recibir un 429."""
        self._next = max(self._next, time.monotonic() + segundos)


# ── Parseo del XML IRIS (plan / fchg) ────────────────────────────────────


def _parse_db_time(value: str | None) -> datetime | None:
    """YYMMDDHHMM (hora local Europa/Berlin) → datetime aware en UTC."""
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%y%m%d%H%M").replace(tzinfo=DB_TZ)
    except ValueError:
        return None
    return dt.astimezone(timezone.utc)


def _delay_minutes(el: ET.Element | None) -> int | None:
    """Minutos de retraso (ct vs pt) de un ar/dp, o ``None`` si no cambia."""
    if el is None:
        return None
    pt = _parse_db_time(el.get("pt"))
    ct = _parse_db_time(el.get("ct"))
    if pt is None or ct is None:
        return None
    return int((ct - pt).total_seconds() // 60)


def _cancelado(el: ET.Element | None) -> bool:
    """True si el ar/dp está cancelado (``cs``/``ps`` = 'c')."""
    if el is None:
        return False
    return el.get("cs") == "c" or el.get("ps") == "c"


def _vigente(salida: datetime, delay_min: int = 0) -> bool:
    """True si la ocupación (incluido su Verspätung) aún no caducó.

    Criterio idéntico al prune del detector: salida efectiva + margen.
    """
    fin = salida + timedelta(minutes=delay_min) + timedelta(seconds=TTL_SALIDA_SEG)
    return fin > utcnow()


def _motivo(s: ET.Element, default: str) -> str:
    """Primer mensaje del stop (causa de retraso / incidencia)."""
    for m in s.findall("m"):
        if m.get("del") == "1":
            continue
        texto = m.get("ext") or m.get("int")
        if texto:
            return texto.strip()[:60] or default
    return default


@dataclass
class StopInfo:
    """Parada de tren parseada de un ``<s>`` (plan o fchg)."""

    sid: str
    ar: ET.Element | None
    dp: ET.Element | None
    ar_pt: datetime | None = None
    dp_pt: datetime | None = None
    ar_pp: str = ""
    dp_pp: str = ""
    ar_cp: str = ""
    dp_cp: str = ""
    delay_ar: int | None = None
    delay_dp: int | None = None
    cancelado: bool = False
    cat: str = ""
    num: str = ""

    def tren_id(self, eva: str) -> str:
        """ID legible del tren: categoría+número+hora local (único por parada).

        Ej.: ``8000207:ICE580-1741``. Si el plan no trae ``<tl>`` se usa el
        id IRIS completo de la parada.
        """
        if self.num:
            ref = self.dp_pt or self.ar_pt
            hhmm = ref.astimezone(DB_TZ).strftime("%H%M") if ref else ""
            if hhmm:
                return f"{eva}:{self.cat}{self.num}-{hhmm}"
        return f"{eva}:{self.sid}"

    @property
    def delay_min(self) -> int | None:
        """Máximo retraso entre llegada y salida (0 si ninguno cambia)."""
        cand = [d for d in (self.delay_ar, self.delay_dp) if d is not None]
        return max(cand) if cand else 0

    @property
    def gleis_plan(self) -> str:
        return self.dp_pp or self.ar_pp

    @property
    def gleis_cp(self) -> str:
        return self.dp_cp or self.ar_cp

    @property
    def estimada(self) -> bool:
        """True si falta llegada o salida y el intervalo es inferido.

        En cabeceras IRIS publica la llegada y la salida como dos ``<s>``
        distintos (mismo tren físico, distinto servicio); inferir el margen
        evita perder la ocupación, pero la marca impide que un solape
        derivado de ella se cuente como conflicto confirmado.
        """
        return self.ar_pt is None or self.dp_pt is None

    @property
    def horas_plan(self) -> tuple[datetime, datetime] | None:
        """(llegada, salida) planificadas con los márgenes de ocupación."""
        llegada = self.ar_pt or (self.dp_pt - MARGEN_ANTES_SALIDA if self.dp_pt else None)
        salida = self.dp_pt or (self.ar_pt + MARGEN_DESPUES_LLEGADA if self.ar_pt else None)
        if llegada is None or salida is None:
            return None
        return llegada, salida


def _parse_stop(s: ET.Element) -> StopInfo | None:
    sid = s.get("id", "")
    if not sid:
        return None
    ar = s.find("ar")
    dp = s.find("dp")
    tl = s.find("tl")
    return StopInfo(
        sid=sid,
        ar=ar,
        dp=dp,
        ar_pt=_parse_db_time(ar.get("pt")) if ar is not None else None,
        dp_pt=_parse_db_time(dp.get("pt")) if dp is not None else None,
        ar_pp=(ar.get("pp") or "") if ar is not None else "",
        dp_pp=(dp.get("pp") or "") if dp is not None else "",
        ar_cp=(ar.get("cp") or "") if ar is not None else "",
        dp_cp=(dp.get("cp") or "") if dp is not None else "",
        delay_ar=_delay_minutes(ar),
        delay_dp=_delay_minutes(dp),
        cancelado=_cancelado(ar) or _cancelado(dp),
        cat=(tl.get("c") or "") if tl is not None else "",
        num=(tl.get("n") or "") if tl is not None else "",
    )


# ── Catálogo de estaciones (StaDa) ───────────────────────────────────────


@dataclass
class StationRef:
    eva: str
    nombre: str
    categoria: int = 6
    region: str = ""                        # Bundesland (StaDa federalState)
    ciudad: str = ""                        # mailingAddress.city
    # Estado de rotación
    last_fchg: float = 0.0
    activity: int = 0                       # stops con cambios en su último fetch
    last_plan: dict[tuple[str, int], float] = field(default_factory=dict)


@dataclass
class StopState:
    """Último estado publicado por (eva, stop_id) — deduplica eventos."""

    updated: float = 0.0
    delay: int | None = None
    gleis: str | None = None
    cancelado: bool = False


class EventProducer:
    """Productor Event-Driven con datos reales de la DB API.

    Rota peticiones ``plan``/``fchg`` sobre el catálogo de estaciones con un
    presupuesto global de rate limit, y publica los eventos resultantes en
    Redis Pub/Sub. Nunca toca la base de datos en el camino crítico.
    """

    def __init__(self) -> None:
        self._redis = None  # bus de eventos (Redis o local)
        self._api = DbApi(DB_CLIENT_ID, DB_API_KEY)
        self._limiter = RateLimiter(DB_RATE_PER_MIN)
        self._running = False
        self._loop_task: asyncio.Task | None = None
        self._auth_error_logged = False

        # Estado
        self.stations: list[StationRef] = []
        self.stop_state: dict[tuple[str, str], StopState] = {}
        self.catalog_loaded_at: float = 0.0
        self._req_times: deque[float] = deque()
        # Alternancia plan/fchg en la selección de jobs (evita starving).
        self._alternar_plan = False

        # Métricas
        self.events_published = 0
        self.published_by_type = {"ocupacion": 0, "retraso": 0, "cancelacion": 0}
        self.req_total = 0
        self.req_429 = 0
        self.req_400 = 0
        self.req_errores = 0
        self.jobs_plan = 0
        self.jobs_fchg = 0
        self.jobs_catalog = 0
        self.plan_horas_perdidas = 0
        self.ultimo_job: str = ""
        self.ultimo_error: str = ""

    # ── Ciclo de vida ───────────────────────────────────────────────────

    async def start(self) -> None:
        self._redis = create_bus()
        self._running = True
        if not self._api.credenciales:
            logger.error(
                "Sin credenciales DB: define DB_CLIENT_ID y DB_API_KEY en .env "
                "(developers.deutschebahn.com). El productor queda en espera."
            )
        else:
            logger.info(
                "EventProducer (datos REALES) iniciado: rate=%s req/min, "
                "TTL fchg activa=%ss / fondo=%ss",
                DB_RATE_PER_MIN, FCHG_TTL_ACTIVA, FCHG_TTL_FONDO,
            )
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except DbAuthError as exc:
                if not self._auth_error_logged:
                    logger.error("DB API auth: %s", exc)
                    self._auth_error_logged = True
                self.ultimo_error = str(exc)
                await asyncio.sleep(60)
            except DbRateLimitError:
                self.req_429 += 1
                self._limiter.penalizar(30)
                logger.warning("429 de la DB API: backoff de 30s")
                await asyncio.sleep(30)
            except Exception as exc:
                self.req_errores += 1
                self.ultimo_error = str(exc)
                logger.exception("Error en ciclo de ingesta (se continúa)")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        self._running = False
        if self._loop_task is not None:
            self._loop_task.cancel()
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def _tick(self) -> None:
        if not self._api.credenciales:
            await asyncio.sleep(30)
            return
        job = self._pick_job()
        if job is None:
            await asyncio.sleep(1.0)
            return
        await self._limiter.acquire()
        await self._run_job(job)

    # ── Selección de job por prioridad (score) ──────────────────────────

    def _pick_job(self) -> tuple | None:
        now = time.time()

        # 1. Catálogo de estaciones (1/día; reintento con backoff si falla).
        if not self.stations or now - self.catalog_loaded_at > CATALOG_TTL_SEG:
            return ("catalog",)

        best_fchg: tuple | None = None
        best_plan: tuple | None = None
        score_fchg = SCORE_VENCIDO
        score_plan = SCORE_VENCIDO
        hoy = datetime.now(DB_TZ)
        fecha = hoy.strftime("%y%m%d")
        hora, minuto = hoy.hour, hoy.minute
        fecha_siguiente = (
            (hoy + timedelta(hours=1)).strftime("%y%m%d") if hora == 23 else fecha
        )

        for st in self.stations:
            # fchg: TTL según actividad + bonus de actividad y categoría.
            # El ratio se acota: una estación "nunca consultada" no debe
            # monopolizar el presupuesto frente a una activa vencida.
            ttl = FCHG_TTL_ACTIVA if st.activity else FCHG_TTL_FONDO
            score = min((now - st.last_fchg) / ttl, 2.0)
            if st.activity:
                score += 0.5
            score += CAT_BONUS.get(st.categoria, 0.0) * 0.5
            if score > score_fchg:
                score_fchg, best_fchg = score, ("fchg", st.eva)

            # plan de la hora actual y de la siguiente: 1 vez por estación y
            # hora. La hora actual pesa menos según avanza la hora (los planes
            # tardíos ya no sirven); la siguiente se prepara con antelación.
            bonus_cat = CAT_BONUS.get(st.categoria, 0.0)
            for h, f in ((hora, fecha), ((hora + 1) % 24, fecha_siguiente)):
                key = (f, h)
                age_ratio = min((now - st.last_plan.get(key, 0.0)) / PLAN_TTL_SEG, 1.05)
                s2 = age_ratio + bonus_cat
                if st.activity:
                    s2 += 0.2
                if h == hora:
                    s2 += 0.3 * (60 - minuto) / 60.0
                else:
                    s2 += 0.15
                if s2 > score_plan:
                    score_plan, best_plan = s2, ("plan", st.eva, key)

        # Alternancia 50/50 entre clases: sin ella, la primera pasada de
        # fchg (~5400 estaciones) se comería todo el presupuesto y no se
        # publicarían ocupaciones (sin plan no hay conflictos).
        if best_fchg and best_plan:
            if self._alternar_plan:
                self._alternar_plan = False
                return best_plan
            self._alternar_plan = True
            return best_fchg
        return best_fchg or best_plan

    def _limpiar_horas_viejas(self) -> None:
        """Descarta planes de horas ya pasadas (presión del rate limit)."""
        hoy = datetime.now(DB_TZ)
        siguiente = hoy + timedelta(hours=1)
        validas = {(hoy.strftime("%y%m%d"), hoy.hour), (siguiente.strftime("%y%m%d"), siguiente.hour)}
        for st in self.stations:
            viejas = [k for k in st.last_plan if k not in validas]
            for k in viejas:
                del st.last_plan[k]
                self.plan_horas_perdidas += 1

    # ── Ejecución de jobs ───────────────────────────────────────────────

    async def _run_job(self, job: tuple) -> None:
        kind = job[0]
        self._req_times.append(time.monotonic())
        self.req_total += 1
        msgs: list[tuple[str, str]] | None = None
        if kind == "catalog":
            self.ultimo_job = "StaDa /stations (catálogo)"
            try:
                await asyncio.to_thread(self._load_catalog)
                self.jobs_catalog += 1
            except Exception as exc:
                # El fallback SEED_EVAS ya está activo; se reintenta en 5 min
                # sin bloquear la ingesta de Timetables.
                self.ultimo_error = str(exc)
                logger.warning("Catálogo StaDa no disponible: %s", exc)
        elif kind == "plan":
            _, eva, key = job
            self.ultimo_job = f"Timetables /plan {eva} {key[0]}:{key[1]:02d}"
            msgs = await asyncio.to_thread(self._fetch_plan, eva, key)
            self.jobs_plan += 1
        elif kind == "fchg":
            _, eva = job
            self.ultimo_job = f"Timetables /fchg {eva}"
            msgs = await asyncio.to_thread(self._fetch_fchg, eva)
            self.jobs_fchg += 1
        if msgs:
            await self._publish(msgs)

    # ── StaDa: catálogo de estaciones de Alemania ───────────────────────

    def _load_catalog(self) -> None:
        try:
            self._cargar_catalogo()
        except Exception as exc:
            # Reintento en 5 min en vez de martillear la API.
            self.catalog_loaded_at = time.time() - CATALOG_TTL_SEG + 300
            if not self.stations:
                self.stations = [
                    StationRef(eva=eva, nombre=nombre, categoria=1)
                    for eva, nombre in SEED_EVAS
                ]
                logger.warning(
                    "StaDa no disponible (%s): fallback temporal con %d "
                    "estaciones reales; se reintenta el catálogo completo en 5 min",
                    exc, len(self.stations),
                )
            raise

    def _cargar_catalogo(self) -> None:
        stations: list[StationRef] = []
        offset = 0
        total = None
        while total is None or offset < total:
            data = self._api.get_stations_page(offset)
            total = int(data.get("total", 0))
            for item in data.get("result", []):
                if item.get("countryCode") not in (None, "DE"):
                    continue
                nombre = (item.get("name") or "").strip()
                if not nombre:
                    continue
                categoria = int(item.get("category") or 6)
                region = (item.get("federalState") or "").strip()
                ciudad = ((item.get("mailingAddress") or {}).get("city") or "").strip()
                # Solo el EVA principal: StaDa añade EVAs "meta" (p. ej.
                # München Hbf 8000261 vs 8098261/8098263) que duplicarían
                # ocupaciones y alertas de la misma estación física.
                evas = [e for e in (item.get("evaNumbers") or []) if e.get("isMain")]
                if not evas:
                    evas = item.get("evaNumbers") or []
                for eva_obj in evas:
                    eva = str(eva_obj.get("number") or "").strip()
                    if eva:
                        stations.append(
                            StationRef(
                                eva=eva,
                                nombre=nombre,
                                categoria=categoria,
                                region=region,
                                ciudad=ciudad,
                            )
                        )
            offset += 10000
            if not data.get("result"):
                break

        # Preservar la rotación ya realizada si el catálogo se recarga.
        prev = {st.eva: st for st in self.stations}
        for st in stations:
            old = prev.get(st.eva)
            if old is not None:
                st.last_fchg = old.last_fchg
                st.activity = old.activity
                st.last_plan = old.last_plan
        if STADA_MAX_ESTACIONES:
            stations = stations[:STADA_MAX_ESTACIONES]
        self.stations = stations
        self.catalog_loaded_at = time.time()
        self._limpiar_horas_viejas()
        logger.info(
            "Catálogo StaDa cargado: %s paradas (EVA) en Alemania", len(stations)
        )

    def _station(self, eva: str) -> StationRef | None:
        for st in self.stations:
            if st.eva == eva:
                return st
        return None

    # ── Timetables: plan por hora (Sollfahrplan con Gleis) ──────────────

    def _fetch_plan(self, eva: str, key: tuple[str, int]) -> list[tuple[str, str]]:
        fecha, hora = key
        st = self._station(eva)
        if st is None:
            return []
        root = self._api.get_xml(f"/plan/{eva}/{fecha}/{hora:02d}")
        st.last_plan[key] = time.time()
        if root is None:
            return []

        msgs: list[tuple[str, str]] = []
        for s in root.findall("s"):
            info = _parse_stop(s)
            if info is None or info.cancelado:
                continue
            horas = info.horas_plan
            if horas is None:
                continue
            state = self._get_state(eva, info.sid)
            if state.cancelado:
                continue  # cancelado por fchg: no re-insertar
            gleis = info.gleis_plan
            if state.gleis:
                gleis = state.gleis  # cambio de Gleis ya publicado (cp)
            if not gleis:
                continue
            if not _vigente(horas[1], state.delay or 0):
                continue  # ocupación ya terminada: no publicar
            tren_id = info.tren_id(eva)
            msgs.append(
                (
                    CH_OCUPACION,
                    ocupacion_event(
                        estacion_id=eva,
                        nombre_estacion=st.nombre,
                        gleis=gleis,
                        tren_id=tren_id,
                        hora_llegada=horas[0],
                        hora_salida=horas[1],
                        retraso_min=float(state.delay or 0),
                        fuente="db-timetables",
                        estimada=info.estimada,
                        region=st.region,
                        ciudad=st.ciudad,
                        tren_categoria=info.cat,
                        tren_numero=info.num,
                    ),
                )
            )
        return msgs

    # ── Timetables: cambios en vivo (fchg) ──────────────────────────────

    def _fetch_fchg(self, eva: str) -> list[tuple[str, str]]:
        st = self._station(eva)
        if st is None:
            return []
        root = self._api.get_xml(f"/fchg/{eva}")
        st.last_fchg = time.time()
        if root is None:
            st.activity = 0
            return []

        msgs: list[tuple[str, str]] = []
        cambios = 0
        for s in root.findall("s"):
            info = _parse_stop(s)
            if info is None:
                continue
            tren_id = info.tren_id(eva)
            state = self._get_state(eva, info.sid)
            horas = info.horas_plan
            delay = info.delay_min or 0
            vigente = horas is not None and _vigente(
                horas[1], max(delay, state.delay or 0)
            )

            # Cancelación (Ausfall) / recuperación.
            if info.cancelado and not state.cancelado:
                state.cancelado = True
                cambios += 1
                if vigente:
                    msgs.append(
                        (
                            CH_CANCELACION,
                            cancelacion_event(
                                estacion_id=eva,
                                tren_id=tren_id,
                                motivo=_motivo(s, "Ausfall"),
                            ),
                        )
                    )
            elif not info.cancelado and state.cancelado:
                state.cancelado = False
                cambios += 1
                if vigente and horas and info.gleis_plan:
                    msgs.append(
                        (
                            CH_OCUPACION,
                            ocupacion_event(
                                estacion_id=eva,
                                nombre_estacion=st.nombre,
                                gleis=info.gleis_plan,
                                tren_id=tren_id,
                                hora_llegada=horas[0],
                                hora_salida=horas[1],
                                retraso_min=float(state.delay or 0),
                                estimada=info.estimada,
                                region=st.region,
                                ciudad=st.ciudad,
                                tren_categoria=info.cat,
                                tren_numero=info.num,
                            ),
                        )
                    )

            if state.cancelado:
                state.updated = time.time()
                continue

            # Verspätung real: sólo si cambió respecto a lo publicado.
            if delay != (state.delay or 0):
                state.delay = delay if delay > 0 else None
                cambios += 1
                if vigente:
                    msgs.append(
                        (
                            CH_RETRASO,
                            retraso_event(
                                estacion_id=eva,
                                tren_id=tren_id,
                                retraso_min=float(delay),
                                motivo=_motivo(s, "Verspätung")
                                if delay >= RETRASO_MINIMO_MIN
                                else "Sin Verspätung",
                            ),
                        )
                    )

            # Cambio de Gleis real (cp): reubicar la ocupación.
            gleis_cp = info.gleis_cp
            if gleis_cp and gleis_cp != (state.gleis or info.gleis_plan):
                state.gleis = gleis_cp
                cambios += 1
                if vigente and horas:
                    msgs.append(
                        (
                            CH_OCUPACION,
                            ocupacion_event(
                                estacion_id=eva,
                                nombre_estacion=st.nombre,
                                gleis=gleis_cp,
                                tren_id=tren_id,
                                hora_llegada=horas[0],
                                hora_salida=horas[1],
                                retraso_min=float(state.delay or 0),
                                estimada=info.estimada,
                                region=st.region,
                                ciudad=st.ciudad,
                                tren_categoria=info.cat,
                                tren_numero=info.num,
                            ),
                        )
                    )
            elif not state.gleis and info.gleis_plan:
                state.gleis = info.gleis_plan

            state.updated = time.time()

        st.activity = min(cambios * 10, 50)
        return msgs

    # ── Estado por parada + publicación ─────────────────────────────────

    def _get_state(self, eva: str, sid: str) -> StopState:
        key = (eva, sid)
        state = self.stop_state.get(key)
        if state is None:
            state = StopState()
            self.stop_state[key] = state
        return state

    def _prune_stop_state(self) -> None:
        """Descarta estado de paradas que ya no interesan (>2 h)."""
        cutoff = time.time() - STOP_STATE_TTL_SEG
        viejas = [k for k, v in self.stop_state.items() if v.updated < cutoff]
        for k in viejas:
            del self.stop_state[k]

    async def _publish(self, msgs: list[tuple[str, str]]) -> None:
        if not msgs or self._redis is None:
            return
        pipe = self._redis.pipeline(transaction=False)
        for channel, event in msgs:
            pipe.publish(channel, json.dumps(event, ensure_ascii=False))
            self.events_published += 1
            self.published_by_type[event["tipo"]] = (
                self.published_by_type.get(event["tipo"], 0) + 1
            )
        try:
            await pipe.execute()
        except Exception:
            logger.exception("No se pudieron publicar %s eventos", len(msgs))

    # ── Métricas para /api/metrics ──────────────────────────────────────

    def metrics(self) -> dict:
        now = time.monotonic()
        while self._req_times and self._req_times[0] < now - 60:
            self._req_times.popleft()
        return {
            "credenciales": self._api.credenciales,
            "modo": "datos-reales" if self._api.credenciales else "sin-credenciales",
            "rate_limit_min": DB_RATE_PER_MIN,
            "req_ultimo_min": len(self._req_times),
            "req_total": self.req_total,
            "req_429": self.req_429,
            "req_400": self._api.bad_requests,
            "req_errores": self.req_errores,
            "estaciones": len(self.stations),
            "catalogo_cargado": bool(self.stations),
            "estaciones_activas": sum(1 for st in self.stations if st.activity),
            "jobs_plan": self.jobs_plan,
            "jobs_fchg": self.jobs_fchg,
            "jobs_catalog": self.jobs_catalog,
            "plan_horas_perdidas": self.plan_horas_perdidas,
            "eventos_publicados": self.events_published,
            "ocupaciones": self.published_by_type["ocupacion"],
            "retrasos": self.published_by_type["retraso"],
            "cancelaciones": self.published_by_type["cancelacion"],
            "stop_state": len(self.stop_state),
            "ultimo_job": self.ultimo_job,
            "ultimo_error": self.ultimo_error,
        }

    # ── Mantenimiento periódico (fuera del presupuesto de rate limit) ───

    async def _maintenance_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(60)
                self._limpiar_horas_viejas()
                self._prune_stop_state()
        except asyncio.CancelledError:
            raise


# Instancia global compartida con /api/metrics
producer = EventProducer()


async def run_ingestor() -> None:
    """Entry point para la task de ingesta en el lifespan de FastAPI."""
    maintenance = asyncio.create_task(producer._maintenance_loop())
    try:
        await producer.start()
    finally:
        maintenance.cancel()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(
        "EventProducer (DB API real). Credenciales: "
        + ("sí" if DB_CLIENT_ID and DB_API_KEY else "NO → define DB_CLIENT_ID/DB_API_KEY")
    )
