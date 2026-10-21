"""Bahnsteigkonflikt-Alarm — Dashboard en Streamlit (Streamlit Cloud).

En Streamlit Cloud no hay Redis, PostgreSQL ni Docker, así que el pipeline
Event-Driven se ejecuta dentro del propio proceso del dashboard, con un bus
de eventos local (``app/bus.py``) que expone la misma API que Redis Pub/Sub:

    DB Timetables + StaDa (datos REALES)
        -> EventProducer  ->  bus  ->  StreamDetector  ->  Dashboard

La lógica de negocio (parseo IRIS, ventanas deslizantes, niveles de alerta,
número de tren, filtros) es exactamente la misma que en el despliegue con
Docker (``app/api.py`` + Redis + PostgreSQL).
"""

from __future__ import annotations

import os

# Sin Redis ni PostgreSQL en Streamlit Cloud: bus de eventos local y
# auditoría desactivada. Debe fijarse ANTES de importar el pipeline.
os.environ.setdefault("BUS_LOCAL", "1")
os.environ.setdefault("AUDIT_ENABLED", "0")

import threading
import time
from datetime import datetime

import streamlit as st

# Credenciales de la DB API: Streamlit Secrets → variables de entorno.
def _secretos_a_entorno() -> None:
    for clave in ("DB_CLIENT_ID", "DB_API_KEY"):
        if os.environ.get(clave):
            continue
        try:
            valor = st.secrets.get(clave)
        except Exception:
            valor = None
        if valor:
            os.environ[clave] = str(valor)


_secretos_a_entorno()

from app.detector import StreamDetector  # noqa: E402
from app.ingest import EventProducer  # noqa: E402

# ── Configuración de la página ──────────────────────────────────────────

st.set_page_config(
    page_title="Bahnsteigkonflikt-Alarm · Deutsche Bahn",
    page_icon="🚆",
    layout="wide",
)

CAUSA_LABEL = {
    "efecto_dominó": "Dominoeffekt (Verspätung)",
    "retraso_contribuyente": "Beitragende Verspätung",
    "solapamiento_programado": "Geplante Überlappung",
}


# ── Motor Event-Driven dentro del proceso ───────────────────────────────

class Motor:
    """Productor + detector corriendo en un hilo aparte del hilo de Streamlit.

    ``snapshot`` se reemplaza (nunca se muta) para que el hilo de la UI lo
    pueda leer sin bloqueos ni carreras.
    """

    def __init__(self) -> None:
        self.snapshot: list[dict] = []
        self.ultimo_update = 0.0
        self.error = ""
        self.productor: EventProducer | None = None
        self.detector: StreamDetector | None = None
        self._hilo: threading.Thread | None = None
        self._arrancar()

    # ── Ciclo de vida ───────────────────────────────────────────────────
    def _arrancar(self) -> None:
        self._hilo = threading.Thread(
            target=self._bucle, name="motor-eventos", daemon=True
        )
        self._hilo.start()

    def vivo(self) -> bool:
        return self._hilo is not None and self._hilo.is_alive()

    def asegurar_vivo(self) -> None:
        """Relanza el pipeline si el hilo murió (p. ej. tras una suspensión)."""
        if not self.vivo() and not self.error:
            self._arrancar()

    def _bucle(self) -> None:
        try:
            import asyncio

            asyncio.run(self._main())
        except Exception as exc:  # pragma: no cover
            self.error = f"{type(exc).__name__}: {exc}"

    async def _main(self) -> None:
        productor = EventProducer()
        detector = StreamDetector()
        self.productor = productor
        self.detector = detector

        async def refresco() -> None:
            while True:
                await asyncio.sleep(2.0)
                try:
                    datos = detector.snapshot()
                except RuntimeError:
                    continue
                self.snapshot = datos
                self.ultimo_update = time.time()

        await asyncio.gather(
            productor.start(), detector.start(), refresco()
        )


@st.cache_resource
def motor() -> Motor:
    """Un único motor por proceso (sobrevive a los reruns de Streamlit)."""
    return Motor()


# ── Utilidades de presentación ──────────────────────────────────────────

def _esc(texto) -> str:
    return (
        str(texto)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _hora(iso) -> str:
    try:
        return datetime.fromisoformat(str(iso)).strftime("%H:%M:%S")
    except Exception:
        return "—"


def _etiqueta_tren(conflicto: dict, n: int) -> str:
    """Número de tren (Zugnummer) — no la línea/ruta."""
    numero = conflicto.get(f"tren_{n}_numero") or ""
    categoria = conflicto.get(f"tren_{n}_categoria") or ""
    if numero:
        return f"{categoria} {numero}".strip()
    identificador = conflicto.get(f"tren_{n}_id") or ""
    parte = identificador.split(":")[1] if ":" in identificador else identificador
    return parte.rsplit("-", 1)[0] if "-" in parte else (parte or "—")


def _catalogo(motor_: Motor) -> tuple[dict, dict]:
    """(regiones -> ciudades, conteo de alertas por región/ciudad)."""
    ciudades_por_region: dict[str, set[str]] = {}
    for estacion in getattr(motor_.productor, "stations", None) or []:
        if estacion.region and estacion.ciudad:
            ciudades_por_region.setdefault(estacion.region, set()).add(
                estacion.ciudad
            )
    alertas_region: dict[str, int] = {}
    alertas_ciudad: dict[str, int] = {}
    for conflicto in motor_.snapshot:
        if conflicto.get("estado") == "resuelto":
            continue
        if conflicto.get("region"):
            alertas_region[conflicto["region"]] = (
                alertas_region.get(conflicto["region"], 0) + 1
            )
        if conflicto.get("ciudad"):
            alertas_ciudad[conflicto["ciudad"]] = (
                alertas_ciudad.get(conflicto["ciudad"], 0) + 1
            )
    for conflicto in motor_.snapshot:
        region = conflicto.get("region")
        ciudad = conflicto.get("ciudad")
        if region and ciudad:
            ciudades_por_region.setdefault(region, set()).add(ciudad)
    return ciudades_por_region, (alertas_region, alertas_ciudad)


def _tarjeta(conflicto: dict) -> str:
    """Tarjeta HTML de una alerta (mismo aspecto que el panel FastAPI)."""
    riesgo = conflicto.get("nivel") == "riesgo"
    clase = "ambar" if riesgo else "rojo"
    color = "#f59e0b" if riesgo else "#ef4444"
    tono = "#fbbf24" if riesgo else "#fca5a5"
    nivel = "Risiko · knapper Puffer" if riesgo else "Konflikt · überlappende Belegung"
    solape = float(conflicto.get("solape_min") or 0)
    if solape > 0:
        medida = (
            f"Überlappung{' (gesch.)' if riesgo else ''}: "
            f"<strong style='color:{tono}'>{solape:.2f} Min.</strong>"
        )
    else:
        medida = (
            "Puffer: <strong style='color:{tono}'>"
            f"{float(conflicto.get('minutos_diferencia') or 0):.2f} Min.</strong>"
        )
    retraso = float(conflicto.get("retraso_min") or 0)
    etiqueta_retraso = (
        f"<span class='chip-ambar'>+{retraso:.0f} Min. Verspätung</span>" if retraso else ""
    )
    gleiswechsel = ""
    if conflicto.get("sugerencia_gleiswechsel"):
        alt = conflicto.get("gleis_alternativo") or "?"
        gleiswechsel = (
            f"<div class='gw'>↻ Gleiswechsel empfohlen &rarr; Gleis {_esc(alt)}"
            "<span>Bahnsteig-Neuvergabe</span></div>"
        )
    lugar = " · ".join(
        x for x in (conflicto.get("ciudad"), conflicto.get("region")) if x
    )
    return f"""
<div class="karte {clase}">
  <div class="k-cab">
    <div>
      <div class="k-nivel" style="color:{tono}">{nivel}</div>
      <div class="k-causa">{_esc(CAUSA_LABEL.get(conflicto.get('causa'), conflicto.get('causa', '')))}</div>
      <h3>{_esc(conflicto.get('nombre_estacion') or conflicto.get('estacion_id'))}</h3>
      {f'<div class="k-lugar">{_esc(lugar)}</div>' if lugar else ''}
    </div>
    <div class="k-gleis" style="background:{color}">Gleis {_esc(conflicto.get('gleis'))}</div>
  </div>
  <div class="k-zuege">
    <div class="k-zug"><div class="k-et">Zug-Nr. 1</div>
      <div class="k-nr">{_esc(_etiqueta_tren(conflicto, 1))}</div></div>
    <div class="k-zug"><div class="k-et">Zug-Nr. 2</div>
      <div class="k-nr">{_esc(_etiqueta_tren(conflicto, 2))}</div></div>
  </div>
  {gleiswechsel}
  <div class="k-pie">
    <span>{medida} {etiqueta_retraso}</span>
    <span class="k-hora">{_esc(_hora(conflicto.get('primera_deteccion')))}</span>
  </div>
</div>"""


CSS = """
<style>
.karte {border-radius:16px;border:2px solid;padding:16px 18px;margin:0 0 14px 0;}
.karte.rojo {border-color:#ef4444;background:linear-gradient(135deg,#450a0a 0%,#0f172a 70%);}
.karte.ambar {border-color:#f59e0b;background:linear-gradient(135deg,#451a03 0%,#0f172a 70%);}
.k-cab {display:flex;justify-content:space-between;align-items:flex-start;gap:12px;}
.k-nivel {font-size:12px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;}
.k-causa {font-size:11px;color:#94a3b8;margin-top:2px;}
.karte h3 {font-size:20px;font-weight:700;margin:4px 0 0;color:#f1f5f9;}
.k-lugar {font-size:11px;color:#64748b;margin-top:2px;}
.k-gleis {flex:none;color:#fff;font-weight:800;padding:6px 12px;border-radius:12px;font-size:18px;}
.k-zuege {display:flex;gap:10px;margin-top:14px;}
.k-zug {flex:1;background:rgba(2,6,23,.7);border:1px solid rgba(148,163,184,.22);
        border-radius:12px;padding:8px 10px;}
.k-et {font-size:10px;text-transform:uppercase;color:#94a3b8;}
.k-nr {font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-weight:700;font-size:18px;
       color:#e2e8f0;overflow-wrap:anywhere;}
.k-gw {margin-top:12px;border:1px solid rgba(245,158,11,.5);background:rgba(245,158,11,.1);
       border-radius:12px;padding:9px 12px;font-size:12px;font-weight:600;color:#fcd34d;
       display:flex;justify-content:space-between;}
.k-gw span {font-weight:400;font-size:10px;color:#fbbf24;}
.k-pie {display:flex;justify-content:space-between;align-items:center;gap:8px;
        margin-top:14px;font-size:14px;color:#cbd5e1;flex-wrap:wrap;}
.k-hora {font-size:12px;color:#94a3b8;}
.chip-ambar {display:inline-block;background:rgba(217,119,6,.2);border:1px solid rgba(217,119,6,.4);
             border-radius:8px;padding:1px 8px;font-size:11px;font-weight:700;color:#fcd34d;margin-left:6px;}
.st-cabecera {display:flex;align-items:center;justify-content:space-between;gap:16px;
              flex-wrap:wrap;margin-bottom:6px;}
.st-titulo {display:flex;align-items:center;gap:12px;}
.st-logo {width:40px;height:40px;border-radius:12px;background:#dc2626;color:#fff;
          display:flex;align-items:center;justify-content:center;font-weight:900;font-size:16px;}
.st-h1 {font-size:20px;font-weight:700;color:#f1f5f9;line-height:1.15;}
.st-sub {font-size:12px;color:#94a3b8;}
.st-badge {border-radius:9999px;padding:4px 12px;font-size:12px;font-weight:600;}
.st-vacio {border:1px solid rgba(16,185,129,.35);background:rgba(16,185,129,.08);
           border-radius:20px;padding:40px;text-align:center;color:#6ee7b7;}
.st-vacio h2 {margin:0;font-size:22px;color:#34d399;}
.st-vacio p {margin:6px 0 0;color:#a7f3d0;font-size:14px;}
</style>
"""


# ── Render ──────────────────────────────────────────────────────────────

st.markdown(CSS, unsafe_allow_html=True)

motor_ = motor()
motor_.asegurar_vivo()
if not motor_.vivo() and motor_.error:
    st.error(f"Fehler im Event-Pipeline: {motor_.error}")

# ── Panel en vivo (todo dentro de un único fragmento) ────────────────────

def _estado_badge(creds: bool) -> tuple[str, str]:
    if motor_.error:
        return "Fehler", "#b45309"
    if not creds:
        return "Keine Zugangsdaten", "#b45309"
    if not motor_.ultimo_update:
        return "Startet…", "#0369a1"
    return "Live (Event-Driven)", "#0284c7"


@st.fragment(run_every="5s")
def panel() -> None:
    """Cabecera + filtros + KPIs + tarjetas; se refresca cada 5 s."""
    prod = motor_.productor
    det = motor_.detector
    creds = bool(
        os.environ.get("DB_CLIENT_ID") and os.environ.get("DB_API_KEY")
    )
    metricas = det.metrics() if det else {}
    productoras = prod.metrics() if prod else {}

    # ── Cabecera ────────────────────────────────────────────────────────
    col_titulo, col_badge = st.columns([4, 1])
    with col_titulo:
        st.markdown(
            """
<div class="st-cabecera">
  <div class="st-titulo">
    <div class="st-logo">DB</div>
    <div>
      <div class="st-h1">Bahnsteigkonflikt-Alarm</div>
      <div class="st-sub">Ereignisgesteuert · DB Timetables + StaDa (Echtzeit) ·
        Gleitende 15-Minuten-Fenster · Horizont 30 Min.</div>
    </div>
  </div>
</div>""",
            unsafe_allow_html=True,
        )
    with col_badge:
        texto_badge, color_badge = _estado_badge(creds)
        st.markdown(
            f'<div style="text-align:right"><span class="st-badge" '
            f'style="background:{color_badge};color:#fff">{texto_badge}</span></div>',
            unsafe_allow_html=True,
        )

    # ── Filtros ─────────────────────────────────────────────────────────
    ciudades_por_region, (alertas_region, _alertas_ciudad) = _catalogo(motor_)
    regiones = sorted(ciudades_por_region.keys() | set(alertas_region.keys()))

    f1, f2, f3, f4 = st.columns([2, 2, 2, 3])
    with f1:
        region = st.selectbox(
            "Region",
            ["Alle Regionen"] + regiones,
            index=0,
            label_visibility="collapsed",
        )
    with f2:
        if region == "Alle Regionen":
            ciudades = sorted(
                {c for lista in ciudades_por_region.values() for c in lista}
            )
        else:
            ciudades = sorted(ciudades_por_region.get(region, set()))
        ciudad = st.selectbox(
            "Stadt",
            ["Alle Städte"] + (ciudades or ["—"]),
            index=0,
            label_visibility="collapsed",
        )
    with f3:
        nivel = st.radio(
            "Stufe",
            ["Alle", "Konflikte", "Risiken"],
            horizontal=True,
            label_visibility="collapsed",
        )
    with f4:
        causa = st.selectbox(
            "Ursache",
            ["Alle Ursachen"] + list(CAUSA_LABEL.values()),
            index=0,
            label_visibility="collapsed",
        )

    # ── KPIs ────────────────────────────────────────────────────────────
    conflictos = motor_.snapshot
    activos = [c for c in conflictos if c.get("estado") != "resuelto"]

    clave_causa = next((k for k, v in CAUSA_LABEL.items() if v == causa), None)
    if region != "Alle Regionen":
        activos = [c for c in activos if c.get("region") == region]
    if ciudad != "Alle Städte":
        activos = [c for c in activos if c.get("ciudad") == ciudad]
    if nivel == "Konflikte":
        activos = [c for c in activos if c.get("nivel") != "riesgo"]
    elif nivel == "Risiken":
        activos = [c for c in activos if c.get("nivel") == "riesgo"]
    if clave_causa:
        activos = [c for c in activos if c.get("causa") == clave_causa]

    n_conflicto = sum(1 for c in activos if c.get("nivel") != "riesgo")
    n_riesgo = len(activos) - n_conflicto

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Aktive Konflikte", n_conflicto)
    c2.metric("Risiken", n_riesgo)
    c3.metric(
        "Verarbeitete Ereignisse",
        f"{metricas.get('events_in', 0):,}".replace(",", "."),
    )
    c4.metric(
        "Verspätungen",
        f"{metricas.get('delays_applied', 0):,}".replace(",", "."),
    )
    c5.metric("DB-API Anfragen/Min", productoras.get("req_ultimo_min", "—"))
    c6.metric(
        "Bahnhöfe (StaDa)",
        f"{productoras.get('estaciones', 0):,}".replace(",", ".")
        if productoras.get("catalogo_cargado")
        else "lädt…",
    )

    st.markdown("---")

    if not creds:
        st.warning(
            "**Keine DB-API-Zugangsdaten.** Im Streamlit Cloud unter "
            "*Settings → Secrets* `DB_CLIENT_ID` und `DB_API_KEY` hinterlegen "
            "(developers.deutschebahn.com). Ohne Zugangsdaten kommen keine "
            "Live-Daten."
        )
        return

    if not activos:
        st.markdown(
            """
<div class="st-vacio">
  <h2>Alles OK</h2>
  <p>Keine Bahnsteigkonflikte im gleitenden Fenster (letzte 15 Min.).</p>
</div>""",
            unsafe_allow_html=True,
        )
        return

    st.markdown(f"**{len(activos)} aktive Warnungen**")
    for i in range(0, min(len(activos), 24), 2):
        izquierda, derecha = st.columns(2)
        with izquierda:
            st.markdown(_tarjeta(activos[i]), unsafe_allow_html=True)
        if i + 1 < len(activos):
            with derecha:
                st.markdown(_tarjeta(activos[i + 1]), unsafe_allow_html=True)
    if len(activos) > 24:
        st.caption(f"+{len(activos) - 24} weitere Warnungen (neueste 24 angezeigt)")


panel()
