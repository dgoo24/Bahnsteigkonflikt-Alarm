"""Motor Event-Driven del dashboard (productor + detector en un hilo aparte).

Vive en un **módulo normal** (y no en el script principal) a propósito:

- ``app/main.py`` se re-ejecuta en cada rerun de Streamlit y su espacio de
  nombres se reconstruye; un objeto cacheado ahí (``st.cache_resource``)
  puede conservar la clase y los ``__globals__`` de una versión anterior del
  código. Aquí, en cambio, el módulo se importa **una sola vez por proceso**,
  así que la clase y sus globales son estables.
- El motor es un **singleton por proceso**: sobrevive a los reruns y a las
  suspensiones de Streamlit Cloud, y no duplica peticiones a la DB API.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
import traceback
import types


class Motor:
    """Productor + detector corriendo en un hilo aparte del hilo de Streamlit.

    ``snapshot`` se reemplaza (nunca se muta) para que el hilo de la UI lo
    pueda leer sin bloqueos ni carreras.
    """

    def __init__(self) -> None:
        self.snapshot: list[dict] = []
        self.ultimo_update = 0.0
        self.error = ""
        self.error_detalle = ""
        self.productor = None
        self.detector = None
        self._hilo: threading.Thread | None = None
        self._loop = None
        self._detenido = False
        self._arrancar()

    # ── Ciclo de vida ───────────────────────────────────────────────────
    def _arrancar(self) -> None:
        self.error = ""
        self.error_detalle = ""
        self._detenido = False
        self._hilo = threading.Thread(
            target=self._bucle, name="motor-eventos", daemon=True
        )
        self._hilo.start()

    def vivo(self) -> bool:
        return self._hilo is not None and self._hilo.is_alive()

    def asegurar_vivo(self) -> None:
        """Relanza el pipeline si el hilo murió (p. ej. tras una suspensión)."""
        if not self.vivo():
            self._arrancar()

    def parar(self) -> None:
        """Detiene el motor (para no duplicar peticiones a la DB API)."""
        self._detenido = True
        loop = self._loop
        if loop is not None and not loop.is_closed():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:  # pragma: no cover
                pass

    # ── Hilo del pipeline ───────────────────────────────────────────────
    def _bucle(self) -> None:
        try:
            import asyncio  # local: inmune a cambios de módulo

            asyncio.run(self._main())
        except Exception as exc:  # pragma: no cover
            if self._detenido:
                return
            self.error = f"{type(exc).__name__}: {exc}"
            self.error_detalle = traceback.format_exc()

    async def _main(self) -> None:
        import asyncio  # local: inmune a cambios de módulo

        from app.detector import StreamDetector
        from app.ingest import EventProducer

        self._loop = asyncio.get_running_loop()

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


# El singleton vive en sys.modules con una clave propia: así sobrevive a
# recargas del script (el watcher puede purgar los módulos del proyecto, pero
# no las entradas añadidas a mano a sys.modules) y sigue siendo UN motor por
# proceso, sin duplicar peticiones a la DB API.
_CLAVE = "_bahnsteigkonflikt_motor"


def obtener_motor() -> Motor:
    """Devuelve el motor del proceso (lo crea la primera vez)."""
    modulo = sys.modules.get(_CLAVE)
    if not isinstance(modulo, types.ModuleType):
        modulo = types.ModuleType(_CLAVE)
        sys.modules[_CLAVE] = modulo
    motor = getattr(modulo, "motor", None)
    if motor is None or not motor.vivo():
        if motor is not None:
            motor.parar()
        motor = Motor()
        modulo.motor = motor
    return motor
