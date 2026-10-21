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
import threading
import time
import traceback


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
        self._arrancar()

    # ── Ciclo de vida ───────────────────────────────────────────────────
    def _arrancar(self) -> None:
        self.error = ""
        self.error_detalle = ""
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
        """Detiene el motor (solo para pruebas)."""
        self._hilo = None

    # ── Hilo del pipeline ───────────────────────────────────────────────
    def _bucle(self) -> None:
        try:
            import asyncio  # local: inmune a cambios de módulo

            asyncio.run(self._main())
        except Exception as exc:  # pragma: no cover
            self.error = f"{type(exc).__name__}: {exc}"
            self.error_detalle = traceback.format_exc()

    async def _main(self) -> None:
        import asyncio  # local: inmune a cambios de módulo

        from app.detector import StreamDetector
        from app.ingest import EventProducer

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


_MOTOR: Motor | None = None


def obtener_motor() -> Motor:
    """Devuelve el motor del proceso (lo crea la primera vez)."""
    global _MOTOR
    if _MOTOR is None:
        _MOTOR = Motor()
    return _MOTOR
