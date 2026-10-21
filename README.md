# Bahnsteigkonflikt-Alarm

MVP **Event-Driven** de detección de conflictos de andén (Gleis) con **datos
reales de la Deutsche Bahn**. Cada alerta indica la estación, el andén, el
**número de tren** de los dos servicios implicados y el motivo (efecto
dominó por retraso, retraso contribuyente o solapamiento programado).

```
DB Timetables API (plan + fchg)  ─┐
                                   ├─► EventProducer ─► bus de eventos ─► StreamDetector ─► Dashboard
DB StaDa API (catálogo ALE)       ─┘                    (Redis / local)   (ventanas 15 min)
```

- **Productor**: rota peticiones a la DB API respetando el rate limit del plan
  Free (60 req/min) y publica eventos `ocupacion` / `retraso` / `cancelacion`.
- **Detector**: ventanas deslizantes por `(estación, Gleis)`; distingue
  **Konflikt** (solape real de ocupaciones) de **Risiko** (margen < 3 min).
- **Auditoría**: PostgreSQL guarda el historial de alertas (fuera del camino
  crítico). Opcional: se desactiva con `AUDIT_ENABLED=0`.

## Despliegue A — Streamlit Cloud (dashboard)

El pipeline completo corre **dentro del proceso** de Streamlit, con un bus de
eventos local (`app/bus.py`) que expone la misma API que Redis.

1. **Main module path**: `app/main.py` (ya viene así por defecto).
2. **Settings → Secrets** (obligatorio para tener datos reales):

   ```toml
   DB_CLIENT_ID = "tu-client-id"
   DB_API_KEY   = "tu-api-key"
   ```

   Se obtienen en <https://developers.deutschebahn.com> (suscripción a
   *Timetables API* y *Station Data API*). Sin estas credenciales el panel
   avisa de que no hay datos.
3. Cambios de código: se aplican solos al hacer push.

> `requirements.txt` (raíz) solo instala `streamlit` y `requests`: `psycopg2`
> queda fuera porque no tiene wheel para Python 3.13+ y hacía fallar el
> despliegue con `pg_config executable not found`.

## Despliegue B — Docker (API FastAPI + SSE)

```bash
cp .env.example .env      # credenciales DB API
docker compose up -d --build
# dashboard: http://localhost:8000
```

- `app/api.py`: API FastAPI (`uvicorn app.api:app`), SSE en
  `GET /api/stream/conflictos` y auditoría en PostgreSQL.
- `app/static/index.html`: panel HTML con SSE (push, sin polling).
- Puertos del host: app `8000`, PostgreSQL `5434`, Redis `6380` (cámbialos en
  `docker-compose.yml` si chocan con otro proyecto local).

| Endpoint | Descripción |
|---|---|
| `GET /` | Dashboard HTML |
| `GET /api/stream/conflictos` | SSE con las alertas en vivo |
| `GET /api/conflictos` | Historial de alertas (PostgreSQL) |
| `GET /api/estaciones` | Catálogo StaDa: regiones y ciudades |
| `GET /api/metrics` | Métricas del productor y del detector |
| `GET /api/health` | Estado de DB / Redis / credenciales |

## Estructura

```
app/
  main.py        Dashboard de Streamlit Cloud (entrypoint)
  api.py         API FastAPI (Docker) con SSE
  bus.py         Bus de eventos: Redis Pub/Sub o local en memoria
  ingest.py      Productor: DB API (Timetables + StaDa), rate limit, rotación
  detector.py    Stream processor: ventanas deslizantes, niveles, Gleiswechsel
  events.py      Contrato de eventos (Pub/Sub)
  db.py          Auditoría en PostgreSQL (opcional)
  static/        Panel HTML con SSE
sql/schema.sql   Esquema + migraciones idempotentes
```

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `DB_CLIENT_ID` / `DB_API_KEY` | — | Credenciales del DB API Marketplace |
| `DB_RATE_PER_MIN` | `55` | Peticiones/min a la DB API (límite 60) |
| `REDIS_URL` | `redis://redis:6379/0` | Bus de eventos |
| `BUS_LOCAL` | `0` | `1` = bus en memoria (Streamlit Cloud) |
| `AUDIT_ENABLED` | `1` | `0` = sin PostgreSQL |
| `VENTANA_SEG` | `900` | Ventana deslizante (15 min) |
| `UMBRAL_SEGUNDOS` | `180` | Umbral de *Risiko* (3 min) |
| `HORIZONTE_SEG` | `1800` | Horizonte de predicción (30 min) |

## Datos

Deutsche Bahn – DB API Marketplace: *Timetables API* y *Station Data API*,
licencia **CC BY 4.0**. Sin datos simulados: si no hay credenciales, el panel
no muestra alertas.
