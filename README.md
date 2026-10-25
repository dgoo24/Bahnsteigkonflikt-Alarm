# 🚆 Bahnsteigkonflikt-Alarm
MVP zur ereignisgesteuerten Erkennung von Bahnsteigkonflikten (Gleis) mit Echtzeitdaten der Deutschen Bahn. Jede Warnmeldung enthält den Bahnhof, das Gleis, die Zugnummern der beiden betroffenen Zugverbindungen und den Grund (Verspätungsfolgen, beitragende Verspätung oder planmäßige Überlappung).
DB Timetables API (plan + fchg)  ─┐
                                   ├─► EventProducer ─► Event-Bus ─► StreamDetector ─► Dashboard
DB StaDa API (ALE-Katalog)       ─┘                    (Redis / lokal)  (15-Min.-Fenster)


- Produzent: Rotiert die Anfragen an die DB-API unter Einhaltung des Rate-Limits des Free-Tarifs (60 Anfragen/Minute) und veröffentlicht Ereignisse vom Typ ocupacion / retraso / cancelacion (Belegung / Verspätung / Ausfall).
- Detektor: Verwendet gleitende Zeitfenster je (Bahnhof, Gleis) und unterscheidet zwischen Konflikt (tatsächliche Überschneidung der Gleisbelegungen) und Risiko (Zeitabstand unter 3 Minuten).
- Auditierung: PostgreSQL speichert den Verlauf der Warnmeldungen außerhalb des kritischen Verarbeitungspfads. Optional kann die Auditierung mit AUDIT_ENABLED=0 deaktiviert werden.
# 🧩 Deployment A — Streamlit Cloud (Dashboard)
Die gesamte Pipeline läuft innerhalb des Streamlit-Prozesses. Als Event-Bus dient eine lokale Implementierung (app/bus.py), die dieselbe API wie Redis bereitstellt.
1. Main Module Path: app/main.py (ist bereits standardmäßig eingestellt).
2. Settings → Secrets (erforderlich für den Zugriff auf echte Daten):
   DB_CLIENT_ID = "deine-client-id"
   DB_API_KEY   = "dein-api-key"
   
   
   Die Zugangsdaten erhältst du unter  durch die Anmeldung für die Timetables API und die Station Data API. Ohne diese Zugangsdaten weist das Dashboard darauf hin, dass keine Daten verfügbar sind.
3. Codeänderungen: Werden nach einem Push automatisch übernommen.
Die requirements.txt im Stammverzeichnis installiert ausschließlich streamlit und requests. psycopg2 wird bewusst ausgeschlossen, da für Python 3.13 und neuere Versionen kein passendes Wheel verfügbar ist und die Bereitstellung andernfalls aufgrund der fehlenden ausführbaren Datei pg_config fehlschlägt.

# 📁 Deployment B — Docker (FastAPI + SSE)
cp .env.example .env      # DB-API-Zugangsdaten
docker compose up -d --build
# Dashboard: http://localhost:8000


- app/api.py: FastAPI-API (uvicorn app.api:app) mit SSE und PostgreSQL-Auditierung.
- app/static/index.html: HTML-Dashboard mit SSE (Push statt Polling).
- Host-Ports: Anwendung 8000, PostgreSQL 5434, Redis 6380. Bei Konflikten mit anderen lokalen Projekten können die Ports in docker-compose.yml geändert werden.
Endpunkt	Beschreibung
GET /	HTML-Dashboard
GET /api/stream/conflictos	SSE-Stream mit Live-Warnmeldungen
GET /api/conflictos	Verlauf der Warnmeldungen aus PostgreSQL
GET /api/estaciones	StaDa-Katalog mit Regionen und Städten
GET /api/metrics	Metriken des Produzenten und des Detektors
GET /api/health	Status von DB, Redis und Zugangsdaten

# ⚡ Projektstruktur
app/
  main.py        Streamlit-Dashboard (Einstiegspunkt für Streamlit Cloud)
  api.py         FastAPI-API (Docker) mit SSE
  bus.py         Event-Bus: Redis Pub/Sub oder lokaler Speicher
  ingest.py      Produzent: DB-API (Timetables + StaDa), Rate-Limit, Rotation
  detector.py    Stream Processor: gleitende Zeitfenster, Warnstufen, Gleiswechsel
  events.py      Ereignisvertrag (Pub/Sub)
  db.py          Auditierung in PostgreSQL (optional)
  static/        HTML-Dashboard mit SSE
sql/schema.sql   Schema und idempotente Migrationen


# 📊 Umgebungsvariablen
Variable	Standardwert	Beschreibung
DB_CLIENT_ID / DB_API_KEY	—	Zugangsdaten für den DB API Marketplace
DB_RATE_PER_MIN	55	Anfragen pro Minute an die DB-API (Limit: 60)
REDIS_URL	redis://redis:6379/0	Event-Bus
BUS_LOCAL	0	1 = In-Memory-Event-Bus (Streamlit Cloud)
AUDIT_ENABLED	1	0 = PostgreSQL-Auditierung deaktiviert
VENTANA_SEG	900	Gleitendes Zeitfenster (15 Minuten)
UMBRAL_SEGUNDOS	180	Schwellenwert für Risiko (3 Minuten)
HORIZONTE_SEG	1800	Prognosehorizont (30 Minuten)

# 📡 Datenquellen
Deutsche Bahn – DB API Marketplace: Timetables API und Station Data API, Lizenz CC BY 4.0.

Keine simulierten Daten: Wenn keine Zugangsdaten vorhanden sind, zeigt das Dashboard keine Warnmeldungen an.
