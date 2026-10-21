#!/bin/sh
set -e

echo "[entrypoint] Esperando a PostgreSQL en ${DB_HOST:-db}:${DB_PORT:-5432}..."
until pg_isready -h "${DB_HOST:-db}" -p "${DB_PORT:-5432}" -U "${DB_USER:-postgres}" >/dev/null 2>&1; do
    sleep 2
done
echo "[entrypoint] PostgreSQL disponible."

echo "[entrypoint] Aplicando esquema SQL (sql/schema.sql)..."
export PGPASSWORD="${DB_PASSWORD:-postgres}"
psql -h "${DB_HOST:-db}" -p "${DB_PORT:-5432}" -U "${DB_USER:-postgres}" \
     -d "${DB_NAME:-db_andenes}" -f /app/sql/schema.sql
echo "[entrypoint] Esquema aplicado."

echo "[entrypoint] Arrancando la aplicación FastAPI..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
