#!/usr/bin/env bash
# Initializes Superset (idempotent), registers the warehouse, imports the bundled dashboards the
# first time (later edits made in the UI are kept) and starts the web server.
set -euo pipefail

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname Superset \
  --lastname Admin \
  --email admin@example.com || true

superset init

python /app/docker/accuweather/import_assets.py \
  || echo "WARNING: could not import the AccuWeather dashboards; see the error above."

exec /usr/bin/run-server.sh
