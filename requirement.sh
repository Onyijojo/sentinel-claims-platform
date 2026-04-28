#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Local Airflow setup for the Sentinel Claims Analytics pipeline.
#
# Usage:
#   bash requirement.sh         # one-time setup (install + db migrate + admin user)
#   bash requirement.sh start   # launches webserver + scheduler in the background
#
# Notes:
#   - Run inside a virtualenv. apache-airflow has many transitive deps.
#   - `airflow db init` is deprecated; we use `airflow db migrate` (Airflow 2.7+).
#   - The default admin password below is for LOCAL DEV ONLY. Change it before
#     exposing the UI to anyone else.
# ----------------------------------------------------------------------------
set -euo pipefail

AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.9.3}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"   # change for any non-local use
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@example.com}"

setup() {
    echo ">>> Installing apache-airflow==${AIRFLOW_VERSION} with constraints"
    pip install "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "${CONSTRAINT_URL}"

    echo ">>> Installing supporting libraries"
    pip install \
        watchtower \
        boto3 \
        google-api-python-client \
        google-auth \
        python-dotenv

    echo ">>> Running Airflow DB migrations"
    airflow db migrate

    echo ">>> Creating admin user (idempotent)"
    airflow users create \
        --username "${ADMIN_USER}" \
        --firstname Data \
        --lastname Engineer \
        --role Admin \
        --email "${ADMIN_EMAIL}" \
        --password "${ADMIN_PASSWORD}" \
        || echo "User '${ADMIN_USER}' already exists — skipping."

    echo ">>> Setup complete. Run 'bash requirement.sh start' to launch services."
}

start() {
    echo ">>> Starting Airflow webserver on :8080 (logs: webserver.log)"
    nohup airflow webserver --port 8080 > webserver.log 2>&1 &

    echo ">>> Starting Airflow scheduler (logs: scheduler.log)"
    nohup airflow scheduler > scheduler.log 2>&1 &

    echo ">>> Done. UI: http://localhost:8080  (user: ${ADMIN_USER})"
}

case "${1:-setup}" in
    setup) setup ;;
    start) start ;;
    *)
        echo "Usage: bash $0 [setup|start]"
        exit 1
        ;;
esac
