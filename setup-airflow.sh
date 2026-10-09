#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# Training Project - Airflow configuration
# Usage:
#   source ./setup-airflow.sh
#   ./setup-airflow.sh init
#   ./setup-airflow.sh start
#   ./setup-airflow.sh test
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AIRFLOW_HOME_DIR="${PROJECT_ROOT}/airflow"
AIRFLOW_CONFIG_FILE="${AIRFLOW_HOME_DIR}/airflow.cfg"
DAGS_FOLDER="${AIRFLOW_HOME_DIR}/dags"

VENV_DIR="${PROJECT_ROOT}/.venv"
AIRFLOW_BIN="${VENV_DIR}/bin/airflow"

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------

export AIRFLOW_HOME="${AIRFLOW_HOME_DIR}"
export AIRFLOW_CONFIG="${AIRFLOW_CONFIG_FILE}"

export AIRFLOW__CORE__DAGS_FOLDER="${DAGS_FOLDER}"
export AIRFLOW__CORE__LOAD_EXAMPLES="false"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"

# DAGs created in this project are not paused automatically.
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="false"

# Python 3.14 changed the Unix multiprocessing default.
# Keep the behaviour expected by this local training environment.
export AIRFLOW__CORE__MP_START_METHOD="fork"

# ------------------------------------------------------------
# Directories
# ------------------------------------------------------------

mkdir -p "${AIRFLOW_HOME_DIR}"
mkdir -p "${DAGS_FOLDER}"
mkdir -p "${AIRFLOW_HOME_DIR}/logs"
mkdir -p "${AIRFLOW_HOME_DIR}/plugins"

# ------------------------------------------------------------
# Virtual environment
# ------------------------------------------------------------

if [[ ! -f "${VENV_DIR}/bin/activate" ]]; then
    echo "❌ Virtual environment not found:"
    echo "   ${VENV_DIR}"
    echo
    echo "Create it first with:"
    echo "   python3 -m venv .venv"
    exit 1
fi

# Activate virtual environment.
source "${VENV_DIR}/bin/activate"

if [[ ! -x "${AIRFLOW_BIN}" ]]; then
    echo "❌ Airflow executable not found:"
    echo "   ${AIRFLOW_BIN}"
    echo
    echo "Install Airflow in .venv first."
    exit 1
fi

echo
echo "=========================================="
echo " Airflow environment configured"
echo "=========================================="
echo "Project root : ${PROJECT_ROOT}"
echo "AIRFLOW_HOME : ${AIRFLOW_HOME}"
echo "Config       : ${AIRFLOW_CONFIG}"
echo "DAGs         : ${AIRFLOW__CORE__DAGS_FOLDER}"
echo "Executor     : ${AIRFLOW__CORE__EXECUTOR}"
echo "Examples     : ${AIRFLOW__CORE__LOAD_EXAMPLES}"
echo

# ------------------------------------------------------------
# Commands
# ------------------------------------------------------------

COMMAND="${1:-env}"

case "${COMMAND}" in

    env)
        echo "Airflow version:"
        airflow version
        echo
        echo "DAG folder:"
        airflow config get-value core dags_folder
        ;;

    init)
        echo "🔧 Migrating Airflow metadata database..."
        airflow db migrate

        echo
        echo "✅ Airflow initialization completed."
        ;;

    start)
        echo "🔧 Migrating Airflow metadata database..."
        airflow db migrate

        echo
        echo "🚀 Starting Airflow standalone..."
        echo

        exec airflow standalone
        ;;

    test)
        echo "🧪 Testing ecommerce_pipeline..."

        airflow dags test ecommerce_pipeline "$(date +%Y-%m-%d)"
        ;;

    list)
        airflow dags list
        ;;

    *)
        echo "Usage:"
        echo "  source ./setup-airflow.sh"
        echo "  ./setup-airflow.sh init"
        echo "  ./setup-airflow.sh start"
        echo "  ./setup-airflow.sh test"
        echo "  ./setup-airflow.sh list"
        exit 1
        ;;

esac