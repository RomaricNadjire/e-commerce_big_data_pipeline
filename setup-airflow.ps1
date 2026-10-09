# ============================================================
# Training Project - Airflow configuration
#
# Usage:
#   .\setup-airflow.ps1
#   .\setup-airflow.ps1 init
#   .\setup-airflow.ps1 start
#   .\setup-airflow.ps1 test
#   .\setup-airflow.ps1 list
# ============================================================

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Project root
# ------------------------------------------------------------

$ProjectRoot = $PSScriptRoot

$AirflowHome   = Join-Path $ProjectRoot "airflow"
$AirflowConfig = Join-Path $AirflowHome "airflow.cfg"
$DagsFolder    = Join-Path $AirflowHome "dags"

$VenvDir = Join-Path $ProjectRoot ".venv"

# ------------------------------------------------------------
# Airflow executable
# ------------------------------------------------------------

if ($IsWindows) {
    $AirflowBin = Join-Path $VenvDir "Scripts\airflow.exe"
}
else {
    $AirflowBin = Join-Path $VenvDir "bin\airflow"
}

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------

$env:AIRFLOW_HOME = $AirflowHome
$env:AIRFLOW_CONFIG = $AirflowConfig

$env:AIRFLOW__CORE__DAGS_FOLDER = $DagsFolder
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "false"
$env:AIRFLOW__CORE__EXECUTOR = "LocalExecutor"
$env:AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION = "false"
$env:AIRFLOW__CORE__MP_START_METHOD = "fork"

# ------------------------------------------------------------
# Directories
# ------------------------------------------------------------

New-Item -ItemType Directory -Force -Path $AirflowHome | Out-Null
New-Item -ItemType Directory -Force -Path $DagsFolder | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $AirflowHome "logs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $AirflowHome "plugins") | Out-Null

# ------------------------------------------------------------
# Virtual environment
# ------------------------------------------------------------

if (-not (Test-Path $VenvDir)) {
    Write-Host "❌ Virtual environment not found:"
    Write-Host "   $VenvDir"
    Write-Host ""
    Write-Host "Create it first."
    exit 1
}

if (-not (Test-Path $AirflowBin)) {
    Write-Host "❌ Airflow executable not found:"
    Write-Host "   $AirflowBin"
    Write-Host ""
    Write-Host "Install Airflow in the project's virtual environment first."
    exit 1
}

Write-Host ""
Write-Host "=========================================="
Write-Host " Airflow environment configured"
Write-Host "=========================================="
Write-Host "Project root : $ProjectRoot"
Write-Host "AIRFLOW_HOME : $AirflowHome"
Write-Host "Config       : $AirflowConfig"
Write-Host "DAGs         : $DagsFolder"
Write-Host "Executor     : $env:AIRFLOW__CORE__EXECUTOR"
Write-Host "Examples     : $env:AIRFLOW__CORE__LOAD_EXAMPLES"
Write-Host ""

# ------------------------------------------------------------
# Command
# ------------------------------------------------------------

$Command = if ($args.Count -gt 0) {
    $args[0]
}
else {
    "env"
}

switch ($Command) {

    "env" {
        & $AirflowBin version

        Write-Host ""
        Write-Host "DAG folder:"
        & $AirflowBin config get-value core dags_folder
    }

    "init" {
        Write-Host "🔧 Migrating Airflow metadata database..."
        & $AirflowBin db migrate

        Write-Host ""
        Write-Host "✅ Airflow initialization completed."
    }

    "start" {
        Write-Host "🔧 Migrating Airflow metadata database..."
        & $AirflowBin db migrate

        Write-Host ""
        Write-Host "🚀 Starting Airflow standalone..."
        Write-Host ""

        & $AirflowBin standalone
    }

    "test" {
        Write-Host "🧪 Testing ecommerce_pipeline..."

        $Today = Get-Date -Format "yyyy-MM-dd"

        & $AirflowBin dags test ecommerce_pipeline $Today
    }

    "list" {
        & $AirflowBin dags list
    }

    default {
        Write-Host "Usage:"
        Write-Host "  .\setup-airflow.ps1"
        Write-Host "  .\setup-airflow.ps1 init"
        Write-Host "  .\setup-airflow.ps1 start"
        Write-Host "  .\setup-airflow.ps1 test"
        Write-Host "  .\setup-airflow.ps1 list"
        exit 1
    }
}