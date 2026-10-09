from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


SPARK_SUBMIT = (
    "docker exec training_spark_master "
    "spark-submit "
    "--packages org.postgresql:postgresql:42.7.8 "
    "--master spark://spark-master:7077 "
)

RETRY_JOB = "/opt/spark/jobs/transactions_to_silver.py"
GOLD_JOB = "/opt/spark/jobs/silver_to_gold.py"


@dag(
    dag_id="retry_waiting_reference",
    description=(
        "Retraite périodiquement les transactions dont les "
        "références client ou produit étaient indisponibles."
    ),
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ecommerce", "spark", "quarantine", "retry"],
)
def retry_waiting_reference():

    @task
    def start_retry():
        print(
            "Début du retraitement des transactions "
            "en attente de référence."
        )

    retry_transactions = BashOperator(
        task_id="retry_waiting_reference_transactions",
        bash_command=(
            SPARK_SUBMIT
            + RETRY_JOB
            + " --retry-waiting-reference"
        ),
        execution_timeout=timedelta(minutes=45),
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold",
        bash_command=SPARK_SUBMIT + GOLD_JOB,
        execution_timeout=timedelta(minutes=60),
    )

    start = start_retry()

    start >> retry_transactions >> refresh_gold


retry_waiting_reference()