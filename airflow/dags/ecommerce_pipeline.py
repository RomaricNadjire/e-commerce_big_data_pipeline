from __future__ import annotations

from pathlib import Path
import re

import subprocess
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator


HDFS_TRANSACTIONS_PATH = "/data/ecommerce/raw/transactions"
HDFS_URI_PREFIX = "hdfs://namenode:8020"

DATASET = "transactions"
POSTGRES_CONN_ID = "postgres_ecommerce"


def list_hdfs_transactions() -> list[str]:
    """
    Retourne les fichiers CSV présents dans le répertoire
    HDFS des transactions.
    """

    command = [
        "docker",
        "exec",
        "training_namenode",
        "hdfs",
        "dfs",
        "-ls",
        HDFS_TRANSACTIONS_PATH,
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
    )

    files = []

    for line in result.stdout.splitlines():
        parts = line.split()

        if len(parts) < 8:
            continue

        path = parts[-1]

        if path.endswith(".csv"):
            files.append(path)

    return sorted(files)


def get_processed_files() -> set[str]:
    """
    Retourne les fichiers transactions déjà traités avec succès.
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    rows = hook.get_records(
        """
        SELECT filename
        FROM processed_files
        WHERE dataset = %s
          AND status = 'SUCCESS'
        """,
        parameters=(DATASET,),
    )

    return {row[0] for row in rows}


@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "data-engineering"],
)
def ecommerce_pipeline():

    @task
    def detect_new_transactions() -> str:
        """
        Cherche le prochain fichier HDFS non encore traité.
        """

        hdfs_files = list_hdfs_transactions()
        processed_files = get_processed_files()

        for hdfs_path in hdfs_files:

            filename = hdfs_path.rsplit("/", 1)[-1]

            if filename not in processed_files:
                full_hdfs_path = f"{HDFS_URI_PREFIX}{hdfs_path}"

                print(f"Nouveau fichier détecté : {full_hdfs_path}")

                return full_hdfs_path

        raise AirflowSkipException(
            "Aucun nouveau fichier transactionnel à traiter."
        )

    input_file = detect_new_transactions()
    
    transactions_to_silver = BashOperator(
        task_id="transactions_to_silver",

        bash_command="""
        docker exec training_spark_master \
            spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark/jobs/transactions_to_silver.py \
            --input "{{ ti.xcom_pull(
                task_ids='detect_new_transactions'
            ) }}"
        """,

        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    quality_check_silver = BashOperator(
        task_id="quality_check_silver",
        bash_command="""
        docker exec training_spark_master \
            spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark/jobs/quality_checks.py \
            --layer silver
        """,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",

        bash_command="""
        docker exec training_spark_master \
            spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark/jobs/silver_to_gold.py
        """,

        retries=2,
        retry_delay=timedelta(minutes=2),
    )
    
    quality_check_gold = BashOperator(
        task_id="quality_check_gold",
        bash_command="""
        docker exec training_spark_master \
            spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark/jobs/quality_checks.py \
            --layer gold
        """,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    
    @task
    def mark_file_as_processed(input_file: str):

        filename = Path(input_file).name

        result = subprocess.run(
            [
                "docker",
                "exec",
                "training_spark_master",
                "spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/opt/spark/jobs/count_silver_rows.py",
                "--source-file",
                filename,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        rows_processed = None

        for line in result.stdout.splitlines():
            if line.startswith("ROWS_PROCESSED="):
                rows_processed = int(
                    line.split("=", 1)[1]
                )
                break

        if rows_processed is None:
            raise RuntimeError(
                "Unable to retrieve rows_processed from Spark."
            )

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        hook.run(
            """
            INSERT INTO processed_files (
                dataset,
                filename,
                processed_at,
                status,
                rows_processed
            )
            VALUES (
                %s,
                %s,
                CURRENT_TIMESTAMP,
                'SUCCESS',
                %s
            )
            ON CONFLICT (dataset, filename)
            DO UPDATE SET
                processed_at = CURRENT_TIMESTAMP,
                status = 'SUCCESS',
                rows_processed = EXCLUDED.rows_processed
            """,
            parameters=(
                "transactions",
                filename,
                rows_processed,
            ),
        )

        print(
            f"File processed successfully: {filename}"
        )
        print(
            f"Rows written to Silver: {rows_processed}"
        )
    
    mark_processed = mark_file_as_processed(input_file)
    
    (
        input_file 
        >> transactions_to_silver 
        >> quality_check_silver 
        >> silver_to_gold 
        >> quality_check_gold 
        >> mark_processed
    )

ecommerce_pipeline()