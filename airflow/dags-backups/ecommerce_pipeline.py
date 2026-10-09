from pathlib import Path

import pendulum
import pandas as pd

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import random


BASE_DIR = Path(
    "/home/romaric/Documents/training/AirFlow/training_project"
)

RAW_FILE = BASE_DIR / "data/raw/transactions.csv"
PROCESSED_FILE = BASE_DIR / "data/processed/transactions_clean.csv"
STATS_FILE = BASE_DIR / "data/processed/statistics.csv"


@dag(
    dag_id="ecommerce_transactions",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["training", "data-engineering"],
)
def ecommerce_pipeline():

    @task
    def check_file():
        if not RAW_FILE.exists():
            raise FileNotFoundError(
                f"Input file not found: {RAW_FILE}"
            )

        print(f"Input file found: {RAW_FILE}")

    @task
    def clean_data():
        df = pd.read_csv(RAW_FILE)

        rows_before = len(df)

        df = df.drop_duplicates()
        df = df.dropna()
        df = df[df["amount"] > 0]

        df.to_csv(PROCESSED_FILE, index=False)

        rows_after = len(df)

        print(f"Rows before cleaning: {rows_before}")
        print(f"Rows after cleaning: {rows_after}")

        return rows_after
        
    @task
    def load_data():

        df = pd.read_csv(PROCESSED_FILE)

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        rows = [
            (
                int(row.transaction_id),
                int(row.customer_id),
                int(row.product_id),
                float(row.amount),
                row.country,
                row.timestamp,
            )
            for row in df.itertuples(index=False)
        ]

        hook.insert_rows(
            table="transactions",
            rows=rows,
            target_fields=[
                "transaction_id",
                "customer_id",
                "product_id",
                "amount",
                "country",
                "timestamp",
            ],
        )

        print(f"{len(rows)} rows inserted into PostgreSQL.") 
    
    @task
    def calculate_statistics(rows_after):
        df = pd.read_csv(PROCESSED_FILE)

        total_revenue = df["amount"].sum()
        average_transaction = df["amount"].mean()

        print(f"Rows after cleaning: {rows_after}")
        print(f"Total revenue: {total_revenue:.2f}")
        print(f"Average transaction: {average_transaction:.2f}")
    
    @task
    def save_results():
        print(f"Statistics saved to: {STATS_FILE}")

    @task
    def quality_check():
        df = pd.read_csv(PROCESSED_FILE)

        if df.empty:
            raise ValueError("Data quality check failed: dataset is empty")

        if (df["amount"] <= 0).any():
            raise ValueError(
                "Data quality check failed: invalid amount detected"
            )

        print("Data quality check passed.")
    
    # @task(retries=2, retry_delay=pendulum.duration(seconds=10))
    # def unstable_task():
    #     print("Tentative d'exécution...")
    #     if  random.random(1) < 0.5:
    #         raise Exception("Erreur volontaire pour tester Airflow") 
    #     print("Exécution réussie.")

    create_table = SQLExecuteQueryOperator(
        task_id="create_transactions_table",
        conn_id="postgres_ecommerce",
        sql="""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                country VARCHAR(2) NOT NULL,
                timestamp TIMESTAMP NOT NULL
            );
        """,
    )

    check = check_file()
    clean = clean_data()
    stats = calculate_statistics(clean)
    load = load_data()
    save = save_results()
    quality = quality_check()
    # unstable = unstable_task()

    check >> clean >> create_table >> load >> stats >> save >> quality


ecommerce_pipeline()