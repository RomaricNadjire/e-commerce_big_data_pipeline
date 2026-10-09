from pathlib import Path

import pandas as pd
import pendulum

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


BASE_DIR = Path(
    "/home/romaric/Documents/training/AirFlow/training_project"
)

RAW_FILE = BASE_DIR / "data/raw/transactions.csv"
PROCESSED_FILE = BASE_DIR / "data/processed/transactions_clean.csv"


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
    
    create_control_table = SQLExecuteQueryOperator(
        task_id="create_pipeline_control_table",
        conn_id="postgres_ecommerce",
        sql="""
            CREATE TABLE IF NOT EXISTS pipeline_control (
                pipeline_name VARCHAR(100) PRIMARY KEY,
                last_processed_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )
    
    create_processed_files_table = SQLExecuteQueryOperator(
        task_id="create_processed_files_table",
        conn_id="postgres_ecommerce",
        sql="""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename VARCHAR(255) PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) NOT NULL,
                rows_processed INTEGER NOT NULL
            );
        """,
    )
    
    @task
    def get_watermark():

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        result = hook.get_first(
            """
            SELECT last_processed_at
            FROM pipeline_control
            WHERE pipeline_name = 'ecommerce_transactions';
            """
        )

        if result is None or result[0] is None:
            print("No previous execution found.")
            return "1970-01-01 00:00:00"

        watermark = result[0]

        print(f"Last processed timestamp: {watermark}")

        return watermark.isoformat(sep=" ")
    
    @task
    def clean_data():

        df = pd.read_csv(RAW_FILE)

        print(f"Rows before cleaning: {len(df)}")

        # Suppression des doublons
        df = df.drop_duplicates()

        # Suppression des valeurs manquantes
        df = df.dropna()

        # Conservation des montants positifs
        df = df[df["amount"] > 0]

        df.to_csv(PROCESSED_FILE, index=False)

        print(f"Rows after cleaning: {len(df)}")
        print(f"Clean file: {PROCESSED_FILE}")

    @task
    def load_data(watermark):

        df = pd.read_csv(PROCESSED_FILE)

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        watermark = pd.Timestamp(watermark)

        new_rows = df[df["timestamp"] > watermark].copy()

        print(f"Watermark: {watermark}")
        print(f"Rows in source: {len(df)}")
        print(f"New rows to process: {len(new_rows)}")

        if new_rows.empty:
            print("No new data to process.")
            return {
                "watermark": watermark.isoformat(sep=" "),
                "rows_processed": 0,
            }

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO transactions (
                transaction_id,
                customer_id,
                product_id,
                amount,
                country,
                timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                amount = EXCLUDED.amount,
                country = EXCLUDED.country,
                timestamp = EXCLUDED.timestamp;
        """

        for row in new_rows.itertuples(index=False):

            cursor.execute(
                sql,
                (
                    int(row.transaction_id),
                    int(row.customer_id),
                    int(row.product_id),
                    float(row.amount),
                    row.country,
                    row.timestamp.to_pydatetime(),
                ),
            )

        conn.commit()

        cursor.close()
        conn.close()

        new_watermark = new_rows["timestamp"].max()

        print(f"New watermark: {new_watermark}")

        return {
            "watermark": new_watermark.isoformat(sep=" "),
            "rows_processed": len(new_rows),
        }
        
    @task
    def quality_check(load_result):
                
        rows_processed = load_result["rows_processed"]
        
        if rows_processed == 0:
            print("No new rows processed. Quality check skipped.")
            return True
        

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        result = hook.get_first(
            """
            SELECT
                COUNT(*),
                MIN(amount),
                MAX(amount)
            FROM transactions;
            """
        )

        total_rows, min_amount, max_amount = result

        print(f"Total rows: {total_rows}")
        print(f"Minimum amount: {min_amount}")
        print(f"Maximum amount: {max_amount}")

        if total_rows == 0:
            raise ValueError("Quality check failed: table is empty.")

        if min_amount <= 0:
            raise ValueError(
                "Quality check failed: invalid amount detected."
            )

        print(
            f"Quality check passed for {rows_processed} processed rows."
        )
        
        return True
    
    @task
    def update_watermark(load_result):

        if load_result is None:
            print("No new data. Watermark remains unchanged.")
            return
        
        if load_result["rows_processed"] == 0:
            print("No new rows processed. Watermark remains unchanged.")
            return
        
        new_watermark = load_result["watermark"]

        hook = PostgresHook(
            postgres_conn_id="postgres_ecommerce"
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO pipeline_control (
                pipeline_name,
                last_processed_at,
                updated_at
            )
            VALUES (
                'ecommerce_transactions',
                %s,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_processed_at = EXCLUDED.last_processed_at,
                updated_at = CURRENT_TIMESTAMP;
        """

        cursor.execute(sql, (new_watermark,))

        conn.commit()

        cursor.close()
        conn.close()

        print(f"Watermark updated to: {new_watermark}")

    check = check_file()
    clean = clean_data()
    watermark = get_watermark()
    load = load_data(watermark)
    quality = quality_check(load)
    updatewatermark = update_watermark(load)

    create_table >> clean
    create_control_table >> watermark
    check >> clean
    clean >> load
    watermark >> load
    load >> quality >> updatewatermark


ecommerce_pipeline()