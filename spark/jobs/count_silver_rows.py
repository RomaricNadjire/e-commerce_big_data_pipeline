from __future__ import annotations

from argparse import ArgumentParser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_TRANSACTIONS_PATH = (
    "hdfs://namenode:8020/data/ecommerce/silver/transactions"
)


def parse_args():
    parser = ArgumentParser(
        description="Count Silver transactions for a source file."
    )

    parser.add_argument(
        "--source-file",
        required=True,
        help="Original RAW filename.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Count-Silver-Rows")
        .getOrCreate()
    )

    try:
        transactions = spark.read.parquet(
            SILVER_TRANSACTIONS_PATH
        )

        rows_processed = (
            transactions
            .filter(
                F.col("source_file") == args.source_file
            )
            .count()
        )

        if rows_processed <= 0:
            raise RuntimeError(
                f"No Silver rows found for source_file="
                f"{args.source_file}"
            )

        print(f"ROWS_PROCESSED={rows_processed}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()