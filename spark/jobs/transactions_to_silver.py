import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)


BASE = "hdfs://namenode:8020/data/ecommerce"

SILVER = f"{BASE}/silver"
QUARANTINE = f"{BASE}/quarantine"


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
    )

    return parser.parse_args()


def add_metadata(df):
    return (
        df
        .withColumn(
            "source_file",
            F.regexp_extract(
                F.input_file_name(),
                r"([^/]+$)",
                1,
            ),
        )
        .withColumn(
            "ingested_at",
            F.current_timestamp(),
        )
    )


def write_quarantine(df, path, status, reason):
    (
        df
        .withColumn(
            "quality_status",
            F.lit(status),
        )
        .withColumn(
            "quality_reason",
            F.lit(reason),
        )
        .write
        .mode("append")
        .parquet(path)
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("TransactionsToSilver")
        .getOrCreate()
    )

    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_rate", DoubleType(), False),
        StructField("amount", DoubleType(), False),
        StructField("country", StringType(), False),
        StructField("status", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("payment_method", StringType(), False),
    ])

    # ========================================================
    # 1. READ ONE BATCH ONLY
    # ========================================================

    print("=== READ TRANSACTION BATCH ===")
    print(f"Input: {args.input}")

    transactions = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .option(
            "timestampFormat",
            "yyyy-MM-dd'T'HH:mm:ss",
        )
        .csv(args.input)
    )

    transactions = add_metadata(transactions)

    # ========================================================
    # 2. BASIC DATA QUALITY
    # ========================================================

    expected_amount = F.round(
        F.col("quantity")
        * F.col("unit_price")
        * (F.lit(1) - F.col("discount_rate")),
        2,
    )

    business_invalid = (
        F.col("transaction_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
        | F.col("discount_rate").isNull()
        | (F.col("discount_rate") < 0)
        | (F.col("discount_rate") > 1)
        | F.col("amount").isNull()
        | (F.col("amount") < 0)
        | F.col("timestamp").isNull()
        | (
            F.abs(
                F.col("amount") - expected_amount
            ) > F.lit(0.01)
        )
    )

    invalid = transactions.filter(
        business_invalid
    )

    valid = transactions.filter(
        ~business_invalid
    )

    print("Invalid transactions:")
    print(invalid.count())

    write_quarantine(
        invalid,
        f"{QUARANTINE}/transactions/invalid",
        "INVALID",
        "Business or structural validation failed",
    )

    # ========================================================
    # 3. LOAD SILVER REFERENCES
    # ========================================================

    customers = (
        spark.read
        .parquet(f"{SILVER}/customers")
        .select("customer_id")
        .dropDuplicates()
    )

    products = (
        spark.read
        .parquet(f"{SILVER}/products")
        .select("product_id")
        .dropDuplicates()
    )

    # ========================================================
    # 4. REFERENCE VALIDATION
    # ========================================================

    checked = (
        valid
        .join(
            customers.withColumn(
                "_customer_exists",
                F.lit(True),
            ),
            on="customer_id",
            how="left",
        )
        .join(
            products.withColumn(
                "_product_exists",
                F.lit(True),
            ),
            on="product_id",
            how="left",
        )
    )

    waiting_reference = checked.filter(
        F.col("_customer_exists").isNull()
        | F.col("_product_exists").isNull()
    )

    valid_transactions = checked.filter(
        F.col("_customer_exists").isNotNull()
        & F.col("_product_exists").isNotNull()
    )

    print("Waiting for references:")
    print(waiting_reference.count())

    write_quarantine(
        waiting_reference.drop(
            "_customer_exists",
            "_product_exists",
        ),
        f"{QUARANTINE}/transactions/waiting_reference",
        "WAITING_FOR_REFERENCE",
        "Customer or product not yet available in Silver",
    )

    # ========================================================
    # 5. PREPARE SILVER
    # ========================================================

    silver_transactions = (
        valid_transactions
        .drop(
            "_customer_exists",
            "_product_exists",
        )
        .withColumn(
            "country",
            F.upper(F.trim("country")),
        )
        .withColumn(
            "status",
            F.lower(F.trim("status")),
        )
        .withColumn(
            "payment_method",
            F.lower(F.trim("payment_method")),
        )
        .withColumn(
            "transaction_date",
            F.to_date("timestamp"),
        )
        .dropDuplicates(["transaction_id"])
    )

    # ========================================================
    # 6. WRITE SILVER
    # ========================================================
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic",
    )

    (
        silver_transactions.write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(
            f"{SILVER}/transactions"
        )
    )

    print("\n=== TRANSACTIONS SILVER CREATED ===")
    silver_transactions.printSchema()

    print(
        f"Valid transactions: "
        f"{silver_transactions.count():,}"
    )

    spark.stop()


if __name__ == "__main__":
    main()