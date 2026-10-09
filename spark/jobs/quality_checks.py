from __future__ import annotations

from argparse import ArgumentParser

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "hdfs://namenode:8020/data/ecommerce/silver"
GOLD_PATH = "hdfs://namenode:8020/data/ecommerce/gold"


def parse_args():
    parser = ArgumentParser(description="E-commerce data quality checks")
    parser.add_argument(
        "--layer",
        required=True,
        choices=["silver", "gold"],
    )
    return parser.parse_args()


def assert_non_empty(df, name: str):
    if df.limit(1).count() == 0:
        raise RuntimeError(f"[{name}] dataset is empty")


def assert_zero(value: int, message: str):
    if value != 0:
        raise RuntimeError(f"{message} -> {value}")


def check_silver(spark: SparkSession):
    print("========== SILVER QUALITY CHECK ==========")

    categories = spark.read.parquet(f"{SILVER_PATH}/categories")
    customers = spark.read.parquet(f"{SILVER_PATH}/customers")
    products = spark.read.parquet(f"{SILVER_PATH}/products")
    transactions = spark.read.parquet(f"{SILVER_PATH}/transactions")

    # ----------------------------------------------------------
    # Dimensions
    # ----------------------------------------------------------

    assert_non_empty(categories, "Silver categories")
    assert_non_empty(customers, "Silver customers")
    assert_non_empty(products, "Silver products")
    assert_non_empty(transactions, "Silver transactions")

    # Categories
    duplicate_categories = (
        categories
        .groupBy("category_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    assert_zero(
        duplicate_categories,
        "Duplicate category_id detected",
    )

    # Customers
    duplicate_customers = (
        customers
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    assert_zero(
        duplicate_customers,
        "Duplicate customer_id detected",
    )

    # Products
    duplicate_products = (
        products
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    assert_zero(
        duplicate_products,
        "Duplicate product_id detected",
    )

    invalid_product_category = (
        products.alias("p")
        .join(
            categories.select("category_id").alias("c"),
            F.col("p.category_id") == F.col("c.category_id"),
            "left_anti",
        )
        .limit(1)
        .count()
    )

    assert_zero(
        invalid_product_category,
        "Products referencing unknown category detected",
    )

    # ----------------------------------------------------------
    # Transactions
    # ----------------------------------------------------------

    duplicate_transactions = (
        transactions
        .groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    assert_zero(
        duplicate_transactions,
        "Duplicate transaction_id detected",
    )

    invalid_transaction_fields = transactions.filter(
        F.col("transaction_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("quantity").isNull()
        | F.col("amount").isNull()
        | F.col("timestamp").isNull()
        | (F.col("quantity") <= 0)
        | (F.col("amount") < 0)
        | (F.col("discount_rate") < 0)
        | (F.col("discount_rate") > 1)
    ).limit(1).count()

    assert_zero(
        invalid_transaction_fields,
        "Invalid transaction fields detected",
    )

    invalid_status = (
        transactions
        .filter(
            ~F.col("status").isin("completed", "cancelled")
        )
        .limit(1)
        .count()
    )

    assert_zero(
        invalid_status,
        "Invalid transaction status detected",
    )

    missing_customers = (
        transactions.alias("t")
        .join(
            customers.select("customer_id").alias("c"),
            F.col("t.customer_id") == F.col("c.customer_id"),
            "left_anti",
        )
        .limit(1)
        .count()
    )

    assert_zero(
        missing_customers,
        "Transactions referencing unknown customers detected",
    )

    missing_products = (
        transactions.alias("t")
        .join(
            products.select("product_id").alias("p"),
            F.col("t.product_id") == F.col("p.product_id"),
            "left_anti",
        )
        .limit(1)
        .count()
    )

    assert_zero(
        missing_products,
        "Transactions referencing unknown products detected",
    )

    print("Silver quality checks: PASSED")


def check_gold(spark: SparkSession):
    print("========== GOLD QUALITY CHECK ==========")

    transactions = spark.read.parquet(
        f"{GOLD_PATH}/transactions_enriched"
    )

    daily_sales = spark.read.parquet(
        f"{GOLD_PATH}/daily_sales"
    )

    sales_by_country = spark.read.parquet(
        f"{GOLD_PATH}/sales_by_country"
    )

    sales_by_category = spark.read.parquet(
        f"{GOLD_PATH}/sales_by_category"
    )

    sales_by_product = spark.read.parquet(
        f"{GOLD_PATH}/sales_by_product"
    )

    sales_by_segment = spark.read.parquet(
        f"{GOLD_PATH}/sales_by_segment"
    )

    # ----------------------------------------------------------
    # Non-empty
    # ----------------------------------------------------------

    for df, name in [
        (transactions, "Gold transactions_enriched"),
        (daily_sales, "Gold daily_sales"),
        (sales_by_country, "Gold sales_by_country"),
        (sales_by_category, "Gold sales_by_category"),
        (sales_by_product, "Gold sales_by_product"),
        (sales_by_segment, "Gold sales_by_segment"),
    ]:
        assert_non_empty(df, name)

    # ----------------------------------------------------------
    # Daily business consistency
    # ----------------------------------------------------------

    inconsistent_daily = daily_sales.filter(
        F.col("completed_transactions")
        + F.col("cancelled_transactions")
        != F.col("total_transactions")
    ).limit(1).count()

    assert_zero(
        inconsistent_daily,
        "Daily transaction totals are inconsistent",
    )

    invalid_cancellation_rate = daily_sales.filter(
        (F.col("cancellation_rate_pct") < 0)
        | (F.col("cancellation_rate_pct") > 100)
    ).limit(1).count()

    assert_zero(
        invalid_cancellation_rate,
        "Invalid cancellation rate detected",
    )

    negative_sales = daily_sales.filter(
        F.col("total_sales_amount") < 0
    ).limit(1).count()

    assert_zero(
        negative_sales,
        "Negative sales amount detected",
    )

    negative_quantity = daily_sales.filter(
        F.col("completed_quantity") < 0
    ).limit(1).count()

    assert_zero(
        negative_quantity,
        "Negative completed quantity detected",
    )

    # ----------------------------------------------------------
    # Global reconciliation
    # ----------------------------------------------------------

    enriched_total = transactions.count()

    completed_total = transactions.filter(
        F.col("status") == "completed"
    ).count()

    cancelled_total = transactions.filter(
        F.col("status") == "cancelled"
    ).count()

    gold_transaction_total = daily_sales.agg(
        F.sum("total_transactions")
    ).first()[0]

    gold_completed_total = daily_sales.agg(
        F.sum("completed_transactions")
    ).first()[0]

    gold_cancelled_total = daily_sales.agg(
        F.sum("cancelled_transactions")
    ).first()[0]

    if gold_transaction_total != enriched_total:
        raise RuntimeError(
            "Daily totals do not reconcile with transactions_enriched"
        )

    if gold_completed_total != completed_total:
        raise RuntimeError(
            "Completed totals do not reconcile"
        )

    if gold_cancelled_total != cancelled_total:
        raise RuntimeError(
            "Cancelled totals do not reconcile"
        )

    # ----------------------------------------------------------
    # Sales reconciliation
    # ----------------------------------------------------------

    transaction_sales = (
        transactions
        .filter(F.col("status") == "completed")
        .agg(F.sum("amount"))
        .first()[0]
        or 0
    )

    gold_sales = (
        daily_sales
        .agg(F.sum("total_sales_amount"))
        .first()[0]
        or 0
    )

    if abs(float(transaction_sales) - float(gold_sales)) > 0.01:
        raise RuntimeError(
            f"Sales reconciliation failed: "
            f"transactions={transaction_sales}, "
            f"daily_sales={gold_sales}"
        )

    print("Gold quality checks: PASSED")


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"Ecommerce-Quality-{args.layer}")
        .getOrCreate()
    )

    try:
        if args.layer == "silver":
            check_silver(spark)
        else:
            check_gold(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()