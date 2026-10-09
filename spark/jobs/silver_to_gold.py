from argparse import ArgumentParser

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def parse_args():
    parser = ArgumentParser(description="Build Gold analytical tables from Silver.")
    parser.add_argument(
        "--silver-path",
        default="hdfs://namenode:8020/data/ecommerce/silver",
    )
    parser.add_argument(
        "--gold-path",
        default="hdfs://namenode:8020/data/ecommerce/gold",
    )
    return parser.parse_args()


def add_cancellation_metrics(df: DataFrame, group_cols: list[str]) -> DataFrame:
    """
    Calcule les KPI de volume et d'annulation.
    Les agrégations utilisent toutes les transactions valides.
    """

    result = (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("total_transactions"),

            F.sum(
                F.when(F.col("status") == "completed", 1).otherwise(0)
            ).alias("completed_transactions"),

            F.sum(
                F.when(F.col("status") == "cancelled", 1).otherwise(0)
            ).alias("cancelled_transactions"),
        )
        .withColumn(
            "cancellation_rate_pct",
            F.round(
                F.when(
                    F.col("total_transactions") > 0,
                    F.col("cancelled_transactions")
                    / F.col("total_transactions")
                    * 100,
                ).otherwise(F.lit(0)),
                2,
            ),
        )
    )

    return result


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Ecommerce-Silver-To-Gold")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    silver = args.silver_path
    gold = args.gold_path

    # ------------------------------------------------------------------
    # 1. Lecture des données Silver
    # ------------------------------------------------------------------

    transactions = spark.read.parquet(
        f"{silver}/transactions"
    )

    customers = spark.read.parquet(
        f"{silver}/customers"
    )

    products = spark.read.parquet(
        f"{silver}/products"
    )

    categories = spark.read.parquet(
        f"{silver}/categories"
    )

    # ------------------------------------------------------------------
    # 2. Enrichissement des transactions
    # ------------------------------------------------------------------

    transactions_enriched = (
        transactions.alias("t")
        .join(
            customers.alias("c"),
            F.col("t.customer_id") == F.col("c.customer_id"),
            "inner",
        )
        .join(
            products.alias("p"),
            F.col("t.product_id") == F.col("p.product_id"),
            "inner",
        )
        .join(
            categories.alias("cat"),
            F.col("p.category_id") == F.col("cat.category_id"),
            "inner",
        )
        .select(
            F.col("t.transaction_id"),
            F.col("t.customer_id"),
            F.col("t.product_id"),
            F.col("p.category_id"),
            F.col("cat.category_name"),
            F.col("p.product_name"),
            F.col("c.segment").alias("customer_segment"),

            F.col("t.quantity"),
            F.col("t.unit_price"),
            F.col("t.discount_rate"),
            F.col("t.amount"),

            F.col("t.country"),
            F.col("t.status"),
            F.col("t.timestamp"),
            F.col("t.payment_method"),
            F.col("t.transaction_date"),
        )
    )

    # ------------------------------------------------------------------
    # 3. Séparation des ventes et des opérations
    # ------------------------------------------------------------------

    completed_sales = transactions_enriched.filter(
        F.col("status") == "completed"
    )

    # ------------------------------------------------------------------
    # 4. Gold - transactions enrichies
    # ------------------------------------------------------------------

    (
        transactions_enriched
        .write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(
            f"{gold}/transactions_enriched"
        )
    )

    # ------------------------------------------------------------------
    # 5. Gold - ventes journalières
    # ------------------------------------------------------------------

    daily_volume = add_cancellation_metrics(
        transactions_enriched,
        ["transaction_date"],
    )

    daily_sales = (
        daily_volume.alias("v")
        .join(
            completed_sales
            .groupBy("transaction_date")
            .agg(
                F.sum("quantity").alias("completed_quantity"),
                F.sum("amount").alias("total_sales_amount"),
                F.round(F.avg("amount"), 2).alias(
                    "avg_completed_transaction_amount"
                ),
                F.countDistinct("customer_id").alias(
                    "unique_completed_customers"
                ),
            )
            .alias("s"),
            ["transaction_date"],
            "left",
        )
        .fillna(
            0,
            subset=[
                "completed_quantity",
                "total_sales_amount",
                "avg_completed_transaction_amount",
                "unique_completed_customers",
            ],
        )
    )

    (
        daily_sales
        .write
        .mode("overwrite")
        .partitionBy("transaction_date")
        .parquet(
            f"{gold}/daily_sales"
        )
    )

    # ------------------------------------------------------------------
    # 6. Gold - analyse par pays
    # ------------------------------------------------------------------

    country_volume = add_cancellation_metrics(
        transactions_enriched,
        ["country"],
    )

    country_sales = (
        completed_sales
        .groupBy("country")
        .agg(
            F.sum("quantity").alias("completed_quantity"),
            F.sum("amount").alias("total_sales_amount"),
            F.round(F.avg("amount"), 2).alias(
                "avg_completed_transaction_amount"
            ),
            F.countDistinct("customer_id").alias(
                "unique_completed_customers"
            ),
        )
    )

    sales_by_country = (
        country_volume
        .join(country_sales, ["country"], "left")
        .fillna(0)
    )

    (
        sales_by_country
        .write
        .mode("overwrite")
        .parquet(
            f"{gold}/sales_by_country"
        )
    )

    # ------------------------------------------------------------------
    # 7. Gold - analyse par catégorie
    # ------------------------------------------------------------------

    category_volume = add_cancellation_metrics(
        transactions_enriched,
        ["category_id", "category_name"],
    )

    category_sales = (
        completed_sales
        .groupBy("category_id", "category_name")
        .agg(
            F.sum("quantity").alias("completed_quantity"),
            F.sum("amount").alias("total_sales_amount"),
            F.round(F.avg("amount"), 2).alias(
                "avg_completed_transaction_amount"
            ),
            F.countDistinct("customer_id").alias(
                "unique_completed_customers"
            ),
        )
    )

    sales_by_category = (
        category_volume
        .join(
            category_sales,
            ["category_id", "category_name"],
            "left",
        )
        .fillna(0)
    )

    (
        sales_by_category
        .write
        .mode("overwrite")
        .parquet(
            f"{gold}/sales_by_category"
        )
    )

    # ------------------------------------------------------------------
    # 8. Gold - analyse par produit
    # ------------------------------------------------------------------

    product_volume = add_cancellation_metrics(
        transactions_enriched,
        ["product_id", "product_name"],
    )

    product_sales = (
        completed_sales
        .groupBy("product_id", "product_name")
        .agg(
            F.sum("quantity").alias("completed_quantity"),
            F.sum("amount").alias("total_sales_amount"),
            F.round(F.avg("amount"), 2).alias(
                "avg_completed_transaction_amount"
            ),
            F.countDistinct("customer_id").alias(
                "unique_completed_customers"
            ),
        )
    )

    sales_by_product = (
        product_volume
        .join(
            product_sales,
            ["product_id", "product_name"],
            "left",
        )
        .fillna(0)
    )

    (
        sales_by_product
        .write
        .mode("overwrite")
        .parquet(
            f"{gold}/sales_by_product"
        )
    )

    # ------------------------------------------------------------------
    # 9. Gold - analyse par segment client
    # ------------------------------------------------------------------

    segment_volume = add_cancellation_metrics(
        transactions_enriched,
        ["customer_segment"],
    )

    segment_sales = (
        completed_sales
        .groupBy("customer_segment")
        .agg(
            F.sum("quantity").alias("completed_quantity"),
            F.sum("amount").alias("total_sales_amount"),
            F.round(F.avg("amount"), 2).alias(
                "avg_completed_transaction_amount"
            ),
            F.countDistinct("customer_id").alias(
                "unique_completed_customers"
            ),
        )
    )

    sales_by_segment = (
        segment_volume
        .join(
            segment_sales,
            ["customer_segment"],
            "left",
        )
        .fillna(0)
    )

    (
        sales_by_segment
        .write
        .mode("overwrite")
        .parquet(
            f"{gold}/sales_by_segment"
        )
    )

    # ------------------------------------------------------------------
    # 10. Quelques informations pour le monitoring
    # ------------------------------------------------------------------

    print("===== GOLD TABLES CREATED =====")
    print(f"Base path: {gold}")
    print(
        "transactions_enriched, daily_sales, "
        "sales_by_country, sales_by_category, "
        "sales_by_product, sales_by_segment"
    )

    spark.stop()


if __name__ == "__main__":
    main()