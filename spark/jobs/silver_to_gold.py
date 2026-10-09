
from argparse import ArgumentParser
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark import StorageLevel


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = ArgumentParser(
        description="Build Gold analytical tables from Silver."
    )
    parser.add_argument(
        "--silver-path",
        default="hdfs://namenode:8020/data/ecommerce/silver",
    )
    parser.add_argument(
        "--gold-path",
        default="hdfs://namenode:8020/data/ecommerce/gold",
    )
    parser.add_argument(
        "--jdbc-partitions",
        type=int,
        default=4,
        help="Nombre de partitions utilisées pour le chargement JDBC.",
    )
    return parser.parse_args()


def add_cancellation_metrics(
    df: DataFrame,
    group_cols: list[str],
) -> DataFrame:
    """Calcule les volumes et le taux d'annulation."""

    return (
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


def write_gold_parquet(
    df: DataFrame,
    path: str,
    partition_cols: list[str] | None = None,
) -> None:
    """Écrit un résultat Gold dans HDFS."""

    writer = df.write.mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)
    logger.info("Parquet Gold écrit : %s", path)


def write_gold_postgres(
    df: DataFrame,
    table: str,
    jdbc_url: str,
    jdbc_properties: dict[str, str],
    jdbc_partitions: int,
) -> None:
    """
    Remplace le snapshot d'une table analytique PostgreSQL.

    Les tables existantes sont tronquées plutôt que supprimées
    lorsque le dialecte JDBC PostgreSQL prend en charge truncate.
    """

    output = df

    if jdbc_partitions > 0:
        output = output.repartition(jdbc_partitions)

    (
        output.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", jdbc_properties["user"])
        .option("password", jdbc_properties["password"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .option("numPartitions", str(max(1, jdbc_partitions)))
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    logger.info("Table PostgreSQL actualisée : %s", table)


def main():
    args = parse_args()

    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "ecommerce")
    postgres_user = os.environ["POSTGRES_USER"]
    postgres_password = os.environ["POSTGRES_PASSWORD"]

    jdbc_url = (
        f"jdbc:postgresql://{postgres_host}:"
        f"{postgres_port}/{postgres_db}"
    )

    jdbc_properties = {
        "user": postgres_user,
        "password": postgres_password,
    }

    spark = (
        SparkSession.builder
        .appName("Ecommerce-Silver-To-Gold")
        .getOrCreate()
    )

    try:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "6")

        silver = args.silver_path.rstrip("/")
        gold = args.gold_path.rstrip("/")

        logger.info("Lecture des données Silver depuis %s", silver)

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

        required_transaction_columns = {
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_rate",
            "amount",
            "country",
            "status",
            "timestamp",
            "payment_method",
            "transaction_date",
        }

        missing = required_transaction_columns - set(
            transactions.columns
        )

        if missing:
            raise ValueError(
                "Colonnes absentes dans Silver/transactions : "
                f"{sorted(missing)}"
            )

        # Contrôle de l'unicité avant tout calcul analytique.
        duplicate_ids = (
            transactions
            .groupBy("transaction_id")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if duplicate_ids:
            raise ValueError(
                "Silver contient des transaction_id dupliqués. "
                "Corriger le traitement Silver avant de calculer Gold."
            )

        # ------------------------------------------------------------------
        # 1. Enrichissement
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
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Matérialise l'enrichissement une seule fois et mesure les pertes
        # éventuelles dues aux jointures inner.
        silver_count = transactions.count()
        enriched_count = transactions_enriched.count()

        logger.info("Transactions Silver : %s", silver_count)
        logger.info("Transactions enrichies : %s", enriched_count)

        if enriched_count < silver_count:
            logger.warning(
                "%s transaction(s) n'ont pas trouvé toutes leurs "
                "références lors de l'enrichissement.",
                silver_count - enriched_count,
            )

        completed_sales = transactions_enriched.filter(
            F.col("status") == "completed"
        )

        # ------------------------------------------------------------------
        # 2. Transactions enrichies
        # ------------------------------------------------------------------

        outputs = []

        outputs.append(
            (
                transactions_enriched,
                f"{gold}/transactions_enriched",
                ["transaction_date"],
                "gold_transactions_enriched",
            )
        )

        # ------------------------------------------------------------------
        # 3. Ventes journalières
        # ------------------------------------------------------------------

        daily_volume = add_cancellation_metrics(
            transactions_enriched,
            ["transaction_date"],
        )

        daily_completed_sales = (
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
        )

        daily_sales = (
            daily_volume
            .join(
                daily_completed_sales,
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

        outputs.append(
            (
                daily_sales,
                f"{gold}/daily_sales",
                ["transaction_date"],
                "gold_daily_sales",
            )
        )

        # ------------------------------------------------------------------
        # 4. Ventes par pays
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

        outputs.append(
            (
                sales_by_country,
                f"{gold}/sales_by_country",
                [],
                "gold_sales_by_country",
            )
        )

        # ------------------------------------------------------------------
        # 5. Ventes par catégorie
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

        outputs.append(
            (
                sales_by_category,
                f"{gold}/sales_by_category",
                [],
                "gold_sales_by_category",
            )
        )

        # ------------------------------------------------------------------
        # 6. Ventes par produit
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

        outputs.append(
            (
                sales_by_product,
                f"{gold}/sales_by_product",
                [],
                "gold_sales_by_product",
            )
        )

        # ------------------------------------------------------------------
        # 7. Ventes par segment client
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

        outputs.append(
            (
                sales_by_segment,
                f"{gold}/sales_by_segment",
                [],
                "gold_sales_by_segment",
            )
        )

        # ------------------------------------------------------------------
        # 8. Écriture des six sorties dans HDFS et PostgreSQL
        # ------------------------------------------------------------------

        for dataframe, hdfs_path, partition_cols, table in outputs:
            logger.info("Traitement de la sortie %s", table)

            write_gold_parquet(
                dataframe,
                hdfs_path,
                partition_cols or None,
            )

            write_gold_postgres(
                dataframe,
                table,
                jdbc_url,
                jdbc_properties,
                args.jdbc_partitions,
            )

        logger.info("===== GOLD TABLES CREATED =====")
        logger.info("Base HDFS : %s", gold)
        logger.info(
            "Tables PostgreSQL : %s",
            ", ".join(item[3] for item in outputs),
        )

    finally:
        if "transactions_enriched" in locals():
            transactions_enriched.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()