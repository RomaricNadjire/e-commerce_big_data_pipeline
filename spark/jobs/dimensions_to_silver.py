import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
)


BASE = "hdfs://namenode:8020/data/ecommerce"

RAW = f"{BASE}/raw"
SILVER = f"{BASE}/silver"
QUARANTINE = f"{BASE}/quarantine"


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--entity",
        required=True,
        choices=["categories", "customers", "products"],
    )

    parser.add_argument(
        "--input",
        required=True,
        help="HDFS path du fichier à traiter",
    )

    return parser.parse_args()


def path_exists(spark, path):
    jvm = spark._jvm

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )

    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


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


def deduplicate_latest(df, key):
    window = (
        Window
        .partitionBy(key)
        .orderBy(
            F.col("ingested_at").desc(),
            F.col("source_file").desc(),
        )
    )

    return (
        df
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def write_quarantine(df, path, reason):
    (
        df
        .withColumn("quality_status", F.lit(reason))
        .write
        .mode("append")
        .parquet(path)
    )


# ============================================================
# CATEGORIES
# ============================================================

def process_categories(spark, input_path):
    schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False),
    ])

    df = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(input_path)
    )

    df = add_metadata(df)

    invalid = df.filter(
        F.col("category_id").isNull()
        | F.col("category_name").isNull()
        | (F.trim(F.col("category_name")) == "")
    )

    valid = (
        df.filter(
            F.col("category_id").isNotNull()
            & F.col("category_name").isNotNull()
            & (F.trim(F.col("category_name")) != "")
        )
        .withColumn(
            "category_name",
            F.trim("category_name"),
        )
    )

    write_quarantine(
        invalid,
        f"{QUARANTINE}/categories/invalid",
        "INVALID",
    )

    valid = deduplicate_latest(
        valid,
        "category_id",
    )

    silver_path = f"{SILVER}/categories"

    if path_exists(spark, silver_path):
        existing = spark.read.parquet(silver_path)

        valid = (
            existing
            .unionByName(valid)
        )

        valid = deduplicate_latest(
            valid,
            "category_id",
        )

    (
        valid.write
        .mode("overwrite")
        .parquet(silver_path)
    )


# ============================================================
# CUSTOMERS
# ============================================================

def process_customers(spark, input_path):
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("country", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("signup_date", DateType(), False),
    ])

    df = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(input_path)
    )

    df = add_metadata(df)

    invalid = df.filter(
        F.col("customer_id").isNull()
        | F.col("country").isNull()
        | F.col("segment").isNull()
        | F.col("signup_date").isNull()
        | (F.length(F.trim("country")) != 2)
    )

    valid = (
        df.filter(
            F.col("customer_id").isNotNull()
            & F.col("country").isNotNull()
            & F.col("segment").isNotNull()
            & F.col("signup_date").isNotNull()
            & (F.length(F.trim("country")) == 2)
        )
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("segment", F.lower(F.trim("segment")))
    )

    write_quarantine(
        invalid,
        f"{QUARANTINE}/customers/invalid",
        "INVALID",
    )

    valid = deduplicate_latest(
        valid,
        "customer_id",
    )

    silver_path = f"{SILVER}/customers"

    if path_exists(spark, silver_path):
        existing = spark.read.parquet(silver_path)

        valid = (
            existing
            .unionByName(valid)
        )

        valid = deduplicate_latest(
            valid,
            "customer_id",
        )

    (
        valid.write
        .mode("overwrite")
        .parquet(silver_path)
    )


# ============================================================
# PRODUCTS
# ============================================================

def process_products(spark, input_path):
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("unit_price", DoubleType(), False),
    ])

    df = (
        spark.read
        .schema(schema)
        .option("header", "true")
        .csv(input_path)
    )

    df = add_metadata(df)

    invalid = df.filter(
        F.col("product_id").isNull()
        | F.col("category_id").isNull()
        | F.col("product_name").isNull()
        | (F.trim("product_name") == "")
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
    )

    valid = (
        df.filter(
            F.col("product_id").isNotNull()
            & F.col("category_id").isNotNull()
            & F.col("product_name").isNotNull()
            & (F.trim("product_name") != "")
            & F.col("unit_price").isNotNull()
            & (F.col("unit_price") >= 0)
        )
        .withColumn(
            "product_name",
            F.trim("product_name"),
        )
    )

    write_quarantine(
        invalid,
        f"{QUARANTINE}/products/invalid",
        "INVALID",
    )

    # Vérification contre Silver Categories
    categories_path = f"{SILVER}/categories"

    if not path_exists(spark, categories_path):
        write_quarantine(
            valid,
            f"{QUARANTINE}/products/waiting_reference",
            "WAITING_FOR_CATEGORIES",
        )
        return

    categories = (
        spark.read.parquet(categories_path)
        .select("category_id")
        .dropDuplicates()
    )

    missing_categories = (
        valid
        .join(
            categories,
            on="category_id",
            how="left_anti",
        )
    )

    valid = (
        valid
        .join(
            categories,
            on="category_id",
            how="left_semi",
        )
    )

    write_quarantine(
        missing_categories,
        f"{QUARANTINE}/products/waiting_reference",
        "WAITING_FOR_CATEGORIES",
    )

    valid = deduplicate_latest(
        valid,
        "product_id",
    )

    silver_path = f"{SILVER}/products"

    if path_exists(spark, silver_path):
        existing = spark.read.parquet(silver_path)

        valid = (
            existing
            .unionByName(valid)
        )

        valid = deduplicate_latest(
            valid,
            "product_id",
        )

    (
        valid.write
        .mode("overwrite")
        .parquet(silver_path)
    )


# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"DimensionsToSilver_{args.entity}")
        .getOrCreate()
    )

    print(f"=== Processing {args.entity} ===")
    print(f"Input: {args.input}")

    if args.entity == "categories":
        process_categories(
            spark,
            args.input,
        )

    elif args.entity == "customers":
        process_customers(
            spark,
            args.input,
        )

    elif args.entity == "products":
        process_products(
            spark,
            args.input,
        )

    print("=== Processing completed ===")

    spark.stop()


if __name__ == "__main__":
    main()