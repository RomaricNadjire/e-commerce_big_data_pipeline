from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("CheckSilver")
    .getOrCreate()
)

base = "hdfs://namenode:8020/data/ecommerce/silver"

transactions = spark.read.parquet(
    f"{base}/transactions"
)

customers = spark.read.parquet(
    f"{base}/customers"
)

products = spark.read.parquet(
    f"{base}/products"
)

categories = spark.read.parquet(
    f"{base}/categories"
)

print("\n=== COUNTS ===")

print("Customers   :", customers.count())
print("Products    :", products.count())
print("Categories  :", categories.count())
print("Transactions:", transactions.count())

print("\n=== TRANSACTION DATES ===")

(
    transactions
    .groupBy("transaction_date")
    .count()
    .orderBy("transaction_date")
    .show()
)

print("\n=== TRANSACTION SCHEMA ===")
transactions.printSchema()

print("\n=== SAMPLE ===")
transactions.show(10, truncate=False)

spark.stop()