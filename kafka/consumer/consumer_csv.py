import json
import os

from confluent_kafka import Consumer
from dotenv import load_dotenv
import psycopg
import csv

load_dotenv()

conf = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "group.id": "ecommerce-postgres-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

conn = psycopg.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)

consumer = Consumer(conf)

consumer.subscribe(["transactions"])


try:
    while True:

        msg = consumer.poll(1.0) # Permettra de récupérer les messages du topic "transactions" avec un délai d'attente de 1 seconde

        if msg is None:
            continue

        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        key = msg.key().decode("utf-8") if msg.key() else None
        value = msg.value().decode("utf-8")

        data = csv.DictReader(value.splitlines())  # header line handled automatically, columns can be reordered
        for row in data:
            print(f"CSV row: {row}")
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO transactions (
                        transaction_id,
                        customer_id,
                        product_id,
                        amount,
                        country,
                        timestamp,
                        source_file
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id)
                    DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        product_id = EXCLUDED.product_id,
                        amount = EXCLUDED.amount,
                        country = EXCLUDED.country,
                        timestamp = EXCLUDED.timestamp,
                        source_file = EXCLUDED.source_file;
                """
                cur.execute(
                    sql,
                    (
                        int(row["transaction_id"]),
                        int(row["customer_id"]),
                        int(row["product_id"]),
                        float(row["amount"]),
                        row["country"],
                        row["timestamp"],
                        row.get("source_file"),
                    ),
                )
                conn.commit()
                print(f"Inserted row into database: {row}")

        print(
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={key} "
            f"value={value}"
        )

        consumer.commit(asynchronous=False)

finally:
    consumer.close()