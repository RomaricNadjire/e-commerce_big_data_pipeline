import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import Json, execute_values


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger(__name__)

TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "postgres-transaction-sink")

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092",
)

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

REQUIRED_FIELDS = {
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
}


INSERT_SQL = """
    INSERT INTO realtime_transaction_events (
        transaction_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        discount_rate,
        amount,
        country,
        status,
        "timestamp",
        payment_method,
        source,
        event_payload
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (transaction_id) DO NOTHING
"""


def parse_timestamp(value: str) -> datetime:
    """Convertit une date ISO 8601 en datetime avec fuseau si disponible."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        raise ValueError("Le timestamp doit contenir un fuseau horaire.")

    return parsed


def normalize_event(payload: dict) -> tuple:
    """Valide et prépare un événement pour PostgreSQL."""
    missing = REQUIRED_FIELDS - payload.keys()

    if missing:
        raise ValueError(
            f"Champs obligatoires manquants : {sorted(missing)}"
        )

    transaction_id = int(payload["transaction_id"])
    customer_id = int(payload["customer_id"])
    product_id = int(payload["product_id"])
    quantity = int(payload["quantity"])

    if transaction_id <= 0 or customer_id <= 0 or product_id <= 0:
        raise ValueError("Les identifiants doivent être positifs.")

    if quantity <= 0:
        raise ValueError("La quantité doit être positive.")

    discount_rate = Decimal(str(payload["discount_rate"]))
    unit_price = Decimal(str(payload["unit_price"]))
    amount = Decimal(str(payload["amount"]))

    if not discount_rate.is_finite() or not Decimal("0") <= discount_rate <= Decimal("1"):
        raise ValueError("Taux de remise invalide.")

    if not unit_price.is_finite() or unit_price < 0:
        raise ValueError("Prix unitaire invalide.")

    if not amount.is_finite() or amount < 0:
        raise ValueError("Montant invalide.")

    country = str(payload["country"]).upper()

    if len(country) != 2:
        raise ValueError("Le pays doit être un code à deux lettres.")

    timestamp = parse_timestamp(str(payload["timestamp"]))
    source = str(payload.get("source", "realtime"))

    return (
        transaction_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        discount_rate,
        amount,
        country,
        str(payload["status"]),
        timestamp,
        str(payload["payment_method"]),
        source,
        Json(payload),
    )


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    conn = None

    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = False

        log.info(
            "Consumer démarré : topic=%s, groupe=%s, brokers=%s",
            TOPIC,
            GROUP_ID,
            KAFKA_SERVERS,
        )

        for message in consumer:
            try:
                event = normalize_event(message.value)

                with conn.cursor() as cur:
                    cur.execute(INSERT_SQL, event)

                # 1. Persister d'abord l'événement.
                conn.commit()

                # 2. Valider ensuite l'offset Kafka.
                consumer.commit()

                log.info(
                    "Événement traité : transaction_id=%s, "
                    "partition=%s, offset=%s",
                    event[0],
                    message.partition,
                    message.offset,
                )

            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                conn.rollback()
                log.exception(
                    "Message invalide : partition=%s, offset=%s",
                    message.partition,
                    message.offset,
                )

                # En production, envoyer le message vers une DLQ
                # avant de valider son offset. Sans DLQ, il faut
                # décider explicitement comment traiter ce message.
                raise RuntimeError(
                    "Message invalide : consommation arrêtée "
                    "pour éviter de perdre l'événement."
                ) from exc

            except psycopg2.Error:
                conn.rollback()
                log.exception(
                    "Échec PostgreSQL : partition=%s, offset=%s",
                    message.partition,
                    message.offset,
                )

                # Pas de commit Kafka : le message pourra être relu.
                raise

    finally:
        if conn is not None:
            conn.close()

        consumer.close()
        log.info("Consumer arrêté.")


if __name__ == "__main__":
    main()