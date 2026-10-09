from datetime import datetime, timezone
from decimal import Decimal
from fastapi import HTTPException
import os
import logging
import requests
from decimal import Decimal
from pydantic import BaseModel
from itertools import count

from app.schemas import TransactionCreate, TransactionEvent, CustomerOut, ProductOut

_id_counter = count(1)

log = logging.getLogger(__name__)

NIFI_URL = os.getenv("NIFI_URL", "http://nifi:8085/api/transactions")
NIFI_TIMEOUT = float(os.getenv("NIFI_TIMEOUT", "3"))


def send_to_nifi(event: TransactionEvent) -> None:
    """
    POST the transaction event as JSON to a NiFi ListenHTTP endpoint.
    Non-fatal: logs on failure so a NiFi outage doesn't break the API.
    """
    # Pydantic v2 → JSON-safe dict (Decimal → str, datetime → ISO)
    payload = event.model_dump(mode="json")

    try:
        r = requests.post(
            NIFI_URL,
            json=payload,
            timeout=NIFI_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()
        log.info("Sent transaction %s to NiFi (%s)", event.transaction_id, r.status_code)
        return r.status_code
    except requests.RequestException as e:
        log.error("Failed to send transaction %s to NiFi: %s", event.transaction_id, e)
        
def persist_transaction(event: TransactionEvent) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions (
                    transaction_id, customer_id, product_id, quantity,
                    unit_price, discount_rate, amount, country,
                    status, timestamp, payment_method, source_file
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (
                    event.transaction_id,
                    event.customer_id,
                    event.product_id,
                    event.quantity,
                    event.unit_price,
                    event.discount_rate,
                    event.amount,
                    event.country,
                    event.status,
                    event.timestamp,
                    event.payment_method,
                    None,   # source_file
                ),
            )
            conn.commit()
    finally:
        conn.close()

def create_transaction(data: TransactionCreate) -> TransactionEvent:
    unit_price = get_product_price(data.product_id)
    if unit_price is None:
        raise HTTPException(404, f"Product {data.product_id} not found")
    
    # check customer existence
    if not get_customer(data.customer_id):
        raise HTTPException(404, f"Customer {data.customer_id} not found")

    amount = (
        Decimal(data.quantity)
        * unit_price
        * (Decimal("1") - data.discount_rate)
    ).quantize(Decimal("0.01"))

    event = TransactionEvent(
        transaction_id=generate_transaction_id(),
        customer_id=data.customer_id,
        product_id=data.product_id,
        quantity=data.quantity,
        unit_price=unit_price,
        discount_rate=data.discount_rate,
        amount=amount,
        country=data.country.upper(),
        status="completed",
        payment_method=data.payment_method,
        timestamp=datetime.now(timezone.utc),
        source="realtime",
    )

    # persist first, then forward
    persist_transaction(event)
    status = send_to_nifi(event)
    
    if status not in [200, 201, 202]:
        raise HTTPException(500, f"Failed to forward transaction to NiFi, status code: {status}")
    
    return event


def generate_transaction_id() -> int:
    """
    Generate a unique transaction ID.
    """
    
    return next(_id_counter)


def get_conn():
    """
    Get a connection to the PostgreSQL database.
    """
    
    try:
        import psycopg2
        return psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"), # from .env
            user=os.getenv("POSTGRES_USER"), # from .env
            password=os.getenv("POSTGRES_PASSWORD"), # from .env
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432") # from .env or default to 5432
        )
    except ImportError:
        raise ImportError("psycopg2 is required to connect to the PostgreSQL database")


def get_product_price(product_id: int) -> Decimal | None:
    """
    Get the price of a product from the PostgreSQL database.
    """
    
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT unit_price FROM products WHERE product_id = %s", (product_id,))
            row = cur.fetchone()
            return Decimal(str(row[0])) if row else None
    finally:
        conn.close()


def get_customers() -> list[CustomerOut]: 
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT customer_id as id, country, segment, signup_date FROM customers")
            rows = cur.fetchall()
            return [CustomerOut(id=row[0], country=row[1], segment=row[2], signup_date=row[3]) for row in rows]
    finally:
        conn.close()

def get_customer(customer_id: int) -> CustomerOut | None:
    """Get a single customer by ID from the PostgreSQL database."""
    conn = get_conn()
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT customer_id as id, country, segment, signup_date FROM customers WHERE customer_id = %s", (customer_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return CustomerOut(id=row[0], country=row[1], segment=row[2], signup_date=row[3]) if row else None
    except Exception as e:
        raise RuntimeError(f"Failed to fetch customers: {e}")


def get_products() -> list[ProductOut]: 
    """Get the list of products from the PostgreSQL database."""
    
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT product_id as id, product_name as name, unit_price as price FROM products")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [ProductOut(id=row[0], name=row[1], price=Decimal(str(row[2]))) for row in rows]
    except Exception as e:
        raise RuntimeError(f"Failed to fetch products: {e}")