import csv
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "generated"

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

NB_CUSTOMERS = 100_000
NB_CATEGORIES = 20
NB_PRODUCTS = 5_000
NB_TRANSACTIONS = 1_000_000

COUNTRIES = ["TG", "BJ", "GH", "CI", "SN", "NG", "CM"]

SEGMENTS = [
    "standard",
    "premium",
    "business",
]

PAYMENT_METHODS = [
    "card",
    "mobile_money",
    "bank_transfer",
    "cash",
]

STATUSES = [
    "completed",
    "completed",
    "completed",
    "completed",
    "cancelled",
]

CATEGORY_NAMES = [
    "Electronics",
    "Phones",
    "Computers",
    "Clothing",
    "Shoes",
    "Home",
    "Beauty",
    "Sports",
    "Books",
    "Food",
    "Gaming",
    "Accessories",
    "Furniture",
    "Automotive",
    "Garden",
    "Health",
    "Office",
    "Travel",
    "Education",
    "Services",
]

CUSTOMER_START_DATE = datetime(2025, 1, 1)
CUSTOMER_END_DATE = datetime(2026, 1, 1)

TRANSACTION_START_DATE = datetime(2026, 1, 1)
TRANSACTION_END_DATE = datetime(2026, 9, 30, 23, 59, 59)


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def money(value: Decimal) -> Decimal:
    return value.quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def generate_categories() -> None:
    path = OUTPUT_DIR / f"categories/categories_{datetime.today().strftime('%Y-%m-%d')}.csv"

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "category_id",
            "category_name",
        ])

        for category_id in range(1, NB_CATEGORIES + 1):
            writer.writerow([
                category_id,
                CATEGORY_NAMES[category_id - 1],
            ])

    print(f"Generated: {path}")


def generate_products() -> dict[int, Decimal]:
    path = OUTPUT_DIR / f"products/products_{datetime.today().strftime('%Y-%m-%d')}.csv"
    products = {}

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "product_id",
            "category_id",
            "product_name",
            "unit_price",
        ])

        for product_id in range(1, NB_PRODUCTS + 1):
            category_id = random.randint(1, NB_CATEGORIES)

            unit_price = money(
                Decimal(random.randint(500, 200_000)) / Decimal(100)
            )

            products[product_id] = unit_price

            writer.writerow([
                product_id,
                category_id,
                f"Product_{product_id}",
                unit_price,
            ])

    print(f"Generated: {path}")

    return products


def generate_customers() -> dict[int, str]:
    path = OUTPUT_DIR / f"customers/customers_{datetime.today().strftime('%Y-%m-%d')}.csv"
    customers = {}

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "customer_id",
            "country",
            "segment",
            "signup_date",
        ])

        for customer_id in range(1, NB_CUSTOMERS + 1):
            country = random.choice(COUNTRIES)

            signup_date = random_datetime(
                CUSTOMER_START_DATE,
                CUSTOMER_END_DATE,
            ).date()

            customers[customer_id] = country

            writer.writerow([
                customer_id,
                country,
                random.choice(SEGMENTS),
                signup_date,
            ])

    print(f"Generated: {path}")

    return customers


def calculate_amount(
    unit_price: Decimal,
    quantity: int,
    discount_rate: Decimal,
) -> Decimal:
    subtotal = unit_price * quantity
    discount = subtotal * discount_rate
    return money(subtotal - discount)


def generate_transactions(products: dict[int, Decimal]) -> None:
    path = OUTPUT_DIR / f"transactions/transactions_{datetime.today().strftime('%Y-%m-%d')}.csv"

    discount_rates = [
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        Decimal("0.05"),
        Decimal("0.10"),
        Decimal("0.15"),
    ]

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
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
        ])

        for transaction_id in range(1, NB_TRANSACTIONS + 1):
            customer_id = random.randint(1, NB_CUSTOMERS)
            product_id = random.randint(1, NB_PRODUCTS)

            quantity = random.randint(1, 5)

            unit_price = products[product_id]

            discount_rate = random.choice(discount_rates)

            amount = calculate_amount(
                unit_price,
                quantity,
                discount_rate,
            )

            timestamp = random_datetime(
                TRANSACTION_START_DATE,
                TRANSACTION_END_DATE,
            )

            status = random.choice(STATUSES)

            writer.writerow([
                transaction_id,
                customer_id,
                product_id,
                quantity,
                unit_price,
                discount_rate,
                amount,
                random.choice(COUNTRIES),
                status,
                timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
                random.choice(PAYMENT_METHODS),
            ])

    print(f"Generated: {path}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "transactions").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "categories").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "products").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "customers").mkdir(parents=True, exist_ok=True)

    print("Generating final ecommerce dataset...\n")

    generate_categories()

    products = generate_products()

    generate_customers()

    generate_transactions(products)

    print("\nGeneration completed.")


if __name__ == "__main__":
    main()