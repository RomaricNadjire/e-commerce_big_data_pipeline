#!/usr/bin/env python3
"""
Load categories, products, and customers from CSVs under ./data/.

Layout expected:
    ./data/generated/categories/*.csv   → category_id, category_name
    ./data/generated/products/*.csv     → product_id, category_id, product_name, unit_price
    ./data/generated/customers/*.csv    → customer_id, country, segment, signup_date

Every CSV in each folder is loaded, concatenated, and upserted.
FK-safe order: categories → products → customers.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger("load_reference_data")

DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data/generated/"))


# --------------------------------------------------------------------------- #
# DB                                                                          #
# --------------------------------------------------------------------------- #
def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "airflow_training_postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


# --------------------------------------------------------------------------- #
# CSV helpers                                                                 #
# --------------------------------------------------------------------------- #
def read_csv_folder(folder: Path, required_cols: list[str]) -> pd.DataFrame:
    """Read and concatenate every *.csv in `folder`, validating columns."""
    if not folder.exists():
        log.warning("Folder %s does not exist — skipping", folder)
        return pd.DataFrame(columns=required_cols)

    files = sorted(folder.glob("*.csv"))
    if not files:
        log.warning("No CSVs found in %s — skipping", folder)
        return pd.DataFrame(columns=required_cols)

    frames = []
    for f in files:
        df = pd.read_csv(f)
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(f"{f}: missing columns {sorted(missing)}")
        frames.append(df[required_cols])
        log.info("  read %s (%d rows)", f.name, len(df))

    combined = pd.concat(frames, ignore_index=True)

    # de-duplicate on the PK, keeping the LAST occurrence (most recent file wins)
    pk = required_cols[0]
    before = len(combined)
    combined = combined.drop_duplicates(subset=pk, keep="last")
    if len(combined) < before:
        log.info("  dropped %d duplicate rows on %s", before - len(combined), pk)

    return combined


def upsert(conn, table: str, columns: list[str], rows: Iterable[tuple], conflict_col: str):
    """INSERT ... ON CONFLICT (conflict_col) DO UPDATE SET ... (upsert)."""
    rows = list(rows)
    if not rows:
        log.info("  nothing to insert into %s", table)
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != conflict_col)

    sql = f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=f"({placeholders})")
    conn.commit()
    log.info("  upserted %d rows into %s", len(rows), table)
    return len(rows)


# --------------------------------------------------------------------------- #
# Loaders                                                                     #
# --------------------------------------------------------------------------- #
def load_categories(conn) -> int:
    log.info("Categories")
    df = read_csv_folder(DATA_ROOT / "categories",
                         ["category_id", "category_name"])
    rows = [(int(r.category_id), str(r.category_name)) for r in df.itertuples(index=False)]
    return upsert(conn, "categories", ["category_id", "category_name"], rows, "category_id")


def load_products(conn) -> int:
    log.info("Products")
    df = read_csv_folder(DATA_ROOT / "products",
                         ["product_id", "category_id", "product_name", "unit_price"])
    rows = [
        (int(r.product_id), int(r.category_id), str(r.product_name), float(r.unit_price))
        for r in df.itertuples(index=False)
    ]
    return upsert(
        conn, "products",
        ["product_id", "category_id", "product_name", "unit_price"],
        rows, "product_id",
    )


def load_customers(conn) -> int:
    log.info("Customers")
    df = read_csv_folder(DATA_ROOT / "customers",
                         ["customer_id", "country", "segment", "signup_date"])
    # normalise dates to Python date objects
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date
    rows = [
        (int(r.customer_id), str(r.country).upper(), str(r.segment), r.signup_date)
        for r in df.itertuples(index=False)
    ]
    return upsert(
        conn, "customers",
        ["customer_id", "country", "segment", "signup_date"],
        rows, "customer_id",
    )


# --------------------------------------------------------------------------- #
# Entrypoint                                                                  #
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(description="Load reference CSVs into Postgres")
    parser.add_argument("--only", choices=["categories", "products", "customers"],
                        help="Load only one table (default: all)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # order matters: categories before products (FK)
    tasks = {
        "categories": load_categories,
        "products":   load_products,
        "customers":  load_customers,
    }
    order = ["categories", "products", "customers"] if not args.only else [args.only]

    conn = get_conn()
    try:
        totals = {}
        for name in order:
            totals[name] = tasks[name](conn)
        log.info("Done. %s", ", ".join(f"{k}={v}" for k, v in totals.items()))
    except Exception:
        conn.rollback()
        log.exception("Load failed — transaction rolled back")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())