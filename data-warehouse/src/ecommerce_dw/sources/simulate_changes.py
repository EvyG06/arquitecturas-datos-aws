#!/usr/bin/env python3
"""Aplica cambios repetibles de ciudad y categoría para demostrar SCD2."""

import argparse
import os
from datetime import datetime

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--customer-id", type=int, default=1)
    parser.add_argument("--product-id", type=int, default=1)
    parser.add_argument("--city", default="Medellín")
    parser.add_argument("--department", default="Antioquia")
    parser.add_argument("--category", default="Hogar")
    parser.add_argument(
        "--changed-at",
        type=datetime.fromisoformat,
        default=datetime(2025, 7, 1),
        help="Fecha de vigencia del cambio, escrita en updated_at. Repetir con el mismo valor es idempotente.",
    )
    args = parser.parse_args()

    connection = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "ecommerce"),
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT city, department FROM customers WHERE customer_id=%s", (args.customer_id,))
            old_customer = cursor.fetchone()
            cursor.execute("SELECT category FROM products WHERE product_id=%s", (args.product_id,))
            old_product = cursor.fetchone()
            if old_customer is None or old_product is None:
                raise ValueError("Customer or product not found. Run generate_data.py with available IDs first.")
            cursor.execute(
                "UPDATE customers SET city=%s, department=%s, updated_at=%s WHERE customer_id=%s",
                (args.city, args.department, args.changed_at, args.customer_id),
            )
            cursor.execute(
                "UPDATE products SET category=%s, updated_at=%s WHERE product_id=%s",
                (args.category, args.changed_at, args.product_id),
            )
        connection.commit()
        print(f"Customer {args.customer_id}: {old_customer[0]}, {old_customer[1]} -> {args.city}, {args.department}")
        print(f"Product {args.product_id}: {old_product[0]} -> {args.category}")
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
