#!/usr/bin/env python3
"""Agrega un lote diario reproducible: pedidos en MySQL y reseñas y eventos en MongoDB del mismo día.

El lote N corresponde al día 2026-01-01 + (N - 1) días. Repetirlo no duplica datos: los pedidos se
confirman si ya existen con los mismos valores y los documentos de MongoDB se reemplazan por su _id.
"""

import argparse
import os
import random
from datetime import datetime, timedelta

from faker import Faker
from pymongo import MongoClient

from ecommerce_dw.sources.generate_data import (
    build_review_text,
    connect_mysql,
    ensure_mongo_collections,
    mongo_event_document,
    mongo_review_document,
    to_mongo_utc,
)

LINES_PER_BATCH = 3
BUSINESS_HOUR = 10
EVENT_TYPES = ["vista_producto", "busqueda", "agregar_carrito"]
# Ids por lote: BASE_ID + batch_id * 10 + index. No colisionan con los datos base ni con los defectuosos.
BASE_ID = 1_000_000
ITEM_BASE_ID = 10_000_000


def load_reference_rows(cursor):
    """Lee clientes y productos base que el lote usa como referencia."""
    cursor.execute("SELECT customer_id, name FROM customers ORDER BY customer_id LIMIT %s", (LINES_PER_BATCH,))
    customers = [{"customer_id": customer_id, "name": name} for customer_id, name in cursor.fetchall()]
    cursor.execute("SELECT product_id, name, category, price FROM products ORDER BY product_id LIMIT %s", (LINES_PER_BATCH,))
    products = [
        {"product_id": product_id, "name": name, "category": category, "price": price}
        for product_id, name, category, price in cursor.fetchall()
    ]
    return customers, products


def build_order_rows(batch_id, business_day, customers, products):
    """Construye pedidos y líneas de pedido del lote con ids estables."""
    rows = []
    for index in range(LINES_PER_BATCH):
        customer = customers[index % len(customers)]
        product = products[index % len(products)]
        order_id = BASE_ID + batch_id * 10 + index
        item_id = ITEM_BASE_ID + batch_id * 10 + index
        quantity = index + 1
        subtotal = product["price"] * quantity
        order_date = business_day + timedelta(hours=BUSINESS_HOUR, minutes=index)
        order = (customer["customer_id"], order_date, "pagado", "web", "PSE", subtotal)
        item = (order_id, product["product_id"], quantity, product["price"], subtotal)
        rows.append((order_id, order, item_id, item))
    return rows


def write_orders(cursor, rows):
    """Inserta pedidos y líneas que falten. Devuelve cuántos pedidos nuevos se insertaron."""
    inserted = 0
    for order_id, order, item_id, item in rows:
        cursor.execute(
            "SELECT customer_id, order_date, status, sales_channel, payment_method, total FROM orders WHERE order_id=%s",
            (order_id,),
        )
        existing_order = cursor.fetchone()
        if existing_order is None:
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, order_date, status, sales_channel, payment_method, total) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (order_id, *order),
            )
            inserted += 1
        elif existing_order != order:
            raise ValueError(f"Order {order_id} already exists with different values")

        cursor.execute(
            "SELECT order_id, product_id, quantity, unit_price, subtotal FROM order_items WHERE order_item_id=%s",
            (item_id,),
        )
        existing_item = cursor.fetchone()
        if existing_item is None:
            cursor.execute(
                "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, subtotal) VALUES (%s, %s, %s, %s, %s, %s)",
                (item_id, *item),
            )
        elif existing_item != item:
            raise ValueError(f"Order item {item_id} already exists with different values")
    return inserted


def build_mongo_documents(batch_id, business_day, customers, products, fake, rng):
    """Construye reseñas y eventos del mismo día que los pedidos del lote."""
    reviews, events = [], []
    for index in range(LINES_PER_BATCH):
        customer = customers[index % len(customers)]
        product = products[index % len(products)]
        document_id = BASE_ID + batch_id * 10 + index
        moment = business_day + timedelta(hours=BUSINESS_HOUR, minutes=index)
        reviews.append(mongo_review_document(
            document_id, customer, product, rng.randint(1, 5), build_review_text(fake),
            to_mongo_utc(moment), bool(rng.getrandbits(1)),
        ))
        events.append(mongo_event_document(
            document_id, rng.choice(EVENT_TYPES), to_mongo_utc(moment), f"session-batch-{batch_id:06d}",
            customer, product, rng.choice(["móvil", "escritorio", "tableta"]), "web",
        ))
    return reviews, events


def write_mongo_documents(reviews, events):
    """Reemplaza por _id los documentos del lote, así repetirlo no duplica."""
    client = MongoClient(os.environ["MONGODB_URI"])
    try:
        db = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        ensure_mongo_collections(db)
        for document in reviews:
            db["reviews"].replace_one({"_id": document["_id"]}, document, upsert=True)
        for document in events:
            db["navigation_events"].replace_one({"_id": document["_id"]}, document, upsert=True)
    finally:
        client.close()


def main():
    """Carga el lote indicado en MySQL y MongoDB."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", type=int, required=True, help="Stable batch ID from 1 to 365")
    args = parser.parse_args()
    if not 1 <= args.batch_id <= 365:
        parser.error("Batch ID must be between 1 and 365")

    business_day = datetime(2026, 1, 1) + timedelta(days=args.batch_id - 1)
    rng = random.Random(args.batch_id)
    fake = Faker("es_CO")
    fake.seed_instance(args.batch_id)

    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            customers, products = load_reference_rows(cursor)
            if not customers or not products:
                raise ValueError("Source customers and products are missing. Run generate_data.sh first.")
            inserted_orders = write_orders(cursor, build_order_rows(args.batch_id, business_day, customers, products))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    reviews, events = build_mongo_documents(args.batch_id, business_day, customers, products, fake, rng)
    write_mongo_documents(reviews, events)
    print(
        f"Batch {args.batch_id} ({business_day:%Y-%m-%d}): {inserted_orders} new orders, "
        f"{len(reviews)} reviews and {len(events)} navigation events upserted"
    )


if __name__ == "__main__":
    main()
