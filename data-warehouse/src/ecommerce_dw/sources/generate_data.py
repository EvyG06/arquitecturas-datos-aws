#!/usr/bin/env python3
"""Genera datos sintéticos reproducibles para ambas fuentes."""

import argparse
import itertools
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql
from faker import Faker
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

DEPARTMENTS = [
    ("Bogotá, D.C.", "Bogotá"),
    ("Antioquia", "Medellín"),
    ("Valle del Cauca", "Cali"),
    ("Atlántico", "Barranquilla"),
    ("Santander", "Bucaramanga"),
    ("Bolívar", "Cartagena"),
    ("Cundinamarca", "Soacha"),
    ("Risaralda", "Pereira"),
]
CATEGORIES = ["Electrónica", "Hogar", "Deportes", "Ropa", "Libros", "Juguetes"]
SALES_CHANNELS = ["web", "móvil", "tienda"]
PAYMENT_METHODS = ["tarjeta", "PSE", "transferencia", "contraentrega"]
ORDER_STATUSES = ["pagado", "enviado", "entregado", "cancelado"]
PRODUCT_NOUNS = [
    "Audífonos", "Cafetera", "Chaqueta", "Lámpara", "Mochila", "Novela", "Balón", "Silla", "Tenis",
    "Licuadora", "Cámara", "Reloj", "Bicicleta", "Tapete", "Juego de mesa", "Termo", "Cuaderno",
    "Teclado", "Bolso", "Estufa",
]
PRODUCT_ADJECTIVES = ["Compacto", "Clásico", "Premium", "Básico", "Profesional", "Ecológico", "Portátil", "Plegable"]
REVIEW_OPENINGS = [
    "Llegó a tiempo", "El producto cumple lo que promete", "La calidad es buena",
    "El empaque llegó en buen estado", "La entrega tardó más de lo esperado",
    "El producto no coincide con la descripción", "Lo recomiendo a otros compradores",
    "No volvería a comprarlo", "La relación precio-calidad es razonable", "El funcionamiento es correcto",
]
REVIEW_DETAILS = [
    "y ya lo estoy usando a diario", "aunque el color es un poco distinto",
    "el servicio al cliente respondió rápido", "el manual venía incompleto",
    "pero se nota el buen acabado", "y el envío fue puntual", "aunque esperaba algo más duradero",
    "y superó mis expectativas", "pero la caja llegó algo maltratada", "y cumple para el uso que le doy",
]
# Ids de registros defectuosos y de referencias huérfanas, fuera del rango válido.
BAD_ID_BASE = 800_000
ORPHAN_ID_BASE = 900_000
# Las fechas se generan en hora de negocio; MongoDB las guarda en UTC.
BUSINESS_TIMEZONE = ZoneInfo("America/Bogota")
YEAR_MINUTES = 365 * 24 * 60


def spread_within_year(index, count, rng):
    """Reparte `count` registros en franjas iguales de 2025 para que todas las fuentes cubran el año completo."""
    slot = YEAR_MINUTES // count
    return timedelta(minutes=index * slot + rng.randrange(slot))

REVIEW_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["review_id", "customer", "product", "rating", "review_text", "reviewed_at", "verification"],
        "properties": {
            "review_id": {"bsonType": "int"},
            "customer": {"bsonType": "object", "required": ["customer_id", "name"], "properties": {"customer_id": {"bsonType": "int"}, "name": {"bsonType": "string"}}},
            "product": {"bsonType": "object", "required": ["product_id", "name", "category"], "properties": {"product_id": {"bsonType": "int"}, "name": {"bsonType": "string"}, "category": {"bsonType": "string"}}},
            "rating": {"bsonType": "int", "minimum": 1, "maximum": 5},
            "review_text": {"bsonType": "string"},
            "reviewed_at": {"bsonType": "date"},
            "verification": {"bsonType": "object", "required": ["purchase_confirmed", "language"], "properties": {"purchase_confirmed": {"bsonType": "bool"}, "language": {"bsonType": "string"}}},
        },
    }
}
EVENT_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["event_id", "event_type", "occurred_at", "session", "customer", "page", "device"],
        "properties": {
            "event_id": {"bsonType": "int"},
            "event_type": {"bsonType": "string", "enum": ["vista_producto", "busqueda", "agregar_carrito"]},
            "occurred_at": {"bsonType": "date"},
            "session": {"bsonType": "object", "required": ["id"], "properties": {"id": {"bsonType": "string"}}},
            "customer": {"bsonType": ["object", "null"]},
            "page": {"bsonType": "object", "required": ["type", "path"], "properties": {"type": {"bsonType": "string"}, "path": {"bsonType": "string"}}},
            "product": {"bsonType": ["object", "null"]},
            "device": {"bsonType": "object", "required": ["type", "sales_channel"], "properties": {"type": {"bsonType": "string"}, "sales_channel": {"bsonType": "string", "enum": SALES_CHANNELS}}},
        },
    }
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--customer-count", type=int, default=50, help="Customer count (minimum 1)")
    parser.add_argument("--product-count", type=int, default=25, help="Product count (minimum 1)")
    parser.add_argument("--order-count", type=int, default=100, help="Order count")
    parser.add_argument("--review-count", type=int, default=150, help="Number of reviews")
    parser.add_argument("--event-count", type=int, default=500, help="Navigation event count")
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible data")
    parser.add_argument("--bad-records", type=int, default=2,
                        help="Defective records injected per data quality rule (0 disables injection)")
    args = parser.parse_args()
    if args.customer_count < 1 or args.product_count < 1 or min(args.order_count, args.review_count, args.event_count, args.bad_records) < 0:
        parser.error("Customer and product counts must be >= 1; other counts must be >= 0")
    return args


def connect_mysql():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "ecommerce"),
        charset="utf8mb4",
        autocommit=False,
    )


def ensure_mongo_collections(db):
    for name, validator in (("reviews", REVIEW_VALIDATOR), ("navigation_events", EVENT_VALIDATOR)):
        if name in db.list_collection_names():
            db.command("collMod", name, validator=validator, validationLevel="strict")
        else:
            try:
                db.create_collection(name, validator=validator, validationLevel="strict")
            except CollectionInvalid:
                db.command("collMod", name, validator=validator, validationLevel="strict")


def build_rows(args):
    rng = random.Random(args.seed)
    fake = Faker("es_CO")
    fake.seed_instance(args.seed)
    start = datetime(2025, 1, 1)

    customers = []
    for i in range(1, args.customer_count + 1):
        department, city = rng.choice(DEPARTMENTS)
        if i == 1:
            department, city = "Bogotá, D.C.", "Bogotá"
        registration_date = start.date() - timedelta(days=rng.randint(1, 1200))
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.unique.ascii_free_email(),
            "phone": fake.phone_number()[:32],
            "city": city,
            "department": department,
            "registration_date": registration_date,
            "updated_at": datetime.combine(registration_date, datetime.min.time()),
        })

    products = []
    for i in range(1, args.product_count + 1):
        category = "Electrónica" if i == 1 else rng.choice(CATEGORIES)
        name = f"{fake.random_element(PRODUCT_NOUNS)} {fake.random_element(PRODUCT_ADJECTIVES)}"
        products.append({
            "product_id": i,
            "name": name,
            "category": category,
            "price": Decimal(rng.randint(1500, 2500000)) / 100,
            "description": f"{name} de la categoría {category}, con garantía de {fake.random_int(min=1, max=24)} meses.",
            "is_active": True,
            "updated_at": start,
        })

    orders, details = [], []
    detail_id = 1
    for order_id in range(1, args.order_count + 1):
        date = start + spread_within_year(order_id - 1, args.order_count, rng)
        selected_products = rng.sample(products, rng.randint(1, min(4, len(products))))
        total = Decimal("0.00")
        lines = []
        for product in selected_products:
            quantity = rng.randint(1, 4)
            subtotal = product["price"] * quantity
            lines.append({
                "order_item_id": detail_id,
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "subtotal": subtotal,
            })
            detail_id += 1
            total += subtotal
        orders.append({
            "order_id": order_id,
            "customer_id": rng.randint(1, args.customer_count),
            "order_date": date,
            "status": rng.choice(ORDER_STATUSES),
            "sales_channel": rng.choice(SALES_CHANNELS),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "total": total,
        })
        details.extend(lines)

    return rng, fake, customers, products, orders, details, start


def _insert_mysql_rows(cursor, customers, products, orders, details):
    if customers:
        cursor.executemany(
            "INSERT INTO customers (customer_id,name,email,phone,city,department,registration_date,updated_at) VALUES (%(customer_id)s,%(name)s,%(email)s,%(phone)s,%(city)s,%(department)s,%(registration_date)s,%(updated_at)s)", customers)
    if products:
        cursor.executemany(
            "INSERT INTO products (product_id,name,category,price,description,is_active,updated_at) VALUES (%(product_id)s,%(name)s,%(category)s,%(price)s,%(description)s,%(is_active)s,%(updated_at)s)", products)
    if orders:
        cursor.executemany(
            "INSERT INTO orders (order_id,customer_id,order_date,status,sales_channel,payment_method,total) VALUES (%(order_id)s,%(customer_id)s,%(order_date)s,%(status)s,%(sales_channel)s,%(payment_method)s,%(total)s)", orders)
    if details:
        cursor.executemany(
            "INSERT INTO order_items (order_item_id,order_id,product_id,quantity,unit_price,subtotal) VALUES (%(order_item_id)s,%(order_id)s,%(product_id)s,%(quantity)s,%(unit_price)s,%(subtotal)s)", details)


def load_mysql(customers, products, orders, details, bad):
    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            schema = Path(__file__).parent / "mysql" / "schema.sql"
            for statement in schema.read_text().split(";"):
                if statement.strip():
                    cursor.execute(statement)
        connection.commit()
        with connection.cursor() as cursor:
            # Full reset keeps row counts deterministic across runs.
            cursor.execute("DELETE FROM order_items")
            cursor.execute("DELETE FROM orders")
            cursor.execute("DELETE FROM customers")
            cursor.execute("DELETE FROM products")
            _insert_mysql_rows(cursor, customers, products, orders, details)
        connection.commit()
        with connection.cursor() as cursor:
            # Orphan references need FK checks disabled; CHECK constraints stay active.
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            _insert_mysql_rows(
                cursor, bad["mysql_customers"], bad["mysql_products"],
                bad["mysql_orders"], bad["mysql_order_items"])
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def to_mongo_utc(business_datetime):
    """Convierte una fecha de hora de negocio (Bogotá) a UTC aware, que es como MongoDB la guarda."""
    if business_datetime is None:
        return None
    return business_datetime.replace(tzinfo=BUSINESS_TIMEZONE).astimezone(timezone.utc)


def build_review_text(fake):
    """Genera una reseña corta en español con el generador sembrado de Faker."""
    return f"{fake.random_element(REVIEW_OPENINGS)} {fake.random_element(REVIEW_DETAILS)}."


def mongo_review_document(review_id, customer, product, rating, review_text, reviewed_at, purchase_confirmed):
    """Construye una reseña que cumple REVIEW_VALIDATOR. customer y product son filas de MySQL."""
    return {
        "_id": f"review-{review_id:08d}",
        "review_id": review_id,
        "customer": {"customer_id": customer["customer_id"], "name": customer["name"]},
        "product": {"product_id": product["product_id"], "name": product["name"], "category": product["category"]},
        "rating": rating,
        "review_text": review_text,
        "reviewed_at": reviewed_at,
        "verification": {"purchase_confirmed": purchase_confirmed, "language": "es-CO"},
    }


def mongo_event_document(event_id, event_type, occurred_at, session_id, customer, product, device_type, sales_channel):
    """Construye un evento de navegación que cumple EVENT_VALIDATOR. customer es None en eventos anónimos."""
    return {
        "_id": f"event-{event_id:08d}",
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "session": {"id": session_id},
        "customer": {"customer_id": customer["customer_id"], "name": customer["name"]} if customer else None,
        "page": {"type": "producto", "path": f"/productos/{product['product_id']}"},
        "product": {"product_id": product["product_id"], "name": product["name"], "category": product["category"]},
        "device": {"type": device_type, "sales_channel": sales_channel},
    }


def build_mongo_documents(args, rng, fake, customers, products, start):
    """Construye reseñas y eventos válidos. Consume rng en el mismo orden que la versión previa."""
    by_client = {row["customer_id"]: row for row in customers}
    by_product = {row["product_id"]: row for row in products}

    review_docs = []
    for i in range(1, args.review_count + 1):
        person = by_client[rng.randint(1, len(customers))]
        product = by_product[rng.randint(1, len(products))]
        rating = rng.randint(1, 5)
        review_text = build_review_text(fake)
        reviewed_at = start + spread_within_year(i - 1, args.review_count, rng)
        purchase_confirmed = bool(rng.getrandbits(1))
        review_docs.append(mongo_review_document(
            i, person, product, rating, review_text, to_mongo_utc(reviewed_at), purchase_confirmed,
        ))

    event_docs = []
    for i in range(1, args.event_count + 1):
        product = by_product[rng.randint(1, len(products))]
        person_id = rng.randint(1, len(customers)) if rng.random() < 0.8 else None
        person = by_client[person_id] if person_id else None
        event_type = rng.choice(["vista_producto", "busqueda", "agregar_carrito"])
        occurred_at = start + spread_within_year(i - 1, args.event_count, rng)
        session_id = f"session-{rng.randint(1, max(1, args.event_count // 4)):06d}"
        device_type = rng.choice(["móvil", "escritorio", "tableta"])
        sales_channel = rng.choice(SALES_CHANNELS)
        event_docs.append(mongo_event_document(
            i, event_type, to_mongo_utc(occurred_at), session_id, person, product, device_type, sales_channel,
        ))
    return review_docs, event_docs


def _mysql_customer(customer_id, name, email, start):
    return {"customer_id": customer_id, "name": name, "email": email, "phone": "3000000000",
            "city": "Bogotá", "department": "Bogotá, D.C.", "registration_date": start.date(),
            "updated_at": start}


def _mysql_product(product_id, category, start):
    return {"product_id": product_id, "name": "Producto de prueba", "category": category,
            "price": Decimal("10.00"), "description": "Producto para cuarentena", "is_active": True,
            "updated_at": start}


def _mysql_order(order_id, customer_id, sales_channel, total, start):
    return {"order_id": order_id, "customer_id": customer_id, "order_date": start, "status": "pagado",
            "sales_channel": sales_channel, "payment_method": "tarjeta", "total": total}


def _mysql_item(order_item_id, order_id, product_id, subtotal):
    return {"order_item_id": order_item_id, "order_id": order_id, "product_id": product_id,
            "quantity": 1, "unit_price": Decimal("10.00"), "subtotal": subtotal}


def _mongo_review(review_id, customer_id, product_id, rating, reviewed_at):
    return {
        "_id": f"review-bad-{review_id:08d}",
        "review_id": review_id,
        "customer": {"customer_id": customer_id, "name": "Cliente de prueba"},
        "product": {"product_id": product_id, "name": "Producto de prueba", "category": "Electrónica"},
        "rating": rating,
        "review_text": "Reseña para cuarentena.",
        "reviewed_at": reviewed_at,
        "verification": {"purchase_confirmed": False, "language": "es-CO"},
    }


def _mongo_event(event_id, event_type, occurred_at, customer_id, product_id):
    return {
        "_id": f"event-bad-{event_id:08d}",
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "session": {"id": "session-bad-000001"},
        "customer": {"customer_id": customer_id, "name": "Cliente de prueba"},
        "page": {"type": "producto", "path": f"/productos/{product_id}"},
        "product": {"product_id": product_id, "name": "Producto de prueba", "category": "Electrónica"},
        "device": {"type": "móvil", "sales_channel": "web"},
    }


def build_bad_records(args, start):
    """Construye registros defectuosos, una instancia por regla y por unidad de --bad-records.

    No usa el generador aleatorio: los registros válidos quedan idénticos con o sin defectos.
    """
    bad = {key: [] for key in ("mysql_customers", "mysql_products", "mysql_orders", "mysql_order_items",
                               "mongo_reviews", "mongo_events")}
    customer_ids = itertools.count(BAD_ID_BASE)
    product_ids = itertools.count(BAD_ID_BASE)
    order_ids = itertools.count(BAD_ID_BASE)
    item_ids = itertools.count(BAD_ID_BASE)
    review_ids = itertools.count(BAD_ID_BASE)
    event_ids = itertools.count(BAD_ID_BASE)
    orphan_ids = itertools.count(ORPHAN_ID_BASE)
    valid_customer, valid_product = 1, 1
    utc_start = to_mongo_utc(start)

    for k in range(args.bad_records):
        bad["mysql_customers"].append(_mysql_customer(next(customer_ids), "   ", f"sin-nombre-{k}@ejemplo.co", start))
        bad["mysql_customers"].append(_mysql_customer(next(customer_ids), f"Correo Inválido {k}", f"correo-invalido-{k}", start))
        bad["mysql_products"].append(_mysql_product(next(product_ids), "   ", start))
        bad["mysql_orders"].append(_mysql_order(next(order_ids), next(orphan_ids), "web", Decimal("0.00"), start))
        bad["mysql_orders"].append(_mysql_order(next(order_ids), valid_customer, "Web", Decimal("0.00"), start))
        bad["mysql_order_items"].append(_mysql_item(next(item_ids), next(orphan_ids), valid_product, Decimal("10.00")))
        bad["mysql_order_items"].append(_mysql_item(next(item_ids), 1, next(orphan_ids), Decimal("10.00")))
        # Valid header isolates the altered subtotal, since (order_id, product_id) is unique.
        header_id = next(order_ids)
        bad["mysql_orders"].append(_mysql_order(header_id, valid_customer, "web", Decimal("10.00"), start))
        bad["mysql_order_items"].append(_mysql_item(next(item_ids), header_id, valid_product, Decimal("15.00")))

        bad["mongo_reviews"].append(_mongo_review(next(review_ids), valid_customer, valid_product, 7, utc_start))
        bad["mongo_reviews"].append(_mongo_review(next(review_ids), valid_customer, valid_product, 3, None))
        bad["mongo_reviews"].append(_mongo_review(next(review_ids), next(orphan_ids), valid_product, 3, utc_start))
        bad["mongo_reviews"].append(_mongo_review(next(review_ids), valid_customer, next(orphan_ids), 3, utc_start))

        bad["mongo_events"].append(_mongo_event(next(event_ids), "clic_fantasma", utc_start, valid_customer, valid_product))
        bad["mongo_events"].append(_mongo_event(next(event_ids), "busqueda", None, valid_customer, valid_product))
        bad["mongo_events"].append(_mongo_event(next(event_ids), "busqueda", utc_start, next(orphan_ids), valid_product))
        bad["mongo_events"].append(_mongo_event(next(event_ids), "busqueda", utc_start, valid_customer, next(orphan_ids)))
    return bad


def load_mongo(args, rng, fake, customers, products, start, bad):
    client = MongoClient(os.environ["MONGODB_URI"])
    try:
        db = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        ensure_mongo_collections(db)
        reviews, events = db["reviews"], db["navigation_events"]
        reviews.delete_many({})
        events.delete_many({})

        review_docs, event_docs = build_mongo_documents(args, rng, fake, customers, products, start)
        if review_docs:
            reviews.insert_many(review_docs, ordered=True)
        if event_docs:
            events.insert_many(event_docs, ordered=True)
        # Simulates an import that skips the validator; the strict schema itself is unchanged.
        if bad["mongo_reviews"]:
            reviews.insert_many(bad["mongo_reviews"], ordered=True, bypass_document_validation=True)
        if bad["mongo_events"]:
            events.insert_many(bad["mongo_events"], ordered=True, bypass_document_validation=True)
    finally:
        client.close()


def main():
    args = parse_args()
    rng, fake, customers, products, orders, details, start = build_rows(args)
    bad = build_bad_records(args, start)
    load_mysql(customers, products, orders, details, bad)
    load_mongo(args, rng, fake, customers, products, start, bad)
    bad_total = sum(len(rows) for rows in bad.values())
    print(f"MySQL: {len(customers)} customers, {len(products)} products, {len(orders)} orders, {len(details)} order items")
    print(f"MongoDB: {args.review_count} reviews, {args.event_count} navigation events; seed={args.seed}")
    print(f"Injected bad rows: {bad_total} (--bad-records={args.bad_records}); "
          "includes valid order headers that isolate subtotal defects")


if __name__ == "__main__":
    main()
