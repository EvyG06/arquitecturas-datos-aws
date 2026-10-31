#!/usr/bin/env python3
"""Copia tablas MySQL y colecciones Mongo a raw en un único DuckDB."""

import os

import pymysql
from bson import json_util
from pymysql.cursors import SSCursor

BATCH_SIZE = 1000
MYSQL_TABLES = {
    "customers": """CREATE TABLE IF NOT EXISTS raw.mysql_customers (
        customer_id BIGINT, name VARCHAR, email VARCHAR, phone VARCHAR,
        city VARCHAR, department VARCHAR, registration_date DATE,
        updated_at TIMESTAMP, _loaded_at TIMESTAMPTZ, _source VARCHAR
    )""",
    "products": """CREATE TABLE IF NOT EXISTS raw.mysql_products (
        product_id BIGINT, name VARCHAR, category VARCHAR, price DECIMAL(12,2),
        description VARCHAR, is_active BOOLEAN, updated_at TIMESTAMP,
        _loaded_at TIMESTAMPTZ, _source VARCHAR
    )""",
    "orders": """CREATE TABLE IF NOT EXISTS raw.mysql_orders (
        order_id BIGINT, customer_id BIGINT, order_date TIMESTAMP, status VARCHAR,
        sales_channel VARCHAR, payment_method VARCHAR, total DECIMAL(14,2),
        _loaded_at TIMESTAMPTZ, _source VARCHAR
    )""",
    "order_items": """CREATE TABLE IF NOT EXISTS raw.mysql_order_items (
        order_item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity INTEGER,
        unit_price DECIMAL(12,2), subtotal DECIMAL(14,2),
        _loaded_at TIMESTAMPTZ, _source VARCHAR
    )""",
}
MONGO_TABLES = {
    "reviews": """CREATE TABLE IF NOT EXISTS raw.mongo_reviews (
        review_id BIGINT, customer_id BIGINT, product_id BIGINT,
        rating INTEGER, reviewed_at TIMESTAMPTZ, document_json JSON,
        _loaded_at TIMESTAMPTZ, _source VARCHAR
    )""",
    "navigation_events": """CREATE TABLE IF NOT EXISTS raw.mongo_navigation_events (
        event_id BIGINT, event_type VARCHAR, occurred_at TIMESTAMPTZ,
        customer_id BIGINT, product_id BIGINT, sales_channel VARCHAR,
        document_json JSON, _loaded_at TIMESTAMPTZ, _source VARCHAR
    )""",
}


def connect_mysql():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "ecommerce"),
        charset="utf8mb4",
        cursorclass=SSCursor,
    )


def flatten_mongo(collection, document, loaded_at):
    if collection == "reviews":
        customer = document.get("customer") or {}
        product = document.get("product") or {}
        values = (
            document.get("review_id"), customer.get("customer_id"),
            product.get("product_id"), document.get("rating"),
            document.get("reviewed_at"), json_util.dumps(document), loaded_at,
            "mongodb.reviews",
        )
        return "raw.mongo_reviews", values

    customer = document.get("customer") or {}
    product = document.get("product") or {}
    device = document.get("device") or {}
    values = (
        document.get("event_id"), document.get("event_type"), document.get("occurred_at"),
        customer.get("customer_id"), product.get("product_id"),
        device.get("sales_channel"), json_util.dumps(document), loaded_at,
        "mongodb.navigation_events",
    )
    return "raw.mongo_navigation_events", values


