import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Thông tin kết nối
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE"),
}

DDL_STATEMENTS = [
    # Tạo schema riêng
    "CREATE SCHEMA IF NOT EXISTS timedealer;",
    "SET search_path TO timedealer;",

    # Bảng lưu raw messages
    """
    CREATE TABLE IF NOT EXISTS messages_raw (
        id SERIAL PRIMARY KEY,
        message TEXT,
        group_name TEXT,
        sender_name TEXT,
        sender_phone TEXT,
        posted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        image TEXT,
        hash_message TEXT,
        phone_message_hash TEXT,
        duplicate_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_time ON messages_raw (phone_message_hash, posted_time);",
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_msg ON messages_raw (hash_message);",
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_sender_phone ON messages_raw (sender_phone);",

    # Bảng lưu message duy nhất
    """
    CREATE TABLE IF NOT EXISTS messages_unique (
        unique_id SERIAL PRIMARY KEY,
        hash_message TEXT UNIQUE,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_unique_hash ON messages_unique (hash_message);",
    "CREATE INDEX IF NOT EXISTS idx_messages_unique_first_last ON messages_unique (first_seen, last_seen);",

    # Bảng lưu các item extract từ message
    """
    CREATE TABLE IF NOT EXISTS message_items (
        item_id SERIAL PRIMARY KEY,
        message_id INTEGER NOT NULL,
        transaction_type TEXT,
        ref TEXT,
        brand TEXT,
        color TEXT,
        price DOUBLE PRECISION,
        country TEXT,
        currency TEXT,
        year TEXT,
        condition TEXT,
        note TEXT
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_items_ref ON message_items (ref);",
    "CREATE INDEX IF NOT EXISTS idx_items_brand_year ON message_items (brand, year);",
    "CREATE INDEX IF NOT EXISTS idx_items_price ON message_items (price);",
    "CREATE INDEX IF NOT EXISTS idx_items_condition ON message_items (condition);",
    "CREATE INDEX IF NOT EXISTS idx_items_unique_id ON message_items (message_id);",

    # Tracking Items
    """
    CREATE TABLE IF NOT EXISTS tracking_items (
        tracking_id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        tracking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        transaction_type TEXT,
        ref TEXT,
        brand TEXT,
        color TEXT,
        min_price DOUBLE PRECISION,
        max_price DOUBLE PRECISION,
        country TEXT,
        currency TEXT,
        year TEXT,
        condition TEXT,
        count INTEGER,
        UNIQUE (user_id, item_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_tracking_items_tracking_time ON tracking_items (tracking_time);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_items_brand ON tracking_items (brand);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_items_min_price ON tracking_items (min_price);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_items_max_price ON tracking_items (max_price);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_items_condition ON tracking_items (condition);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_items_color ON tracking_items (color);",

    # Tracking results
    """
    CREATE TABLE IF NOT EXISTS tracking_results (
        id SERIAL PRIMARY KEY,
        tracking_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (tracking_id, item_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_tracking_results_tracking_id ON tracking_results (tracking_id);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_results_item_id ON tracking_results (item_id);",
]

def init_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for ddl in DDL_STATEMENTS:
        cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ PostgreSQL schema (timedealer) created successfully.")

if __name__ == "__main__":
    init_postgres()
