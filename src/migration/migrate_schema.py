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
    # Tạo schema riêng cho timedealer
    "CREATE SCHEMA IF NOT EXISTS timedealer;",
    
    # Bảng lưu raw messages
    """
    CREATE TABLE IF NOT EXISTS timedealer.messages_raw (
        id SERIAL PRIMARY KEY,
        message TEXT,
        group_name TEXT,
        sender_name TEXT,
        sender_phone TEXT,
        time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        image TEXT,
        hash_message TEXT,
        phone_message_hash TEXT,
        duplicate_count INTEGER DEFAULT 0
    );
    """,
    # "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_time ON timedealer.messages_raw (phone_message_hash, time);",
    # "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_msg ON timedealer.messages_raw (hash_message);",
    # "CREATE INDEX IF NOT EXISTS idx_messages_raw_sender_phone ON timedealer.messages_raw (sender_phone);",

    # Bảng lưu message duy nhất (theo hash_message)
    """
    CREATE TABLE IF NOT EXISTS timedealer.messages_unique (
        unique_id SERIAL PRIMARY KEY,
        hash_message TEXT UNIQUE,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    # "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_unique_hash ON timedealer.messages_unique (hash_message);",
    # "CREATE INDEX IF NOT EXISTS idx_messages_unique_first_last ON timedealer.messages_unique (first_seen, last_seen);",

    # Bảng lưu các item extract từ message
    """
    CREATE TABLE IF NOT EXISTS timedealer.message_items (
        item_id SERIAL PRIMARY KEY,
        unique_id INTEGER NOT NULL,
        transaction_type TEXT,
        ref TEXT,
        brand TEXT,
        color TEXT,
        price DOUBLE PRECISION,
        currency TEXT,
        year TEXT,
        condition TEXT,
        note TEXT
    );
    """,
    # "CREATE INDEX IF NOT EXISTS idx_items_ref ON timedealer.message_items (ref);",
    # "CREATE INDEX IF NOT EXISTS idx_items_brand_year ON timedealer.message_items (brand, year);",
    # "CREATE INDEX IF NOT EXISTS idx_items_price ON timedealer.message_items (price);",
    # "CREATE INDEX IF NOT EXISTS idx_items_condition ON timedealer.message_items (condition);",
    # "CREATE INDEX IF NOT EXISTS idx_items_unique_id ON timedealer.message_items (unique_id);",
]

def migrate():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        for ddl in DDL_STATEMENTS:
            cursor.execute(ddl)
        conn.commit()
        cursor.close()
        conn.close()
        print("Database schema created successfully in PostgreSQL")
    except Exception as e:
        print("Migration error:", e)


if __name__ == "__main__":
    migrate()
