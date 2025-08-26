import sqlite3

conn = sqlite3.connect("my_local_v1.db")
cursor = conn.cursor()

DDL_STATEMENTS = [
    # Bảng lưu raw messages
    """
    CREATE TABLE IF NOT EXISTS messages_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT,
        group_name TEXT,
        sender_name TEXT,
        sender_phone TEXT,
        time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        image TEXT,
        hash_message TEXT,          -- hash toàn message (lọc trùng toàn thời gian)
        phone_message_hash TEXT,    -- hash(phone + message) (lọc trùng theo số trong 7 ngày)
        duplicate_count INTEGER DEFAULT 0
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_time ON messages_raw (phone_message_hash, time);",
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_msg ON messages_raw (hash_message);",
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_sender_phone ON messages_raw (sender_phone);",
    # Bảng lưu message duy nhất (theo hash_message)
    """
    CREATE TABLE IF NOT EXISTS messages_unique (
        unique_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        unique_id INTEGER NOT NULL,
        transaction_type TEXT,
        ref TEXT,
        brand TEXT,
        color TEXT,
        price REAL,
        currency TEXT,
        year TEXT,
        condition TEXT,
        note TEXT,
        FOREIGN KEY (unique_id) REFERENCES messages_unique(unique_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_items_ref ON message_items (ref);",
    "CREATE INDEX IF NOT EXISTS idx_items_brand_year ON message_items (brand, year);",
    "CREATE INDEX IF NOT EXISTS idx_items_price ON message_items (price);",
    "CREATE INDEX IF NOT EXISTS idx_items_condition ON message_items (condition);",
    "CREATE INDEX IF NOT EXISTS idx_items_unique_id ON message_items (unique_id);",
]

for ddl in DDL_STATEMENTS:
    cursor.execute(ddl)

# Lưu và đóng
conn.commit()
cursor.close()
conn.close()
print("Database schema created successfully in SQLite (my_local.db)")
