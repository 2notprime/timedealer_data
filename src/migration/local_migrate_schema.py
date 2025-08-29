import sqlite3

conn = sqlite3.connect("my_local_v2.db")
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
        posted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        image TEXT,
        hash_message TEXT,          -- hash toàn message (lọc trùng toàn thời gian)
        phone_message_hash TEXT,    -- hash(phone + message) (lọc trùng theo số trong 7 ngày)
        duplicate_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_messages_raw_hash_time ON messages_raw (phone_message_hash, posted_time);",
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
        message_id INTEGER NOT NULL,
        transaction_type TEXT,
        ref TEXT,
        brand TEXT,
        color TEXT,
        price REAL,
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
        tracking_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        tracking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        transaction_type TEXT,
        ref TEXT,
        brand TEXT,
        color TEXT,
        min_price REAL,
        max_price REAL,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tracking_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (tracking_id, item_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_tracking_results_tracking_id ON tracking_results (tracking_id);",
    "CREATE INDEX IF NOT EXISTS idx_tracking_results_item_id ON tracking_results (item_id);",
]

for ddl in DDL_STATEMENTS:
    cursor.execute(ddl)

# Lưu và đóng
conn.commit()
cursor.close()
conn.close()
print("Database schema created successfully in SQLite (my_local.db)")
