import sqlite3
import hashlib
import random
import datetime

def fake_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

def create_fake_data(db_path="my_local_v2.db", n=20):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    brands = ["Nike", "Adidas", "Puma", "Gucci", "LV"]
    colors = ["Black", "White", "Red", "Blue", "Green"]
    conditions = ["new", "used", "like new"]
    transaction_types = ["wtb", "forsale"]
    countries = ["VN", "US", "JP", "KR", "CN", "DE", "FR", "IT", "GB", "SG"]

    for i in range(1, n + 1):
        # Fake message
        sender_phone = f"+8498{random.randint(100000,999999)}"
        message = f"{random.choice(transaction_types).upper()} {random.choice(brands)} {random.choice(colors)} size {random.randint(36,45)}"
        posted_time = datetime.datetime.now() - datetime.timedelta(days=random.randint(0,30))
        hash_msg = fake_hash(message)
        phone_message_hash = fake_hash(sender_phone + message)

        # 1. Insert vào messages_raw
        cur.execute("""
            INSERT INTO messages_raw (message, group_name, sender_name, sender_phone, posted_time, image, hash_message, phone_message_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            message,
            f"group_{random.randint(1,5)}",
            f"user_{random.randint(1,10)}",
            sender_phone,
            posted_time,
            "http://example.com/image.jpg",
            hash_msg,
            phone_message_hash
        ))
        message_id = cur.lastrowid

        # 2. Sync sang messages_unique
        cur.execute("SELECT unique_id FROM messages_unique WHERE hash_message = ?", (hash_msg,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE messages_unique SET last_seen=? WHERE unique_id=?", (posted_time, row[0]))
            unique_id = row[0]
        else:
            cur.execute("INSERT INTO messages_unique (hash_message, first_seen, last_seen) VALUES (?, ?, ?)",
                        (hash_msg, posted_time, posted_time))
            unique_id = cur.lastrowid

        # 3. Insert fake item
        cur.execute("""
            INSERT INTO message_items (message_id, transaction_type, ref, brand, color, price, country, currency, year, condition, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            message_id,
            random.choice(transaction_types),
            f"REF-{i:04d}",
            random.choice(brands),
            random.choice(colors),
            round(random.uniform(50,500),2),
            random.choice(countries),   # dùng mã quốc gia ISO-2
            "USD",
            str(random.randint(2015,2025)),
            random.choice(conditions),
            "fake data for testing"
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {n} fake records successfully.")

if __name__ == "__main__":
    create_fake_data()
