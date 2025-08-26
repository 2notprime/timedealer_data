import sqlite3
import hashlib
import random
from faker import Faker

def insert_sample_data(db_path="my_local.db", n=100):
    fake = Faker()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for i in range(n):
        # Fake message
        brand = random.choice(["iPhone", "Samsung", "Xiaomi", "Oppo", "Nokia"])
        color = random.choice(["Black", "White", "Blue", "Gold", "Silver"])
        year = random.choice(["2019", "2020", "2021", "2022", "2023"])
        condition = random.choice(["New", "Like New", "Used"])
        price = random.randint(3000000, 30000000)  # 3tr - 30tr
        currency = "VND"

        message = f"Selling {brand} {year}, {color}, {condition}, price {price} {currency}"
        sender_phone = fake.phone_number()

        # Hash values
        hash_message = hashlib.sha256(message.encode()).hexdigest()
        phone_message_hash = hashlib.sha256((sender_phone + message).encode()).hexdigest()

        # Insert vào messages_raw
        cursor.execute("""
            INSERT INTO messages_raw (message, group_name, sender_name, sender_phone, image, hash_message, phone_message_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (message, fake.company(), fake.name(), sender_phone, None, hash_message, phone_message_hash))
        raw_id = cursor.lastrowid

        # Insert vào messages_unique (nếu chưa có)
        cursor.execute("INSERT OR IGNORE INTO messages_unique (hash_message) VALUES (?)", (hash_message,))
        cursor.execute("SELECT unique_id FROM messages_unique WHERE hash_message = ?", (hash_message,))
        unique_id = cursor.fetchone()[0]

        # Insert vào message_items
        cursor.execute("""
            INSERT INTO message_items (unique_id, transaction_type, ref, brand, color, price, currency, year, condition, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            unique_id,
            "sell",
            f"REF-{i+1:03d}",
            brand,
            color,
            price,
            currency,
            year,
            condition,
            fake.text(max_nb_chars=50)
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {n} sample records successfully!")

if __name__ == "__main__":
    insert_sample_data(n=100)
