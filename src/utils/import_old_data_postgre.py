import psycopg2
import psycopg2.extras
import hashlib
from datetime import datetime
import csv
from tqdm import tqdm
import json
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_unique_id(cursor, hash_message):
    cursor.execute(
        "SELECT unique_id FROM timedealer.messages_unique WHERE hash_message = %s",
        (hash_message,),
    )
    row = cursor.fetchone()
    if row:
        cursor.execute(
            "UPDATE timedealer.messages_unique SET last_seen = CURRENT_TIMESTAMP WHERE unique_id = %s",
            (row[0],),
        )
        return row[0]
    else:
        cursor.execute(
            "INSERT INTO timedealer.messages_unique (hash_message) VALUES (%s) RETURNING unique_id",
            (hash_message,),
        )
        return cursor.fetchone()[0]


def insert_raw(raw: dict):
    conn = get_conn()
    cursor = conn.cursor()

    uuid = raw.get("Uuid", "")
    message = raw.get("MessageRaw", "")
    group_name = raw.get("GroupName", "")
    sender_name = raw.get("SenderName", "")
    sender_phone = raw.get("SenderPhone", "")
    image = raw.get("Image", "")
    time_str = raw.get("Time")

    time_obj = None
    if time_str:
        try:
            time_obj = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except Exception:
            pass

    hash_message = hashlib.sha256(message.encode()).hexdigest()
    phone_message_hash = hashlib.sha256((sender_phone + message).encode()).hexdigest()

    cursor.execute(
        """
        INSERT INTO timedealer.messages_raw 
        (message, group_name, sender_name, sender_phone, time, image, hash_message, phone_message_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            message,
            group_name,
            sender_name,
            sender_phone,
            time_obj,
            image,
            hash_message,
            phone_message_hash,
        ),
    )

    unique_id = get_unique_id(cursor, hash_message)

    conn.commit()
    conn.close()

    return {"uuid": uuid, "unique_id": unique_id, "hash_message": hash_message}


def insert_items_from_csv(csv_path, uuid_to_unique):
    conn = get_conn()
    cursor = conn.cursor()

    success, fail = 0, 0
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader):
            try:
                uuid = row["Uuid"]
                unique_id = uuid_to_unique.get(uuid)
                if not unique_id:
                    print(f"⚠️ Bỏ qua item vì không tìm thấy raw: {uuid}")
                    continue

                cursor.execute(
                    """
                    INSERT INTO timedealer.message_items 
                    (unique_id, transaction_type, ref, brand, color, price, currency, year, condition, note)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        unique_id,
                        row["transaction"],
                        row["ref"],
                        row["brand"],
                        row["color"],
                        float(row["price"]) if row["price"] else None,
                        row["currency"],
                        row["year"],
                        row["condition"],
                        row["note"] if row["note"] != "null" else None,
                    ),
                )
                success += 1
            except Exception as e:
                print(f"❌ Lỗi khi insert row {row}: {e}")
                fail += 1

    conn.commit()
    conn.close()
    return success, fail


if __name__ == "__main__":
    # 1. Load raw messages
    file_path = r"C:\timedealer_data\data\2025_export_data.json"
    with open(file_path, "r", encoding="utf-8") as f:
        raw_list = json.load(f)
    print(f"Has {len(raw_list)} raw messages")

    uuid_to_unique = {}
    for raw in tqdm(raw_list):
        mapping = insert_raw(raw)
        uuid_to_unique[mapping["uuid"]] = mapping["unique_id"]

    file_path_uuid = r"C:\timedealer_data\data\uuid2unique.json"
    with open(file_path_uuid, "w", encoding="utf-8") as f:
        json.dump(uuid_to_unique, f, ensure_ascii=False, indent=4)

    # 2. Insert items from CSV
    with open(file_path_uuid, "r", encoding="utf-8") as f:
        uuid_to_unique = json.load(f)

    success, fail = insert_items_from_csv(r"C:\timedealer_data\data\items.csv", uuid_to_unique)
    print(f"✅ Success: {success}\n❌ Fail: {fail}")
