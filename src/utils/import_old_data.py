import sqlite3
import hashlib
from datetime import datetime
import csv
from tqdm import tqdm

DB_PATH = "my_local_v1.db"


def get_unique_id(cursor, hash_message):
    cursor.execute(
        "SELECT unique_id FROM messages_unique WHERE hash_message = ?", (hash_message,)
    )
    row = cursor.fetchone()
    if row:
        cursor.execute(
            "UPDATE messages_unique SET last_seen = CURRENT_TIMESTAMP WHERE unique_id = ?",
            (row[0],),
        )
        return row[0]
    else:
        cursor.execute(
            "INSERT INTO messages_unique (hash_message) VALUES (?)", (hash_message,)
        )
        return cursor.lastrowid


def insert_raw(raw: dict):
    conn = sqlite3.connect(DB_PATH)
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

    hash_message = hashlib.sha256(message.encode("utf-8")).hexdigest()
    phone_message_hash = hashlib.sha256(
        (sender_phone + message).encode("utf-8")
    ).hexdigest()

    cursor.execute(
        """
        INSERT OR IGNORE INTO messages_raw 
        (message, group_name, sender_name, sender_phone, time, image, hash_message, phone_message_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        success = 0
        fail = 0
        for row in tqdm(reader):
            try:
                uuid = row["Uuid"]
                unique_id = uuid_to_unique.get(uuid)
                if not unique_id:
                    print(f"⚠️ Bỏ qua item vì không tìm thấy raw: {uuid}")
                    continue

                cursor.execute(
                    """
                    INSERT INTO message_items 
                    (unique_id, transaction_type, ref, brand, color, price, currency, year, condition, note)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            except:
                fail += 1

    conn.commit()
    conn.close()
    return success, fail

import json

file_path = r"C:\timedealer_data\data\2025_export_data.json"

with open(file_path, "r", encoding="utf-8") as f:
    raw_list = json.load(f) 
n = len(raw_list)
print(f"Has {n} raw messages")
uuid_to_unique = {}
for raw in tqdm(raw_list):   # raw_list là list JSON bạn có
    mapping = insert_raw(raw)
    uuid_to_unique[mapping["uuid"]] = mapping["unique_id"]
    
file_path_uuid = r"C:\timedealer_data\data\uuid2unique.json"

with open(file_path_uuid, "w", encoding="utf-8") as f:
    json.dump(uuid_to_unique, f, ensure_ascii=False, indent=4)


with open(file_path_uuid, "r", encoding="utf-8") as f:
    uuid_to_unique = json.load(f) 
success, fail = insert_items_from_csv(r"C:\timedealer_data\data\items.csv", uuid_to_unique) 
print(f"Success: {success}\nFail: {fail}")   