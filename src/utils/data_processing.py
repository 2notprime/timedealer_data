import sqlite3
import hashlib
from datetime import datetime, timedelta

DB_PATH = "my_local.db"

def extract_message(message: str) -> list[dict]:
    # Xử lý message ở đây
    return message

def insert_message_api(item: dict):
    """
    Chèn raw message, update messages_unique và insert items.
    Trả về dict để API biết trạng thái.
    """
    message = item['message']
    group_name = item['groupName']
    sender_name = item['senderName']
    sender_phone = item['senderPhone']
    time = item['time']  # datetime object
    image = item.get('image', '')

    hash_message = hashlib.sha256(message.encode()).hexdigest()
    phone_message_hash = hashlib.sha256((sender_phone + message).encode()).hexdigest()
    seven_days_ago = time - timedelta(days=7)

    response = {
        "unique_id": None,
        "updated_status": None,
        "raw_inserted": 0,
        "items_inserted": 0,
        "success": False
    }

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # --- duplicate_count trong 7 ngày ---
        cursor.execute("""
            SELECT COUNT(*)
            FROM messages_raw
            WHERE phone_message_hash = :hash
            AND time BETWEEN :time_start AND :time_end
        """, {
            "hash": phone_message_hash,
            "time_start": seven_days_ago,
            "time_end": time
        })
        duplicate_count = cursor.fetchone()[0]

        # --- insert raw message ---
        cursor.execute("""
            INSERT INTO messages_raw
            (message, group_name, sender_name, sender_phone, time, image, hash_message, phone_message_hash, duplicate_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (message, group_name, sender_name, sender_phone, time, image, hash_message, phone_message_hash, duplicate_count))
        response["raw_inserted"] = cursor.rowcount

        # --- insert/update messages_unique ---
        cursor.execute("""
            INSERT INTO messages_unique (hash_message, first_seen, last_seen)
            VALUES (?, ?, ?)
            ON CONFLICT(hash_message) DO UPDATE SET
                last_seen = excluded.last_seen
            RETURNING unique_id, first_seen
        """, (hash_message, time, time))
        row = cursor.fetchone()
        unique_id, first_seen = row
        updated_status = 0 if first_seen == time else 1
        response["unique_id"] = unique_id
        response["updated_status"] = updated_status

        # --- insert items nếu message mới ---
        if updated_status == 0:
            message_items = extract_message(message) 
            for mi in message_items:
                cursor.execute("""
                    INSERT INTO message_items (
                        unique_id, transaction_type, ref, brand, color, price, currency, year, condition, note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    unique_id,
                    mi.get("transaction_type"),
                    mi.get("ref"),
                    mi.get("brand"),
                    mi.get("color"),
                    mi.get("price"),
                    mi.get("currency"),
                    mi.get("year"),
                    mi.get("condition"),
                    mi.get("note")
                ))
            response["items_inserted"] = len(message_items)

        conn.commit()
        response["success"] = True
        return response

    except Exception as e:
        # Nếu có lỗi, rollback và trả về lỗi
        conn.rollback()
        response["success"] = False
        response["error"] = str(e)
        return response

    finally:
        conn.close()
