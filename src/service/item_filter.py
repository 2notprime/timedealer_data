import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "prefer"),
}

SCHEMA = "timedealer"

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.cursor().execute(f"SET search_path TO {SCHEMA};")
    return conn

def filter_and_add_count(es_items, conn=get_db()):
    """
    Input:
        es_items: list JSON từ ES [{...,"item_id", "message_id"}]
    Output:
        list JSON đã giữ nguyên data + thêm duplicate_count,
        và chỉ giữ lại item có message_id mới nhất theo hash_message
        remove price nếu là wtb
    """

    if not es_items:
        return []

    # Lấy danh sách messid từ ES
    message_ids = [item["message_id"] for item in es_items]

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        format_strings = ",".join(["%s"] * len(message_ids))
        sql = f"""
            SELECT id AS message_id, hash_message, duplicate_count
            FROM messages_raw
            WHERE id IN ({format_strings})
        """
        cursor.execute(sql, message_ids)
        mess_rows = cursor.fetchall()

    # B1: group theo hash_message, giữ message_id lớn nhất
    latest_by_hash = {}
    for row in mess_rows:
        h = row["hash_message"]
        if h not in latest_by_hash or row["message_id"] > latest_by_hash[h]["message_id"]:
            latest_by_hash[h] = row

    # B2: build dict {message_id: duplicate_count}
    valid_message_ids = {row["message_id"]: row for row in latest_by_hash.values()}

    # B3: duyệt ES items → chỉ giữ item nào gắn với messid mới nhất
    result = []
    for item in es_items:
        mid = item["message_id"]
        if mid in valid_message_ids:
            row = valid_message_ids[mid]
            enriched_item = dict(item)  # copy giữ nguyên data
            enriched_item["duplicate_count"] = row["duplicate_count"]
            if "transaction_type" in enriched_item and enriched_item["transaction_type"] == "wtb":
                enriched_item.pop("price", None)
                enriched_item.pop("usd_price", None)
                enriched_item.pop("currency", None)
            result.append(enriched_item)

    return result