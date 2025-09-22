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
    es_items: list JSON từ ES [{item_id, message_id}, ...]
    conn: connection PostgreSQL (psycopg2.connect)
    """

    if not es_items:
        return []

    message_ids = [item["message_id"] for item in es_items]

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Lấy hash_message và duplicate_count tương ứng
        # Chỉ query những message_id cần thiết
        format_strings = ",".join(["%s"] * len(message_ids))
        sql = f"""
            SELECT id AS message_id, hash_message, duplicate_count
            FROM messages_raw
            WHERE id IN ({format_strings})
        """
        cursor.execute(sql, message_ids)
        mess_rows = cursor.fetchall()

    # Python xử lý giữ message_id mới nhất theo hash
    latest_by_hash = {}
    for row in mess_rows:
        h = row["hash_message"]
        if h not in latest_by_hash or row["message_id"] > latest_by_hash[h]["message_id"]:
            latest_by_hash[h] = row

    # map lại theo message_id
    mess_dict = {row["message_id"]: row for row in latest_by_hash.values()}

    # build output
    result = []
    for item in es_items:
        mid = item["message_id"]
        if mid in mess_dict:
            row = mess_dict[mid]
            result.append({
                "item_id": item["item_id"],
                "message_id": mid,
                "hash_message": row["hash_message"],
                "duplicate_count": row["duplicate_count"],
            })

    return result
