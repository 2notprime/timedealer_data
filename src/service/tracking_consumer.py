import os
import time
import json
import redis
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Redis config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# PostgreSQL config
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "prefer"),
}

def get_db_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.cursor().execute("SET search_path TO timedealer;")
    return conn

def process_item(item, cur):
    """
    item = [id, message_id, transaction_type, ref, brand, color, price, country, currency, year, condition, note]
    """
    conditions = []
    params = []
    print("Processing item: ID ", item[0])
    # brand
    if item[4]:
        conditions.append("brand = %s")
        params.append(item[4])

    # ref
    if item[3]:
        conditions.append("ref = %s")
        params.append(item[3])

    # price ±5%
    if item[6] is not None:
        conditions.append("min_price <= %s")
        conditions.append("max_price >= %s")
        params.extend([item[6], item[6]])

    # condition
    if item[10]:
        conditions.append("condition = %s")
        params.append(item[10])

    # currency
    if item[8]:
        conditions.append("currency = %s")
        params.append(item[8])

    # country
    if item[7]:
        conditions.append("country = %s")
        params.append(item[7])

    # year
    if item[9]:
        conditions.append("year = %s")
        params.append(item[9])

    # transaction_type
    if item[2]:
        conditions.append("transaction_type = %s")
        params.append(item[2])

    # dynamic query
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    query = f"SELECT tracking_id, item_id FROM tracking_items WHERE {where_clause};"
    print(query)
    print(params)
    cur.execute(query, params)
    matches = cur.fetchall()
    print(f"Found {len(matches)} matches for item ID {item[0]}")

    # insert vào tracking_results nếu match
    for match in matches:
        tracking_id, tracking_item_id = match
        try:
            cur.execute("""
                INSERT INTO tracking_results (tracking_id, item_id)
                VALUES (%s, %s)
                ON CONFLICT (tracking_id, item_id) DO NOTHING
                RETURNING id
            """, (tracking_id, item[0]))
            result = cur.fetchone()
            if result:
                print(f"Inserted successfully: tracking_result_id = {result[0]}")
            else:
                print(f"Already exists, skipped: tracking_id = {tracking_id}, item_id = {item[0]}")
        except Exception as e:
            print(f"Insert failed: {e}")

def main_loop():
    conn = get_db_conn()
    cur = conn.cursor()

    print("Consumer started, waiting for items...")
    try:
        while True:
            item_json = r.lpop("tracking_queue")
            if not item_json:
                # Queue rỗng → đợi 5 giây
                time.sleep(5)
                continue

            try:
                item = json.loads(item_json)
            except json.JSONDecodeError:
                print(f"Invalid JSON: {item_json}")
                continue

            process_item(item, cur)
            conn.commit()
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main_loop()
