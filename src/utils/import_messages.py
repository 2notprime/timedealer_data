import hashlib
from typing import List, Dict
from datetime import datetime, timezone, timedelta
from utils.extract_messages import analyze_message
from utils.exchange_currency import get_exchange_rate_usd
from utils.preprocessing_data import parse_date

def process_and_insert_messages(data: List[Dict], conn) -> List[Dict]:
    """
    Xử lý danh sách messages và insert vào DB (schema timedealer).
    - Hash message và hash phone+message
    - Insert vào messages_unique nếu mới
    - Insert vào messages_raw
    - Nếu là message mới -> chạy extract và insert items
    - Nếu message đã có -> copy items từ bản cũ sang
    - Duplicate count tính theo (phone_message_hash, 7 ngày gần nhất)
    - Trả về danh sách tất cả item insert (cả mới và copy)
    """
    cur = conn.cursor()
    all_items = []

    for msg in data:
        message = msg.get("message", "")
        sender_phone = msg.get("senderPhone", "")

        # parse time
        time_str = msg.get("time")
        posted_time = None
        if time_str:
            # try:
            #     posted_time = datetime.fromisoformat(time_str)
            # except ValueError:
            #     try:
            #         posted_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            #     except ValueError:
            #         posted_time = datetime.now()  # fallback
            try:
                posted_time = datetime.fromisoformat(time_str)
                if posted_time.tzinfo is None:  # nếu thiếu tzinfo
                    posted_time = posted_time.replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    posted_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    posted_time = posted_time.replace(tzinfo=timezone.utc)
                except ValueError:
                    posted_time = datetime.now(timezone.utc)  # fallback
        print(posted_time)

        # Hash để check trùng
        hash_message = hashlib.sha256(message.encode()).hexdigest()
        phone_message_hash = hashlib.sha256((sender_phone + message).encode()).hexdigest()

        # Check message unique
        cur.execute("SELECT unique_id FROM timedealer.messages_unique WHERE hash_message = %s", (hash_message,))
        row = cur.fetchone()
        unique_id = row[0] if row else None

        # Tính duplicate_count
        cur.execute("""
            SELECT COUNT(*) 
            FROM timedealer.messages_raw
            WHERE phone_message_hash = %s 
              AND posted_time >= NOW() - INTERVAL '7 days';
        """, (phone_message_hash,))
        row = cur.fetchone()
        dup_count = row[0] if row else 0

        if unique_id is None:
            # Thêm vào messages_unique
            cur.execute("""
                INSERT INTO timedealer.messages_unique (hash_message, first_seen, last_seen)
                VALUES (%s, NOW(), NOW())
                RETURNING unique_id;
            """, (hash_message,))
            row = cur.fetchone()
            unique_id = row[0] if row else None

        # Insert vào messages_raw
        cur.execute("""
            INSERT INTO timedealer.messages_raw
                (message, group_name, sender_name, sender_phone, posted_time, image,
                 hash_message, phone_message_hash, duplicate_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (
            message,
            msg.get("groupName"),
            msg.get("senderName"),
            sender_phone,
            posted_time,
            msg.get("image"),
            hash_message,
            phone_message_hash,
            dup_count
        ))
        row = cur.fetchone()
        message_id = row[0] if row else None

        # Nếu là message mới -> extract item
        if unique_id and dup_count == 0:
            items = analyze_message(message)
            for item in items:
                currency = item.get("currency", None)
                price = item.get("price", None)
                if currency and price:
                    try:
                        usd_price = float(price) / get_exchange_rate_usd(currency)
                    except:
                        usd_price = None
                else:
                    usd_price = None    

                release_date, precision = parse_date(item.get("year", ""))
                cur.execute("""
                    INSERT INTO timedealer.message_items
                        (message_id, transaction_type, ref, brand, color, price, usd_price, country,
                         currency, release_date, condition, note, precision)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *;
                """, (
                    message_id,
                    item.get("transaction"),
                    item.get("ref"),
                    item.get("brand"),
                    item.get("color"),
                    float(price) if price else None,
                    float(usd_price) if usd_price else None,
                    item.get("country"),
                    item.get("currency"),
                    release_date,
                    item.get("condition"),
                    item.get("note"),
                    precision
                ))
                row_item = cur.fetchone()
                if row_item:
                    all_items.append(row_item)
        else:
            # Copy items từ message cũ
            # cur.execute("""
            #     SELECT id FROM timedealer.messages_raw
            #     WHERE hash_message = %s
            #     ORDER BY posted_time ASC
            #     LIMIT 1;
            # """, (hash_message,))
            cur.execute("""
                SELECT mr.id
                FROM timedealer.messages_raw mr
                WHERE mr.hash_message = %s
                AND EXISTS (SELECT 1 FROM timedealer.message_items mi WHERE mi.message_id = mr.id)
                ORDER BY mr.posted_time ASC
                LIMIT 1;
            """, (hash_message,))
            row = cur.fetchone()
            old_message_id = row[0] if row else None

            if old_message_id and message_id:
                cur.execute("""
                    INSERT INTO timedealer.message_items
                        (message_id, transaction_type, ref, brand, color, price, usd_price, country,
                         currency, release_date, condition, note, precision)
                    SELECT %s, transaction_type, ref, brand, color, price, usd_price, country,
                           currency, release_date, condition, note, precision
                    FROM timedealer.message_items
                    WHERE message_id = %s
                    RETURNING *;
                """, (message_id, old_message_id))
                copied_items = cur.fetchall()
                if copied_items:
                    all_items.extend(copied_items)

        # Update last_seen
        if unique_id:
            cur.execute("""
                UPDATE timedealer.messages_unique
                SET last_seen = NOW()
                WHERE unique_id = %s
            """, (unique_id,))

    conn.commit()
    cur.close()
    return all_items
