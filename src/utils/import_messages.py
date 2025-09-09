# import hashlib
# from typing import List, Dict
# from utils.extract_messages import analyze_message


# def process_and_insert_messages(data: List[Dict], conn) -> int:
#     """
#     Xử lý danh sách messages và insert vào DB (schema timedealer).
#     - Hash message và hash phone+message
#     - Insert vào messages_unique nếu mới
#     - Insert vào messages_raw
#     - Nếu là message mới -> chạy extract và insert items
#     - Nếu message đã có -> copy items từ bản cũ sang
#     - Duplicate count tính theo (phone_message_hash, 7 ngày gần nhất)
#     """
#     cur = conn.cursor()
#     inserted = 0

#     for msg in data:
#         message = msg.get("message", "")
#         sender_phone = msg.get("senderPhone", "")

#         # Hash để check trùng
#         hash_message = hashlib.sha256(message.encode()).hexdigest()
#         phone_message_hash = hashlib.sha256((sender_phone + message).encode()).hexdigest()

#         # Check message unique theo hash_message
#         cur.execute("""
#             SELECT unique_id FROM timedealer.messages_unique WHERE hash_message = %s
#         """, (hash_message,))
#         unique_row = cur.fetchone()

#         # Tính duplicate_count (trong vòng 7 ngày theo phone+message)
#         cur.execute("""
#             SELECT COUNT(*) 
#             FROM timedealer.messages_raw
#             WHERE phone_message_hash = %s 
#               AND posted_time >= NOW() - INTERVAL '7 days';
#         """, (phone_message_hash,))
#         dup_count = cur.fetchone()[0]

#         if unique_row is None:
#             # ✅ chưa có → thêm vào unique
#             cur.execute("""
#                 INSERT INTO timedealer.messages_unique (hash_message, first_seen, last_seen)
#                 VALUES (%s, NOW(), NOW())
#                 RETURNING unique_id;
#             """, (hash_message,))
#             unique_id = cur.fetchone()[0]

#             # Insert raw
#             cur.execute("""
#                 INSERT INTO timedealer.messages_raw
#                     (message, group_name, sender_name, sender_phone, posted_time, image,
#                      hash_message, phone_message_hash, duplicate_count)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                 RETURNING id;
#             """, (
#                 message,
#                 msg.get("groupName"),
#                 msg.get("senderName"),
#                 sender_phone,
#                 msg.get("time"),
#                 msg.get("image"),
#                 hash_message,
#                 phone_message_hash,
#                 dup_count
#             ))
#             message_id = cur.fetchone()[0]

#             # Extract items thật
#             items = analyze_message(message)
#             for item in items:
#                 cur.execute("""
#                     INSERT INTO timedealer.message_items
#                         (message_id, transaction_type, ref, brand, color, price, country,
#                          currency, year, condition, note)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#                 """, (
#                     message_id,
#                     item.get("transaction"),
#                     item.get("ref"),
#                     item.get("brand"),
#                     item.get("color"),
#                     float(item["price"]) if item.get("price") else None,
#                     item.get("country"),
#                     item.get("currency"),
#                     item.get("year"),
#                     item.get("condition"),
#                     item.get("note")
#                 ))

#         else:
#             # ✅ đã có unique rồi
#             unique_id = unique_row[0]

#             # Update last_seen
#             cur.execute("""
#                 UPDATE timedealer.messages_unique
#                 SET last_seen = NOW()
#                 WHERE unique_id = %s
#             """, (unique_id,))

#             # Lấy message_id cũ (bản đầu tiên theo hash_message)
#             cur.execute("""
#                 SELECT id FROM timedealer.messages_raw
#                 WHERE hash_message = %s
#                 ORDER BY posted_time ASC
#                 LIMIT 1;
#             """, (hash_message,))
#             old_message_id = cur.fetchone()[0]

#             # Insert bản raw mới
#             cur.execute("""
#                 INSERT INTO timedealer.messages_raw
#                     (message, group_name, sender_name, sender_phone, posted_time, image,
#                      hash_message, phone_message_hash, duplicate_count)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#                 RETURNING id;
#             """, (
#                 message,
#                 msg.get("groupName"),
#                 msg.get("senderName"),
#                 sender_phone,
#                 msg.get("time"),
#                 msg.get("image"),
#                 hash_message,
#                 phone_message_hash,
#                 dup_count
#             ))
#             new_message_id = cur.fetchone()[0]

#             # Copy items từ bản cũ sang bản mới
#             cur.execute("""
#                 INSERT INTO timedealer.message_items
#                     (message_id, transaction_type, ref, brand, color, price, country,
#                      currency, year, condition, note)
#                 SELECT %s, transaction_type, ref, brand, color, price, country,
#                        currency, year, condition, note
#                 FROM timedealer.message_items
#                 WHERE message_id = %s;
#             """, (new_message_id, old_message_id))

#         inserted += 1

#     conn.commit()
#     cur.close()
#     return inserted

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
            try:
                posted_time = datetime.fromisoformat(time_str)
            except ValueError:
                try:
                    posted_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    posted_time = datetime.now()  # fallback
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
                    usd_price = float(price) / get_exchange_rate_usd(currency)
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
            cur.execute("""
                SELECT id FROM timedealer.messages_raw
                WHERE hash_message = %s
                ORDER BY posted_time ASC
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
