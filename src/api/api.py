# from fastapi import FastAPI, Body, HTTPException
# from pydantic import BaseModel, Field
# from typing import Optional
# import sqlite3
# from typing import Literal


# app = FastAPI()
# DB_PATH = r"C:\timedealer_data\my_local_v2.db"
    

# class SearchRequest(BaseModel):
#     ref: Optional[str] = Field(None, example="RE")
#     transaction_type: Literal[0, 1] = Field(1, example=0, description="0: forsale, 1: wtb")
#     condition: Literal[0, 1] = Field(0, example=0, description="0: new, 1: used")
#     brand: Optional[str] = Field(None, example="Puma")
#     year: Optional[str] = Field(None, example="2021")
#     country: Optional[list[str]] = Field(None, example=["VN", "US", "DE"])
#     time_range: Optional[int] = Field(None, example=8640000)
#     price_min: Optional[float] = Field(None, example=0)
#     price_max: Optional[float] = Field(None, example=1000000000)
#     currency: Optional[str] = Field("USD", example="HKD")
#     sort_price: Literal[0, 1] = Field(None, example=0, description="0: asc, 1: desc")
#     limit: int = Field(50, example=50)
#     offset: int = Field(0, example=0)

# class TrackingRequest(BaseModel):
#     user_id: int
#     item_id: int
#     min_price: Optional[float] = None
#     max_price: Optional[float] = None

# def get_db():
#     conn = sqlite3.connect(DB_PATH)
#     conn.row_factory = sqlite3.Row
#     return conn

# @app.post("/search_items")
# def search_items(body: SearchRequest = Body(...)):
#     base_query = "FROM message_items mi JOIN messages_raw mr ON mi.message_id = mr.id"
#     params = []

#     if body.transaction_type:
#         if body.transaction_type == 0:
#             transaction_type = "forsale"
#         else:
#             transaction_type = "wtb"
#         base_query += " AND mi.transaction_type = ?"
#         params.append(transaction_type)

#     if body.condition:
#         if body.condition == 0:
#             condition = "new"
#         else:
#             condition = "used"
#         base_query += " AND mi.condition = ?"
#         params.append(condition)

#     if body.brand:
#         base_query += " AND mi.brand = ?"
#         params.append(body.brand)

#     if body.year:
#         base_query += " AND mi.year = ?"
#         params.append(body.year)

#     if body.price_min is not None:
#         base_query += " AND mi.price >= ?"
#         params.append(body.price_min)

#     if body.price_max is not None:
#         base_query += " AND mi.price <= ?"
#         params.append(body.price_max)

#     if body.currency:
#         base_query += " AND mi.currency = ?"
#         params.append(body.currency)

#     if body.ref:
#         base_query += " AND mi.ref LIKE ?"
#         params.append(f"%{body.ref}%")
    
#     if body.time_range:
#         base_query += " AND mr.posted_time >= datetime('now', '-' || ? || ' seconds')"
#         params.append(body.time_range)

#     if body.country:
#         base_query += " AND mi.country IN ({})".format(", ".join("?" for _ in body.country))
#         params.extend(body.country)
    
    

#     # Kết nối DB
#     conn = get_db()
#     cursor = conn.cursor()

#     # SELECT dữ liệu
#     query = f"""
#     SELECT 
#             mi.item_id,
#             mi.message_id,
#             mr.message,
#             mr.sender_name,
#             mr.sender_phone,
#             mi.transaction_type,
#             mi.ref,
#             mi.brand,
#             mi.color,
#             mi.price,
#             mi.country,
#             mi.currency,
#             mi.year,
#             mi.condition,
#             mi.note,
#             mr.posted_time {base_query}"""
    
#     if body.sort_price:
#         if body.sort_price == 0:
#             query += f" ORDER BY mi.price ASC"
#         else:
#             query += f" ORDER BY mi.price DESC"
#     else:
#         query += f" ORDER BY mr.posted_time DESC"

#     if body.limit:
#         query += f" LIMIT {int(body.limit)}"
        
#     if body.offset:
#         query += f" OFFSET {int(body.offset)}"

#     cursor.execute(query, params)
#     results = cursor.fetchall()
    

#     # Trả về dạng dict
#     columns = [col[0] for col in cursor.description]
#     items = [dict(zip(columns, row)) for row in results]
#     total = len(items)
#     conn.close()

#     return {"total": total, "items": items}

# @app.post("/tracking/add")
# def add_tracking(req: TrackingRequest):
#     conn = get_db()
#     cursor = conn.cursor()
#     try:
#         # lấy thông tin item gốc từ message_items
#         cursor.execute("""
#             SELECT transaction_type, ref, brand, color, price, country, currency, year, condition
#             FROM message_items
#             WHERE item_id = ?
#         """, (req.item_id,))
#         row = cursor.fetchone()

#         if not row:
#             conn.close()
#             raise HTTPException(status_code=404, detail="Item not found in message_items")

#         transaction_type, ref, brand, color, price, country, currency, year, condition = row
#         if not req.min_price:
#             req.min_price = price if price else None
#         if not req.max_price:
#             req.max_price = price if price else None

#         cursor.execute("""
#             INSERT INTO tracking_items (user_id, item_id, tracking_time, transaction_type, ref, brand, color, min_price, max_price, country, currency, year, condition)
#             VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             ON CONFLICT(user_id, item_id) DO UPDATE SET tracking_time = CURRENT_TIMESTAMP
#         """, (req.user_id, req.item_id, transaction_type, ref, brand, color, req.min_price, req.max_price, country, currency, year, condition))
#         existed = False if cursor.lastrowid else True
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         conn.close()
#     return {"status": "success", "message": f"Tracking item {'added' if not existed else 'updated'} successfully."}

# @app.delete("/tracking/remove")
# def remove_tracking(req: TrackingRequest):
#     conn = get_db()
#     cursor = conn.cursor()
#     try:
#         cursor.execute("SELECT tracking_id FROM tracking_items WHERE user_id = ? AND item_id = ?", (req.user_id, req.item_id))
#         rows = cursor.fetchall()
        
#         tracking_ids = [row[0] for row in rows]
        
#         if tracking_ids:
#             cursor.executemany("DELETE FROM tracking_results WHERE tracking_id = ?", [(tid,) for tid in tracking_ids])
#             cursor.execute("DELETE FROM tracking_items WHERE user_id = ? AND item_id = ?", (req.user_id, req.item_id))
#         else:
#             raise HTTPException(status_code=404, detail="No tracking found for the given user_id and item_id.")
        
#         conn.commit()
#     except HTTPException:
#         # nếu là HTTPException thì không wrap lại thành 500, cứ raise luôn
#         raise
#     except Exception as e:
#         conn.rollback()
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         conn.close()

#     return {"status": "success", "message": "Tracking item removed successfully."}

import os
from fastapi import FastAPI, APIRouter, Body, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Literal
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

router = APIRouter()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "prefer"),
}

SCHEMA = "timedealer"


class SearchRequest(BaseModel):
    ref: Optional[str] = Field(None, example="RE")
    transaction_type: Literal[0, 1] = Field(0, example=0, description="0: forsale, 1: wtb")
    condition: Literal[0, 1] = Field(0, example=0, description="0: new, 1: used")
    brand: Optional[str] = Field(None, example="Puma")
    year: Optional[str] = Field(None, example="2021")
    country: Optional[list[str]] = Field(None, example=["VN", "US", "DE"])
    time_range: Optional[int] = Field(None, example=8640000)
    price_min: Optional[float] = Field(None, example=0)
    price_max: Optional[float] = Field(None, example=1000000000)
    currency: Optional[str] = Field(None, example="HKD")
    sort_price: Optional[Literal[0, 1]] = Field(None, example=0, description="0: asc, 1: desc")
    limit: int = Field(50, example=50)
    offset: int = Field(0, example=0)


class TrackingRequest(BaseModel):
    user_id: int
    item_id: int
    min_price: Optional[float] = None
    max_price: Optional[float] = None


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.cursor().execute(f"SET search_path TO {SCHEMA};")
    return conn

@router.get("/items/{item_id}")
def get_item(item_id: int):
    query = f"""
    SELECT 
        mi.item_id,
        mi.message_id,
        mr.message,
        mr.sender_name,
        mr.sender_phone,
        mi.transaction_type,
        mi.ref,
        mi.brand,
        mi.color,
        mi.price,
        mi.country,
        mi.currency,
        mi.year,
        mi.condition,
        mi.note,
        mr.posted_time
        FROM message_items mi
        JOIN messages_raw mr ON mi.message_id = mr.id
        WHERE mi.item_id = %s
    """
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (item_id,))
            item = cursor.fetchone()
        if not item:
            raise HTTPException(status_code=404, detail="Item not found")
    finally:
        conn.close()
    return item

@router.post("/search_items")
def search_items(body: SearchRequest = Body(...)):
    base_query = f"FROM message_items mi JOIN messages_raw mr ON mi.message_id = mr.id WHERE 1=1"
    params = []

    if body.transaction_type is not None:
        transaction_type = "forsale" if body.transaction_type == 0 else "wtb"
        base_query += " AND mi.transaction_type = %s"
        params.append(transaction_type)

    if body.condition is not None:
        condition = "new" if body.condition == 0 else "used"
        base_query += " AND mi.condition = %s"
        params.append(condition)

    if body.brand is not None and body.brand.strip() != "":
        base_query += " AND mi.brand = %s"
        params.append(body.brand)

    if body.year is not None:
        base_query += " AND mi.year = %s"
        params.append(body.year)

    if body.price_min is not None:
        base_query += " AND mi.price >= %s"
        params.append(body.price_min)

    if body.price_max is not None:
        base_query += " AND mi.price <= %s"
        params.append(body.price_max)

    if body.currency is not None and body.currency.strip() != "":
        base_query += " AND mi.currency = %s"
        params.append(body.currency)

    if body.ref:
        base_query += " AND mi.ref LIKE %s"
        params.append(f"%{body.ref}%")

    if body.time_range:
        # PostgreSQL: now() - interval 'x seconds'
        base_query += " AND mr.posted_time >= (now() - (%s || ' seconds')::interval)"
        params.append(str(body.time_range))

    if body.country:
        placeholders = ", ".join(["%s"] * len(body.country))
        base_query += f" AND mi.country IN ({placeholders})"
        params.extend(body.country)

    query = f"""
    SELECT 
        mi.item_id,
        mi.message_id,
        mr.message,
        mr.sender_name,
        mr.sender_phone,
        mi.transaction_type,
        mi.ref,
        mi.brand,
        mi.color,
        mi.price,
        mi.country,
        mi.currency,
        mi.year,
        mi.condition,
        mi.note,
        mr.posted_time
    {base_query}
    """

    if body.sort_price is not None:
        if body.sort_price == 0:
            query += " ORDER BY mi.price ASC"
        else:
            query += " ORDER BY mi.price DESC"
    else:
        query += " ORDER BY mr.posted_time DESC"

    query += " LIMIT %s OFFSET %s"
    params.extend([body.limit, body.offset])

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            items = cursor.fetchall()
        total = len(items)
    finally:
        conn.close()

    return {"total": total, "items": items}


@router.post("/tracking/add")
def add_tracking(req: TrackingRequest):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT transaction_type, ref, brand, color, price, country, currency, year, condition
                FROM message_items
                WHERE item_id = %s
            """, (req.item_id,))
            row = cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Item not found in message_items")

            transaction_type, ref, brand, color, price, country, currency, year, condition = row
            if req.min_price is None:
                req.min_price = price if price else None
            if req.max_price is None:
                req.max_price = price if price else None

            cursor.execute(f"""
                INSERT INTO tracking_items 
                (user_id, item_id, tracking_time, transaction_type, ref, brand, color, min_price, max_price, country, currency, year, condition)
                VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(user_id, item_id) 
                DO UPDATE SET tracking_time = CURRENT_TIMESTAMP
                RETURNING tracking_id
            """, (req.user_id, req.item_id, transaction_type, ref, brand, color, req.min_price, req.max_price, country, currency, year, condition))
            tracking_id = cursor.fetchone()[0]
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {"status": "success", "tracking_id": tracking_id, "message": "Tracking item added/updated successfully."}


@router.delete("/tracking/remove")
def remove_tracking(req: TrackingRequest):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT tracking_id 
                FROM tracking_items 
                WHERE user_id = %s AND item_id = %s
            """, (req.user_id, req.item_id))
            rows = cursor.fetchall()

            tracking_ids = [r[0] for r in rows]

            if tracking_ids:
                cursor.executemany(
                    f"DELETE FROM tracking_results WHERE tracking_id = %s",
                    [(tid,) for tid in tracking_ids]
                )
                cursor.execute(
                    f"DELETE FROM tracking_items WHERE user_id = %s AND item_id = %s RETURNING tracking_id",
                    (req.user_id, req.item_id)
                )
                tracking_id = cursor.fetchone()[0]
            else:
                raise HTTPException(status_code=404, detail="No tracking found for the given user_id and item_id.")

        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {"status": "success", "tracking_id": tracking_id, "message": "Tracking item removed successfully."}
