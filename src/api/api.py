import os
import sys
from fastapi import FastAPI, APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, Literal
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Cho phép import từ thư mục cha
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from service.item_service import query_items

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
    ref: Optional[str] = Field(None, example="155")
    transaction_type: Literal[0, 1] = Field(0, example=0, description="0: forsale, 1: wtb")
    condition: Literal[0, 1] = Field(0, example=0, description="0: new, 1: used")
    brand: Optional[str] = Field(None, example="Audemars Piguet")
    year: Optional[list[int]] = Field(None, example=[2000, 2025])
    country: Optional[list[str]] = Field(None, example=["VN", "US", "DE", "HK"])
    time_range: Optional[int] = Field(None, example=8640000)
    price_min: Optional[float] = Field(None, example=0)
    price_max: Optional[float] = Field(None, example=1000000000)
    currency: Optional[str] = Field(None, example="HKD")
    sort_price: Optional[Literal[0, 1]] = Field(None, example=0, description="0: asc, 1: desc")
    limit: int = Field(50, example=50)
    offset: int = Field(0, example=0)
    using_usd: Literal[0, 1] = Field(0, example=0, description="0: không sử dụng USD, 1: sử dụng USD")


class TrackingRequest(BaseModel):
    user_id: int
    ref: str = Field(..., example="RE")
    transaction_type: Literal[0, 1] = Field(0, example=0, description="0: forsale, 1: wtb")
    year: Optional[str] = Field(None, example="2021")
    condition: Literal[0, 1] = Field(0, example=0, description="0: new, 1: used")
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
        CASE mi.precision
            WHEN 'year'  THEN TO_CHAR(mi.release_date, 'YYYY')
            WHEN 'month' THEN TO_CHAR(mi.release_date, 'YYYY-MM')
            WHEN 'day'   THEN TO_CHAR(mi.release_date, 'YYYY-MM-DD')
        END AS release_date,
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
    return query_items(body)


@router.post("/tracking/add")
def add_tracking(req: TrackingRequest):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            # map transaction_type và condition sang text
            transaction_type = "forsale" if req.transaction_type == 0 else "wtb"
            condition = None
            if req.condition is not None:
                condition = "new" if req.condition == 0 else "used"

            cursor.execute("""
                INSERT INTO tracking_queries 
                (user_id, tracking_time, transaction_type, ref, min_price, max_price, year, condition, count)
                VALUES (%s, (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'), %s, %s, %s, %s, %s, %s, 0)
                ON CONFLICT(user_id, transaction_type, ref, min_price, max_price, year, condition)
                DO UPDATE SET 
                    tracking_time = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                RETURNING tracking_id
            """, (
                req.user_id,
                transaction_type,
                req.ref,
                req.min_price,
                req.max_price,
                req.year,
                condition
            ))
            tracking_id = cursor.fetchone()[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {
        "status": "success",
        "tracking_id": tracking_id,
        "message": "Tracking query added/updated successfully."
    }

@router.post("/tracking/update")
def update_tracking(tracking_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE tracking_queries
                SET tracking_time = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                WHERE tracking_id = %s
                RETURNING tracking_id, tracking_time
            """, (tracking_id,))
            row = cursor.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Tracking ID not found")

        conn.commit()
        return {
            "status": "success",
            "tracking_id": row[0],
            "tracking_time": row[1],
            "message": "Tracking time updated successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@router.delete("/tracking/remove")
def remove_tracking(tracking_id: int = Query(..., description="ID of the tracking query to remove")):
    conn = get_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM tracking_queries WHERE tracking_id = %s RETURNING tracking_id",
                (tracking_id,)
            )
            deleted = cursor.fetchone()
            if not deleted:
                raise HTTPException(status_code=404, detail="Tracking query not found")
        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    return {
        "status": "success",
        "tracking_id": tracking_id,
        "message": "Tracking query removed successfully."
    }

@router.get("/tracking/list")
def list_tracking(user_id: int = Query(..., description="ID of the user to list tracking queries for")):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT * FROM tracking_queries WHERE user_id = %s
            """, (user_id,))
            items = cursor.fetchall()
    finally:
        conn.close()
    return items

@router.get("/tracking/matching_items")
def list_matching_items(tracking_id: int = Query(..., description="ID of the tracking query to list matching items for"),
                        limit: int = Query(50, description="Number of items to return"), 
                        offset: int = Query(0)):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT tracking_id, user_id, transaction_type, ref, min_price, max_price, year, condition,
                EXTRACT(EPOCH FROM (now() - tracking_time))::int AS time_range
                FROM tracking_queries
                WHERE tracking_id = %s
                """,
                (tracking_id,)
            )   
            tracking = cursor.fetchone()

        if not tracking:
            raise HTTPException(status_code=404, detail="Tracking query not found")

    finally:
        conn.close()

    # map sang "body" giả lập SearchRequest
    class TrackingSearchBody:
        def __init__(self, row):
            self.transaction_type = 0 if row["transaction_type"] == "forsale" else 1 if row["transaction_type"] == "wtb" else None
            self.condition = 0 if row["condition"] == "new" else 1 if row["condition"] == "used" else None
            self.ref = row["ref"]
            self.year = row["year"]
            self.price_min = row["min_price"]
            self.price_max = row["max_price"]
            self.time_range = row["time_range"]
            
            self.limit = 50
            self.offset = 0
            self.using_usd = 1
            self.brand = None
            self.currency = None
            self.country = None
            self.sort_price = None

    body = TrackingSearchBody(tracking)
    return query_items(body)