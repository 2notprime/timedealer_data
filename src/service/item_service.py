import os
import sys
from fastapi import FastAPI, APIRouter, Body, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Literal
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.map_code2dial import code_to_dial
from utils.exchange_currency import get_exchange_rate_usd

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

def query_items(body):
    base_query = f"FROM message_items mi JOIN messages_raw mr ON mi.message_id = mr.id WHERE 1=1 AND mi.currency IS NOT NULL AND mi.price IS NOT NULL"
    params = []
    message = []

    if body.transaction_type is not None:
        transaction_type = "forsale" if body.transaction_type == 0 else "wtb"
        base_query += " AND mi.transaction_type = %s"
        params.append(transaction_type)

    if body.condition is not None:
        if body.condition == 0:
            condition = ["new"]
        elif body.condition == 1:
            condition = ["used"]
        elif body.condition == 2:
            condition = ["new", "used"]  # Both new and used
        else:
            condition = []
        base_query += " AND mi.condition = ANY(%s)"
        params.append(condition)

    if body.brand is not None and body.brand.strip() != "":
        base_query += " AND mi.brand = %s"
        params.append(body.brand)

    if body.year is not None and len(body.year) == 2:
        start_year, end_year = body.year
        base_query += " AND EXTRACT(YEAR FROM mi.release_date) BETWEEN %s AND %s"
        params.extend([start_year, end_year])
    
    if body.currency is not None and body.currency.strip() != "":
        if body.price_min is not None:
            try:
                price_min = float(body.price_min) / get_exchange_rate_usd(body.currency)
                base_query += " AND mi.usd_price >= %s"
                params.append(price_min)
            except:
                message.append(f"Invalid currency code: {body.currency}")
        if body.price_max is not None:
            try:
                price_max = float(body.price_max) / get_exchange_rate_usd(body.currency)
                base_query += " AND mi.usd_price <= %s"
                params.append(price_max)
            except:
                message.append(f"Invalid currency code: {body.currency}")
            
    # if body.using_usd is not None and body.using_usd == 0:
    #     if body.price_min is not None:
    #         base_query += " AND mi.price >= %s"
    #         params.append(body.price_min)

    #     if body.price_max is not None:
    #         base_query += " AND mi.price <= %s"
    #         params.append(body.price_max)
    elif body.using_usd is not None and body.using_usd == 1:
        if body.price_min is not None:
            base_query += " AND mi.usd_price >= %s"
            params.append(body.price_min)

        if body.price_max is not None:
            base_query += " AND mi.usd_price <= %s"
            params.append(body.price_max)

    if body.ref:
        base_query += " AND mi.ref LIKE %s"
        params.append(f"%{body.ref}%")

    if body.time_range:
        # PostgreSQL: now() - interval 'x seconds'
        base_query += " AND mr.posted_time >= (now() - (%s || ' seconds')::interval)"
        params.append(str(body.time_range))

    # if body.country:
    #     placeholders = ", ".join(["%s"] * len(body.country))
    #     base_query += f" AND mi.country IN ({placeholders})"
    #     params.extend(body.country)

    if body.country:
        dial_codes = []
        for c in body.country:
            dial_codes.extend(code_to_dial.get(c, []))

        if dial_codes:
            placeholders = " OR ".join(["mr.sender_phone LIKE %s"] * len(dial_codes))
            base_query += f" AND ({placeholders})"
            params.extend([d + "%" for d in dial_codes])

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
    {base_query}
    """

    if body.sort_price is not None:
        if body.sort_price == 0:
            query += " ORDER BY mi.usd_price ASC"
        else:
            query += " ORDER BY mi.usd_price DESC"
    
        query += " ,mr.posted_time DESC"
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

    return {"total": total, "items": items, "message": message}