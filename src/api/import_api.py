import os
import sys
import psycopg2
import redis
import json
from fastapi import FastAPI, APIRouter, Request, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from typing import List, Dict
import logging

# Cho phép import từ thư mục cha
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.import_messages import process_and_insert_messages

load_dotenv()
r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# Config DB
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE"),
}

router = APIRouter()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageRaw(BaseModel):
    message: str
    groupName: str
    senderName: str
    senderPhone: str
    time: str
    image: str

def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    # conn.cursor().execute("SET search_path TO timedealer;")
    return conn


@router.post("/import_messages")
async def import_messages(data: List[MessageRaw]):
    conn = get_conn()
    try:
        # convert Pydantic objects to list of dicts
        data_dicts = [msg.dict() for msg in data]

        all_items = process_and_insert_messages(data_dicts, conn)
        for item in all_items:
            # Đẩy từng item vào queue
            r.rpush("tracking_queue", json.dumps(item))
        queue_items = r.lrange("tracking_queue", 0, -1)
        for q in queue_items:
            print(json.loads(q))
        logger.info(f"Imported {len(all_items)} messages")
        return {"status": "ok", "processed": len(all_items), "items": all_items}
    except Exception as e:
        conn.rollback()
        logger.error(f"Import failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
