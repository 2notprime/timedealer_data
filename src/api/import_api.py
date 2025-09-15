import os
import sys
import uuid
import psycopg2
import redis
import json
import time
from fastapi import FastAPI, APIRouter, Request, HTTPException, BackgroundTasks, Body
from pydantic import BaseModel
from typing import List
from rq import Queue
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import logging

# Cho phép import từ thư mục cha
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from utils.import_messages import process_and_insert_messages
from service.import_worker import import_messages_job
from utils.import_messages_dbes import process_and_insert_messages

src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
logging_file = os.path.join(src_dir, "logs", "import_message_results.log")

load_dotenv()

# Config Redis
redis_conn = redis.Redis(host="127.0.0.1", port=6379, db=0)
q = Queue("default", connection=redis_conn)

# Config DB
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE"),
}

# Config Elasticsearch
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "datatimedealer")
ES_INDEX = os.getenv("ES_INDEX", "message_items")

es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASSWORD))

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

def import_messages_task(data_dicts, job_id=None):
    conn = get_conn()
    try:
        # all_items = process_and_insert_messages(data_dicts, conn)
        all_items = process_and_insert_messages(data_dicts, conn, es_client=es, es_index=ES_INDEX)
        result = {"job_id": job_id, "processed": len(all_items)}
        print(all_items)
        logger.info(json.dumps(result, ensure_ascii=False))
    except Exception as e:
        conn.rollback()
        error = {"job_id": job_id, "error": str(e)}
        logger.info(json.dumps(error, ensure_ascii=False))
    finally:
        conn.close()

# @router.post("/import_messages")
# async def import_messages(data: List[MessageRaw] = Body(..., max_length=100000000)):
#     conn = get_conn()
#     try:
#         # convert Pydantic objects to list of dicts
#         data_dicts = [msg.dict() for msg in data]

#         all_items = process_and_insert_messages(data_dicts, conn)
#         logger.info(f"Imported {len(all_items)} messages")
#         return {"status": "ok", "processed": len(all_items)}
#     except Exception as e:
#         conn.rollback()
#         logger.error(f"Import failed: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         conn.close()

# @router.post("/import_messages")
# async def import_messages(data: List[MessageRaw] = Body(..., max_length=100000000)):
#     try:
#         data_dicts = [msg.dict() for msg in data]
#         job_id = str(uuid.uuid4())
#         job = q.enqueue(import_messages_job, data_dicts, job_id=job_id)
#         return {"status": "queued", "job_id": job.id, "queued": len(data_dicts)}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

@router.post("/import_messages")
async def import_messages(data: List[MessageRaw] = Body(..., max_length=100000000), background_tasks: BackgroundTasks=None):
    data_dicts = [msg.dict() for msg in data]
    job_id = str(int(time.time() * 1000))  # ID tạm thời, timestamp
    background_tasks.add_task(import_messages_task, data_dicts, job_id)
    return {"status": "processing", "job_id": job_id}
    
