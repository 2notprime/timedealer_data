import json
import logging
import os
import sys
import psycopg2
import json
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import logging

# Cho phép import từ thư mục cha
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.import_messages_dbes import process_and_insert_messages
from service.celery_app import celery

src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
log_dir = os.path.join(src_dir, "logs")
os.makedirs(log_dir, exist_ok=True) 

logging_file = os.path.join(log_dir, "import_message_results.log")

if not os.path.exists(logging_file):
    with open(logging_file, "w", encoding="utf-8") as f:
        f.write("")

load_dotenv()

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    # conn.cursor().execute("SET search_path TO timedealer;")
    return conn

@celery.task
def import_messages_task(data_dicts, job_id=None):
    conn = get_conn()
    try:
        all_items = process_and_insert_messages(data_dicts, conn, es_client=es, es_index=ES_INDEX)
        result = {"job_id": job_id, "processed": len(all_items)}
        logger.info(json.dumps(result, ensure_ascii=False))
        return result
    except Exception as e:
        conn.rollback()
        error = {"job_id": job_id, "error": str(e)}
        logger.error(json.dumps(error, ensure_ascii=False))
        return error
    finally:
        conn.close()
