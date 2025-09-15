import json
import os
import time
import psycopg2
import logging
from dotenv import load_dotenv
import sys
import redis
from rq import Queue, Worker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.import_messages import process_and_insert_messages

load_dotenv()

redis_conn = redis.Redis(host="localhost", port=6379, db=0)
q = Queue("default", connection=redis_conn)

src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
logging_file = os.path.join(src_dir, "logs", "import_message_results.log")
logging.basicConfig(
    filename=logging_file,
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE"),
}

def import_messages_job(data_dicts, job_id=None):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        all_items = process_and_insert_messages(data_dicts, conn)
        result = {"job_id": job_id, "processed": len(all_items)}
        logger.info(json.dumps(result, ensure_ascii=False))
        return result
    except Exception as e:
        conn.rollback()
        error = {"job_id": job_id, "error": str(e)}
        logger.info(json.dumps(error, ensure_ascii=False))
        raise
    finally:
        conn.close()
        
# if __name__ == "__main__":
#     # Windows không fork, Python dùng spawn
#     worker = Worker([q], connection=redis_conn)
#     worker.work(burst=False) 

