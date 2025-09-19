from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
backend_url = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

celery = Celery(
    "tasks",
    broker=broker_url,
    backend=backend_url,
    include=["service.import_tasks"]
)

