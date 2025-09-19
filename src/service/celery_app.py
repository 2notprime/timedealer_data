from celery import Celery

celery = Celery(
    "tasks",
    broker="redis://redis:6379/0",   # Redis trong docker-compose
    backend="redis://redis:6379/0"
)
