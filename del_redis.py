import redis
from datetime import datetime, timedelta, timezone
import time


utc_plus7 = timezone(timedelta(hours=7))


r = redis.Redis(host="localhost", port=6379, db=0)

while True:
    deleted = r.delete("celery")
    ts = datetime.now(utc_plus7).strftime("%Y-%m-%d %H:%M:%S")
    print(ts)
    print("wait 45s ...")
    time.sleep(45)
