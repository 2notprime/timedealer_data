# delete_index.py
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "datatimedealer")
ES_INDEX = os.getenv("ES_INDEX", "message_items")

# Connect Elasticsearch
es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASSWORD))
if es.ping():
    print("Connected to Elasticsearch")
else:
    raise Exception("Cannot connect to Elasticsearch")

# Xóa index nếu tồn tại
if es.indices.exists(index=ES_INDEX):
    es.indices.delete(index=ES_INDEX)
    print(f"Deleted index {ES_INDEX}")
else:
    print(f"Index {ES_INDEX} does not exist")
