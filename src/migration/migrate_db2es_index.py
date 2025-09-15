from elasticsearch import Elasticsearch

ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "datatimedealer"

es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASSWORD),
)

# Tạo index
ES_INDEX = "message_items"
mapping = {
    "properties": {
        "item_id": {"type": "keyword"},
        "message_id": {"type": "keyword"},
        "message": {"type": "text"},
        "sender_name": {"type": "keyword"},
        "sender_phone": {"type": "keyword"},
        "transaction_type": {"type": "keyword"},
        "ref": {"type": "text"},
        "brand": {"type": "keyword"},
        "color": {"type": "keyword"},
        "price": {"type": "float"},
        "usd_price": {"type": "float"},
        "country": {"type": "keyword"},
        "currency": {"type": "keyword"},
        "release_date": {"type": "date", "format": "yyyy-MM-dd||yyyy-MM||yyyy"},
        "condition": {"type": "keyword"},
        "note": {"type": "text"},
        "posted_time": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}
    }
}


from elasticsearch import exceptions

try:
    es.indices.create(index=ES_INDEX, mappings=mapping)
    print(f"✅ Created index '{ES_INDEX}'")
except exceptions.RequestError as e:
    if "resource_already_exists_exception" in str(e):
        print(f"⚠️ Index '{ES_INDEX}' already exists")
    else:
        raise
