import os
import sys
from fastapi import FastAPI, APIRouter, Body, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Literal, List
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, exceptions
from datetime import datetime


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


from utils.map_code2dial import code_to_dial
from utils.exchange_currency import get_exchange_rate_usd
from service.item_filter import filter_and_add_count

load_dotenv()

# --- Elasticsearch setup ---
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "datatimedealer")
ES_INDEX = "message_items"

es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASSWORD)
)

# --- Pydantic model for query ---
class SearchRequest(BaseModel):
    ref: Optional[str] = Field(None, example="155")
    transaction_type: Literal[0, 1] = Field(0, example=0, description="0: forsale, 1: wtb")
    condition: Optional[int] = Field(0, example=0, description="0: new, 1: used, 2: both")
    brand: Optional[str] = Field(None, example="Audemars Piguet")
    year: Optional[list[int]] = Field(None, example=[2000, 2025])
    country: Optional[list[str]] = Field(None, example=["VN", "US", "DE", "HK"])
    time_range: Optional[int] = Field(None, example=8640000)
    price_min: Optional[float] = Field(None, example=0)
    price_max: Optional[float] = Field(None, example=1000000000)
    currency: Optional[str] = Field(None, example="HKD")
    sort_price: Optional[Literal[0, 1]] = Field(None, example=0, description="0: asc, 1: desc")
    limit: int = Field(50, example=50)
    offset: int = Field(0, example=0)
    using_usd: Literal[0, 1] = Field(0, example=0, description="0: không sử dụng USD, 1: sử dụng USD")

router = APIRouter()

def format_release_date(item):
    rd = item.get("release_date")
    precision = item.get("precision")
    if rd and precision:
        if precision == "year":
            return rd[:4]
        elif precision == "month":
            return rd[:7]
        else:
            return rd[:10]
    return rd

# --- Helper function to build ES query ---
def build_es_query(body: SearchRequest):
    must_filters = []
    range_filters = []

    # transaction_type
    if body.transaction_type is not None:
        must_filters.append({
            "term": {"transaction_type": "forsale" if body.transaction_type == 0 else "wtb"}
        })

    # condition
    if body.condition is not None:
        if body.condition == 0:  # new
            must_filters.append({"term": {"condition": "new"}})
        elif body.condition == 1:  # used
            must_filters.append({"term": {"condition": "used"}})
        elif body.condition == 2:  # both
            must_filters.append({"terms": {"condition": ["new", "used"]}})

    # brand
    if body.brand:
        must_filters.append({"term": {"brand": body.brand}})

    # year
    if body.year and len(body.year) == 2:
        start_year, end_year = body.year
        range_filters.append({
            "range": {
                "release_date": {"gte": f"{start_year}-01-01", "lte": f"{end_year}-12-31"}
            }
        })

    # price
    if body.currency and (body.price_min or body.price_max):
        try:
            if body.price_min:
                price_min_usd = float(body.price_min) / get_exchange_rate_usd(body.currency)
            else:
                price_min_usd = None
            if body.price_max:
                price_max_usd = float(body.price_max) / get_exchange_rate_usd(body.currency)
            else:
                price_max_usd = None
            print(f"Converted price range to USD: {price_min_usd} - {price_max_usd}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid currency: {body.currency}")

        price_field = "usd_price"
        range_filter = {}
        if price_min_usd is not None:
            range_filter["gte"] = price_min_usd
        if price_max_usd is not None:
            range_filter["lte"] = price_max_usd
        range_filters.append({"range": {price_field: range_filter}})

    # ref
    if body.ref:
        must_filters.append({"wildcard": {"ref": f"{body.ref}*"}})

    # time_range
    if body.time_range:
        must_filters.append({
            "range": {"posted_time": {"gte": f"now-{body.time_range}s"}}
        })

    # country -> dial code
    if body.country:
        dial_codes = []
        for c in body.country:
            dial_codes.extend(code_to_dial.get(c, []))
        if dial_codes:
            must_filters.append({
                "bool": {
                    "should": [{"prefix": {"sender_phone": d}} for d in dial_codes]
                }
            })

    query_body = {
        "query": {
            "bool": {
                "must": must_filters + range_filters
            }
        },
        "from": body.offset,
        "size": body.limit
    }

    # sort
    if body.sort_price is not None:
        query_body["sort"] = [
        {"usd_price" : {"order": "asc" if body.sort_price == 0 else "desc"}},
        {"posted_time": {"order": "desc"}}   # luôn kèm sort theo time
    ]
    else:
        query_body["sort"] = [
            {"posted_time": {"order": "desc"}}
        ]

    return query_body

# --- API endpoint ---
@router.post("/search")
def search_items(body: SearchRequest = Body(...)):
    try:
        query = build_es_query(body)
        res = es.search(index=ES_INDEX, body=query)
        hits = res.get("hits", {}).get("hits", [])
        total = res.get("hits", {}).get("total", {}).get("value", 0)
        items = []
        for hit in hits:
            doc = hit["_source"]
            doc["release_date"] = format_release_date(doc)
            doc.pop("precision", None)
            items.append(doc)
        return {"total": total, "items": items}
    except exceptions.ElasticsearchException as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/search_test")
def search_items(body: SearchRequest = Body(...)):
    try:
        query = build_es_query(body)
        res = es.search(index=ES_INDEX, body=query)
        hits = res.get("hits", {}).get("hits", [])
        # total = res.get("hits", {}).get("total", {}).get("value", 0)
        items = []
        for hit in hits:
            doc = hit["_source"]
            doc["release_date"] = format_release_date(doc)
            doc.pop("precision", None)
            items.append(doc)
        items = filter_and_add_count(items)
        total = len(items)
        return {"total": total, "items": items}
    except exceptions.ElasticsearchException as e:
        raise HTTPException(status_code=500, detail=str(e))
