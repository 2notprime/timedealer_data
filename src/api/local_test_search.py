from fastapi import FastAPI, Query
import sqlite3
from typing import Optional, List, Any
from pydantic import BaseModel

app = FastAPI()

DB_PATH = "my_local_v1.db"

class Item(BaseModel):
    item_id: int
    unique_id: int
    transaction_type: str
    ref: str
    brand: str
    color: str
    price: Optional[float]
    currency: str
    year: str
    condition: str
    note: str

class SearchItemsResponse(BaseModel):
    total: int
    data: List[Item]

def query_db(query, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/search", response_model=SearchItemsResponse)
def search_items(
    ref: Optional[str] = Query(None),
    color: Optional[str] = Query(None),
    currency: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    min_price: Optional[float] = Query(None),
    max_price: Optional[float] = Query(None),
    sort_price: Optional[str] = Query(None, regex="^(asc|desc)$")
):
    query = "SELECT * FROM message_items WHERE 1=1"
    params = []

    if ref:
        ref = ref.upper()
        query += " AND ref LIKE ?"
        params.append(f"%{ref}%")

    if color:
        query += " AND color = ?"
        params.append(color)

    if currency:
        query += " AND currency = ?"
        params.append(currency)

    if year:
        query += " AND year = ?"
        params.append(year)

    if min_price is not None:
        query += " AND price >= ?"
        params.append(min_price)

    if max_price is not None:
        query += " AND price <= ?"
        params.append(max_price)

    if sort_price:
        query += f" ORDER BY price {sort_price.upper()}"
    query += f" LIMIT 100"
    data = query_db(query, params)

    return SearchItemsResponse(total=len(data), data=data)
