# uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
import sys
import os
from fastapi.middleware.cors import CORSMiddleware


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI
from src.api import import_api, api

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # cho phép tất cả các domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers từ 2 file khác nhau
app.include_router(import_api.router)

