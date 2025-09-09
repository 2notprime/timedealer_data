import sys
import os
from fastapi.middleware.cors import CORSMiddleware


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI
from src.api import import_api, api
from src.ws import tracking

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # cho phép tất cả các domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers từ 2 file khác nhau
app.include_router(import_api.router, prefix="/import")
app.include_router(api.router)
# app.include_router(tracking.router, prefix="/ws")
