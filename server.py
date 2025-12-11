from fastapi import FastAPI, Query, HTTPException
import ticker_routes
from typing import Union
import json
import os
import csv
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ticker_routes.router, prefix="/ticker", tags=["Ticker"])

@app.get("/")
def read_root():
    return {"message": "Hello World"}


@app.get("/sp100")
def read_item():
    with open("data/sp100_live_data.json", "r") as f:
        data = json.load(f)
    return data


@app.get("/news")
def get_company_news(
    ticker: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),  # "no of records"
):
    # Build file path like data/AAPL.csv, data/MSFT.csv, etc.
    file_path = os.path.join("data/reuters_news", f"{ticker.upper()}.csv")

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"No news file found for ticker '{ticker.upper()}'",
        )

    # Read CSV into list of dicts
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)

    # Pagination math
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    # If page is beyond available data, return empty list but keep metadata
    if start_idx >= total:
        return {
            "ticker": ticker.upper(),
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": [],
        }

    page_rows = rows[start_idx:end_idx]

    return {
        "ticker": ticker.upper(),
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": page_rows,
    }