from fastapi import APIRouter
import pandas as pd
from typing import List, Dict
import yfinance as yf

def download_1m(symbols: List[str], days: int) -> Dict[str, pd.Series]:
    out = {s: pd.Series(dtype=float) for s in symbols}
    try:
        df = yf.download(
            symbols,
            period=f"{days}d",
            interval="1m",
            group_by="ticker",
            auto_adjust=True,
            threads=True,
            progress=False,
            prepost=True,
        )
    except Exception:
        df = pd.DataFrame()

    for sym in symbols:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                # Multi-ticker shape
                if (sym, "Close") in df.columns:
                    ser = df[(sym, "Close")].dropna()
                else:
                    ser = df[sym]["Close"].dropna()
            else:
                # Single-ticker shape
                ser = df["Close"].dropna() if "Close" in df.columns and len(symbols) == 1 else pd.Series(dtype=float)
        except Exception:
            ser = pd.Series(dtype=float)
        out[sym] = ser
    return out

router = APIRouter()

@router.get("/{ticker}")
def get_ticker_last_day_data(ticker: str):
    data = download_1m(symbols=[ticker], days=2)
    last_date = data[ticker].index.date[-1]
    last_day = data[ticker][data[ticker].index.date == last_date]
    points = [
        {
            "timestamp": ts.isoformat(),
            "close": float(val),
        }
        for ts, val in last_day.items()
    ]
    return {
        "ticker": ticker,
        "date": str(last_date),
        "points": points,
    }