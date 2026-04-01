"""
news_fetcher.py
===============
Daily incremental news + macro data pipeline for the Finance RAG system.

Sources & what they actually give you historically
---------------------------------------------------
  1. Finnhub (free key)
       - Company news per ticker, tagged to symbol
       - History: ~1 year per call on free tier
       - Rate limit: 60 req/min
       - Get key: https://finnhub.io

  2. GDELT Doc API (no key needed)
       - Global news articles searchable by company name / keyword
       - History: 2017-01-01 to present via date-windowed queries
       - Rate limit: none enforced, but 250 articles max per query
       - This is the ONLY free source with multi-year historical news text
       - Caveat: URLs only — full article text requires fetching each URL

  3. RSS feeds (no key needed)
       - Reuters, MarketWatch, AP, Investing.com, Nasdaq
       - History: rolling 30-90 days ONLY — feeds do not carry older items
       - Use: for keeping current after the historical backfill is done
       - Rate limit: unlimited

  4. FRED API (free key)
       - Macro economic time series: Fed rate, CPI, GDP, VIX, yields...
       - History: 1960s onward for most series
       - Rate limit: 120 req/min
       - Get key: https://fred.stlouisfed.org/docs/api/api_key.html

What to run and when
--------------------
  First-ever run (builds full history):
    python news_fetcher.py --full --sources gdelt fred finnhub
    # GDELT takes a while - it chunks 2017 to today in 30-day windows per ticker
    # RSS is automatically skipped in --full (only has recent articles anyway)

  After that, daily cron (picks up only new content):
    python news_fetcher.py

  Cron schedule (weekdays at 7am):
    0 7 * * 1-5  cd /project && python news_fetcher.py >> logs/news.log 2>&1

Output layout
-------------
  data/news/
    state.json                           <- incremental checkpoint (never delete)
    articles/
      {YYYY-MM-DD}_{source}_{id}.json    <- one JSON file per article / data-point
    metadata/
      company/
        {TICKER}_news.json               <- index of all articles mentioning ticker
      macro/
        {SERIES_ID}_macro.json           <- index of all FRED observations
      global_news.json                   <- index of all RSS + GDELT global articles

Unified article schema (matches edgar_metadata.py filing schema for KG ingestion)
----------------------------------------------------------------------------------
  {
    "doc_id":         "gdelt_AAPL_2024-11-01_a3f9b1c2",
    "source":         "gdelt" | "finnhub" | "rss_reuters" | "fred",
    "source_type":    "company_news" | "global_news" | "macro",
    "ticker":         "AAPL",           # null if no single ticker
    "tickers":        ["AAPL"],         # all tickers mentioned
    "company_name":   "Apple Inc.",
    "published_at":   "2024-11-01T18:32:00Z",
    "fetched_at":     "2024-11-02T06:00:00Z",
    "title":          "Apple reports record Q4 earnings",
    "summary":        "...",
    "url":            "https://...",
    "content_path":   "data/news/articles/...",
    "category":       "earnings",
    "sentiment":      null,
    "related_filing": "",               # accession_number if linked to 8-K
    "series_id":      null,             # FRED series ID
    "value":          null,             # numeric value (FRED only)
    "unit":           null,
    "tags":           ["earnings", "technology"]
  }

Requirements
  pip install requests feedparser tqdm
"""

import argparse
import datetime
import hashlib
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Optional

import feedparser
import requests
from tqdm import tqdm
from dotenv import load_dotenv

# ══════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════
load_dotenv()
FINNHUB_API_KEY: str = os.environ.get("FINNHUB_API_KEY", "")
FRED_API_KEY:    str = os.environ.get("FRED_API_KEY", "")

NEWS_DIR:   Path = Path(os.environ.get("NEWS_DIR",         "data/news"))
EDGAR_DIR:  Path = Path(os.environ.get("EDGAR_OUTPUT_DIR", "data/edgar_documents"))
SP100_JSON: Path = Path("data/sp100_tickers.json")

# Finnhub
FINNHUB_INITIAL_LOOKBACK_DAYS: int = 365
FINNHUB_CHUNK_DAYS:            int = 30

# GDELT
GDELT_START_DATE:    str   = "2017-01-01"   # earliest reliable GDELT Doc API date
GDELT_CHUNK_DAYS:    int   = 30             # query window per chunk
GDELT_MAX_PER_QUERY: int   = 250            # hard API limit
GDELT_SLEEP:         float = 1.5            # be polite - no official rate limit

# RSS
RSS_MAX_ARTICLES_PER_FEED: int = 200

# FRED
FRED_INITIAL_LOOKBACK_YEARS: int = 30

# HTTP
MAX_RETRIES:  int   = 4
BACKOFF_BASE: float = 2.0
REQ_SLEEP:    float = 0.12

# ── RSS feeds ──────────────────────────────────────────────────────────────
RSS_FEEDS: list[dict] = [
    {"url": "https://feeds.reuters.com/reuters/businessNews",
     "source": "rss_reuters", "category": "business"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories",
     "source": "rss_marketwatch", "category": "markets"},
    {"url": "https://feeds.marketwatch.com/marketwatch/marketpulse",
     "source": "rss_marketwatch", "category": "market_pulse"},
    {"url": "https://www.investing.com/rss/news.rss",
     "source": "rss_investing", "category": "investing"},
    {"url": "https://feeds.nasdaq.com/rss/2.0/headlines.aspx",
     "source": "rss_nasdaq", "category": "markets"},
    {"url": "https://apnews.com/hub/business.rss",
     "source": "rss_ap", "category": "business"},
    {"url": "https://feeds.reuters.com/reuters/economicsnews",
     "source": "rss_reuters", "category": "economics"},
    {"url": "https://feeds.marketwatch.com/marketwatch/economy-politics",
     "source": "rss_marketwatch", "category": "economy"},
]

# ── FRED series ────────────────────────────────────────────────────────────
FRED_SERIES: list[dict] = [
    {"id": "FEDFUNDS",     "name": "Fed funds rate",           "unit": "%"},
    {"id": "DGS10",        "name": "10-year Treasury yield",   "unit": "%"},
    {"id": "DGS2",         "name": "2-year Treasury yield",    "unit": "%"},
    {"id": "T10Y2Y",       "name": "Yield curve (10y-2y)",     "unit": "%"},
    {"id": "CPIAUCSL",     "name": "CPI all urban",            "unit": "index"},
    {"id": "CPILFESL",     "name": "Core CPI ex food/energy",  "unit": "index"},
    {"id": "PCEPI",        "name": "PCE price index",          "unit": "index"},
    {"id": "GDP",          "name": "Gross domestic product",   "unit": "USD_bn"},
    {"id": "GDPC1",        "name": "Real GDP",                 "unit": "USD_bn_2017"},
    {"id": "INDPRO",       "name": "Industrial production",    "unit": "index"},
    {"id": "UNRATE",       "name": "Unemployment rate",        "unit": "%"},
    {"id": "PAYEMS",       "name": "Nonfarm payrolls",         "unit": "thousands"},
    {"id": "BAMLH0A0HYM2", "name": "High-yield spread OAS",   "unit": "bps"},
    {"id": "VIXCLS",       "name": "CBOE VIX",                 "unit": "index"},
    {"id": "DCOILWTICO",   "name": "WTI crude oil",            "unit": "USD/bbl"},
    {"id": "DEXUSEU",      "name": "USD/EUR exchange rate",    "unit": "USD_per_EUR"},
]

# ══════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
#  HTTP
# ══════════════════════════════════════════════
_SESSION: Optional[requests.Session] = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({"Accept-Encoding": "gzip, deflate"})
    return _SESSION


def _get(url: str, params: dict = None, timeout: int = 30) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session().get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 2)
                log.warning("  Rate-limited (attempt %d) - %.1fs", attempt, wait)
                time.sleep(wait)
                continue
            if resp.status_code in (500, 502, 503, 504):
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
                log.warning("  HTTP %s (attempt %d) - %.1fs", resp.status_code, attempt, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            log.warning("  Connection error (attempt %d): %s - %.1fs", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {url}")


# ══════════════════════════════════════════════
#  Directory layout
# ══════════════════════════════════════════════
ARTICLES_DIR = NEWS_DIR / "articles"
META_COMPANY = NEWS_DIR / "metadata" / "company"
META_MACRO   = NEWS_DIR / "metadata" / "macro"
STATE_FILE   = NEWS_DIR / "state.json"
GLOBAL_INDEX = NEWS_DIR / "metadata" / "global_news.json"


def _init_dirs() -> None:
    for d in [ARTICLES_DIR, META_COMPANY, META_MACRO]:
        d.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════
#  State  (the incremental-run brain)
# ══════════════════════════════════════════════
class State:
    """
    Persists fetch progress so each daily run only retrieves new content.

    Structure:
      finnhub -> {TICKER: {last_fetched_date, seen_ids[]}}
      gdelt   -> {TICKER: {last_fetched_date}}   # dedup by file existence, not ID set
      rss     -> {feed_url: {seen_ids[]}}
      fred    -> {SERIES_ID: {last_observation_date}}

    Saved atomically after every ticker/feed so a crash mid-run never loses
    progress - the next run resumes from exactly where it stopped.
    """
    MAX_SEEN = 5_000

    def __init__(self, path: Path, force_full: bool = False):
        self._path = path
        self._data: dict = {"finnhub": {}, "gdelt": {}, "rss": {}, "fred": {}}
        if not force_full and path.exists():
            try:
                with open(path, encoding="utf-8") as fh:
                    loaded = json.load(fh)
                for k in self._data:
                    self._data[k] = loaded.get(k, {})
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("Could not load state (%s) - starting fresh", exc)

    def finnhub_last_date(self, ticker: str) -> Optional[datetime.date]:
        raw = self._data["finnhub"].get(ticker, {}).get("last_fetched_date")
        return datetime.date.fromisoformat(raw) if raw else None

    def finnhub_seen(self, ticker: str) -> set:
        return set(self._data["finnhub"].get(ticker, {}).get("seen_ids", []))

    def finnhub_update(self, ticker: str, date: datetime.date, new_ids: set) -> None:
        entry  = self._data["finnhub"].setdefault(ticker, {})
        merged = set(entry.get("seen_ids", [])) | new_ids
        if len(merged) > self.MAX_SEEN:
            merged = set(list(merged)[-self.MAX_SEEN:])
        entry["last_fetched_date"] = date.isoformat()
        entry["seen_ids"]          = sorted(merged)

    def gdelt_last_date(self, ticker: str) -> Optional[datetime.date]:
        raw = self._data["gdelt"].get(ticker, {}).get("last_fetched_date")
        return datetime.date.fromisoformat(raw) if raw else None

    def gdelt_update(self, ticker: str, date: datetime.date) -> None:
        self._data["gdelt"].setdefault(ticker, {})["last_fetched_date"] = date.isoformat()

    def rss_seen(self, feed_url: str) -> set:
        return set(self._data["rss"].get(feed_url, {}).get("seen_ids", []))

    def rss_update(self, feed_url: str, new_ids: set) -> None:
        entry  = self._data["rss"].setdefault(feed_url, {})
        merged = set(entry.get("seen_ids", [])) | new_ids
        if len(merged) > self.MAX_SEEN:
            merged = set(list(merged)[-self.MAX_SEEN:])
        entry["seen_ids"] = sorted(merged)

    def fred_last_date(self, series_id: str) -> Optional[datetime.date]:
        raw = self._data["fred"].get(series_id, {}).get("last_observation_date")
        return datetime.date.fromisoformat(raw) if raw else None

    def fred_update(self, series_id: str, date: datetime.date) -> None:
        self._data["fred"].setdefault(series_id, {})["last_observation_date"] = date.isoformat()

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2, ensure_ascii=False)
        tmp.replace(self._path)


# ══════════════════════════════════════════════
#  Article persistence + indexes
# ══════════════════════════════════════════════
def _article_filename(doc_id: str, published_at: str) -> str:
    date_prefix = published_at[:10] if published_at else "0000-00-00"
    safe = doc_id.replace("/", "_").replace(":", "_")[:80]
    return f"{date_prefix}_{safe}.json"


def save_article(article: dict) -> Path:
    fname = _article_filename(article["doc_id"], article["published_at"])
    dest  = ARTICLES_DIR / fname
    if not dest.exists():
        with open(dest, "w", encoding="utf-8") as fh:
            json.dump(article, fh, indent=2, ensure_ascii=False)
    return dest


def _load_index(path: Path) -> list[dict]:
    if path.exists():
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_index(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2, ensure_ascii=False)
    tmp.replace(path)


def _append_to_index(path: Path, new_records: list[dict]) -> None:
    existing     = _load_index(path)
    existing_ids = {r["doc_id"] for r in existing}
    merged       = existing + [r for r in new_records if r["doc_id"] not in existing_ids]
    merged.sort(key=lambda r: r["published_at"], reverse=True)
    _save_index(path, merged)


def update_company_index(ticker: str, records: list[dict]) -> None:
    _append_to_index(META_COMPANY / f"{ticker}_news.json", records)


def update_global_index(records: list[dict]) -> None:
    _append_to_index(GLOBAL_INDEX, records)


def update_macro_index(series_id: str, records: list[dict]) -> None:
    _append_to_index(META_MACRO / f"{series_id}_macro.json", records)


# ══════════════════════════════════════════════
#  Ticker loader
# ══════════════════════════════════════════════
_FALLBACK_SP100 = [
    "AAPL","ABBV","ABT","ACN","ADBE","AIG","AMD","AMGN","AMT","AMZN",
    "AVGO","AXP","BA","BAC","BK","BKNG","BLK","BMY","BRK-B","C",
    "CAT","CHTR","CL","CMCSA","COF","COP","COST","CRM","CSCO","CVS",
    "CVX","DE","DHR","DIS","DUK","EMR","FDX","GD","GE","GILD","GM",
    "GOOGL","GS","HD","HON","IBM","INTC","INTU","ISRG","JNJ","JPM",
    "KO","LIN","LLY","LMT","LOW","MA","MCD","MDLZ","MDT","MET","META",
    "MMM","MO","MRK","MS","MSFT","NEE","NFLX","NKE","NOW","NVDA",
    "ORCL","PEP","PFE","PG","PLTR","PM","PYPL","QCOM","RTX","SBUX",
    "SCHW","SO","SPG","T","TGT","TMO","TMUS","TSLA","TXN","UNH",
    "UNP","UPS","USB","V","VZ","WFC","WMT","XOM",
]


def load_sp100() -> list[dict]:
    if SP100_JSON.exists():
        with open(SP100_JSON, encoding="utf-8") as fh:
            return json.load(fh)
    log.warning("sp100_tickers.json not found - using built-in fallback list")
    return [{"ticker": t, "company_name_sec": "", "company_name_wiki": ""}
            for t in _FALLBACK_SP100]


def _name(entry: dict) -> str:
    return entry.get("company_name_sec") or entry.get("company_name_wiki") or ""


# ══════════════════════════════════════════════
#  Source 1: Finnhub
# ══════════════════════════════════════════════
FINNHUB_BASE = "https://finnhub.io/api/v1"


def _date_chunks(
    start: datetime.date, end: datetime.date, chunk_days: int
) -> list[tuple[datetime.date, datetime.date]]:
    chunks, cur = [], start
    while cur <= end:
        chunks.append((cur, min(cur + datetime.timedelta(days=chunk_days - 1), end)))
        cur += datetime.timedelta(days=chunk_days)
    return chunks


def fetch_finnhub_ticker(
    ticker: str, company_name: str, state: State, force_full: bool
) -> list[dict]:
    if not FINNHUB_API_KEY:
        return []

    today = datetime.date.today()
    seen  = state.finnhub_seen(ticker)
    last  = state.finnhub_last_date(ticker)
    start = (today - datetime.timedelta(days=FINNHUB_INITIAL_LOOKBACK_DAYS)
             if (force_full or last is None)
             else last - datetime.timedelta(days=2))

    records: list[dict] = []
    new_ids: set        = set()
    max_date            = start

    for chunk_start, chunk_end in _date_chunks(start, today, FINNHUB_CHUNK_DAYS):
        try:
            resp = _get(f"{FINNHUB_BASE}/company-news", params={
                "symbol": ticker,
                "_from":  chunk_start.isoformat(),
                "to":     chunk_end.isoformat(),
                "token":  FINNHUB_API_KEY,
            })
            raw = resp.json()
        except Exception as exc:
            log.warning("  Finnhub %s (%s-%s): %s", ticker, chunk_start, chunk_end, exc)
            time.sleep(REQ_SLEEP * 5)
            continue

        if not isinstance(raw, list):
            break

        for item in raw:
            art_id = str(item.get("id", ""))
            if not art_id or art_id in seen:
                continue

            pub_dt  = datetime.datetime.utcfromtimestamp(item.get("datetime", 0))
            pub_str = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            doc_id  = f"finnhub_{ticker}_{art_id}"

            article = {
                "doc_id":         doc_id,
                "source":         "finnhub",
                "source_type":    "company_news",
                "ticker":         ticker,
                "tickers":        [ticker],
                "company_name":   company_name,
                "published_at":   pub_str,
                "fetched_at":     datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title":          item.get("headline", ""),
                "summary":        item.get("summary", ""),
                "url":            item.get("url", ""),
                "content_path":   "",
                "category":       item.get("category", ""),
                "sentiment":      item.get("sentiment", None),
                "related_filing": "",
                "series_id":      None,
                "value":          None,
                "unit":           None,
                "tags":           [item.get("category")] if item.get("category") else [],
                "image":          item.get("image", ""),
                "source_raw":     "finnhub",
            }
            dest = save_article(article)
            article["content_path"] = str(dest)
            records.append(article)
            new_ids.add(art_id)
            if pub_dt.date() > max_date:
                max_date = pub_dt.date()

        time.sleep(REQ_SLEEP)

    state.finnhub_update(ticker, max(max_date, today), seen | new_ids)
    return records


# ══════════════════════════════════════════════
#  Source 2: GDELT Doc API  (historical backfill)
# ══════════════════════════════════════════════
GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"


def _gdelt_query(keyword: str, start: datetime.date, end: datetime.date) -> list[dict]:
    """
    Query GDELT for English-language articles mentioning keyword in [start, end].
    Returns up to 250 articles. No API key required.

    GDELT date format: YYYYMMDDHHMMSS
    """
    params = {
        "query":         f'"{keyword}"',
        "mode":          "artlist",
        "maxrecords":    GDELT_MAX_PER_QUERY,
        "format":        "json",
        "startdatetime": start.strftime("%Y%m%d") + "000000",
        "enddatetime":   end.strftime("%Y%m%d")   + "235959",
        "sourcelang":    "english",
    }
    try:
        resp = _get(GDELT_API, params=params, timeout=45)
        return resp.json().get("articles", []) or []
    except Exception as exc:
        log.debug("  GDELT query failed (%s %s-%s): %s", keyword, start, end, exc)
        return []


def fetch_gdelt_ticker(
    ticker: str, company_name: str, state: State, force_full: bool
) -> list[dict]:
    """
    Fetch news for one ticker from GDELT, searching by company name and ticker symbol.

    Deduplication strategy: file-based (check if article file already exists on disk)
    rather than ID-based, because GDELT article IDs are unstable across queries.
    The URL hash is the stable unique key.
    """
    today       = datetime.date.today()
    last        = state.gdelt_last_date(ticker)
    gdelt_start = datetime.date.fromisoformat(GDELT_START_DATE)

    start = (gdelt_start if (force_full or last is None)
             else last - datetime.timedelta(days=3))

    # Build keywords: prefer cleaned company name, always include ticker symbol
    keywords = [ticker]
    if company_name:
        clean = (company_name
                 .replace(" Inc.", "").replace(" Inc", "")
                 .replace(" Corp.", "").replace(" Corp", "")
                 .replace(" Ltd.", "").replace(" Ltd", "")
                 .replace(" LLC", "").replace(" Co.", "").strip())
        if clean and clean != ticker:
            keywords.insert(0, clean)

    records:  list[dict] = []
    max_date: datetime.date = start

    for chunk_start, chunk_end in _date_chunks(start, today, GDELT_CHUNK_DAYS):
        for keyword in keywords:
            for item in _gdelt_query(keyword, chunk_start, chunk_end):
                url = item.get("url", "")
                if not url:
                    continue

                url_hash = hashlib.sha1(url.encode()).hexdigest()[:12]
                doc_id   = f"gdelt_{ticker}_{url_hash}"

                # Parse GDELT seendate: "20241101T183200Z" or "20241101000000"
                raw_date = item.get("seendate", "")
                try:
                    if "T" in raw_date:
                        dt = datetime.datetime.strptime(raw_date, "%Y%m%dT%H%M%SZ")
                    else:
                        dt = datetime.datetime.strptime(raw_date[:8], "%Y%m%d")
                    pub_iso  = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    pub_date = dt.date()
                except ValueError:
                    pub_iso  = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    pub_date = chunk_start

                # Skip if file already exists from a prior run
                if (ARTICLES_DIR / _article_filename(doc_id, pub_iso)).exists():
                    continue

                article = {
                    "doc_id":         doc_id,
                    "source":         "gdelt",
                    "source_type":    "company_news",
                    "ticker":         ticker,
                    "tickers":        [ticker],
                    "company_name":   company_name,
                    "published_at":   pub_iso,
                    "fetched_at":     datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title":          item.get("title", ""),
                    "summary":        "",   # GDELT provides URL + title only, no body text
                    "url":            url,
                    "content_path":   "",
                    "category":       item.get("domain", ""),
                    "sentiment":      None,
                    "related_filing": "",
                    "series_id":      None,
                    "value":          None,
                    "unit":           None,
                    "tags":           ["gdelt", ticker.lower()],
                    "image":          item.get("socialimage", ""),
                    "source_raw":     "gdelt",
                }
                dest = save_article(article)
                article["content_path"] = str(dest)
                records.append(article)

                if pub_date > max_date:
                    max_date = pub_date

        time.sleep(GDELT_SLEEP)

    state.gdelt_update(ticker, max(max_date, today))
    return records


# ══════════════════════════════════════════════
#  Source 3: RSS (daily top-up only)
# ══════════════════════════════════════════════
def _rss_item_id(entry) -> str:
    raw = getattr(entry, "id", None) or getattr(entry, "link", None) or str(entry.get("title", ""))
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _rss_pub(entry) -> str:
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return datetime.datetime(*val[:6]).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pass
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _find_tickers(text: str, known: set) -> list[str]:
    found = []
    for word in text.upper().split():
        clean = word.strip("$.,;:!?()")
        if clean in known:
            found.append(clean)
    return list(dict.fromkeys(found))


def fetch_rss_feed(feed_cfg: dict, state: State, known_tickers: set) -> list[dict]:
    url      = feed_cfg["url"]
    source   = feed_cfg["source"]
    category = feed_cfg["category"]
    seen     = state.rss_seen(url)

    try:
        parsed = feedparser.parse(url)
    except Exception as exc:
        log.warning("  RSS error (%s): %s", url, exc)
        return []

    records: list[dict] = []
    new_ids: set        = set()

    for entry in parsed.entries[:RSS_MAX_ARTICLES_PER_FEED]:
        item_id   = _rss_item_id(entry)
        if item_id in seen:
            continue

        pub_str   = _rss_pub(entry)
        title     = getattr(entry, "title", "")
        summary   = getattr(entry, "summary", "") or getattr(entry, "description", "")
        link      = getattr(entry, "link", "")
        doc_id    = f"{source}_{item_id}"
        mentioned = _find_tickers(f"{title} {summary}", known_tickers)

        article = {
            "doc_id":         doc_id,
            "source":         source,
            "source_type":    "global_news",
            "ticker":         mentioned[0] if len(mentioned) == 1 else None,
            "tickers":        mentioned,
            "company_name":   "",
            "published_at":   pub_str,
            "fetched_at":     datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "title":          title,
            "summary":        summary,
            "url":            link,
            "content_path":   "",
            "category":       category,
            "sentiment":      None,
            "related_filing": "",
            "series_id":      None,
            "value":          None,
            "unit":           None,
            "tags":           [category],
            "image":          "",
            "source_raw":     url,
        }
        dest = save_article(article)
        article["content_path"] = str(dest)
        records.append(article)
        new_ids.add(item_id)

    state.rss_update(url, seen | new_ids)
    return records


# ══════════════════════════════════════════════
#  Source 4: FRED macro data
# ══════════════════════════════════════════════
FRED_BASE = "https://api.stlouisfed.org/fred"


def fetch_fred_series(series: dict, state: State, force_full: bool) -> list[dict]:
    if not FRED_API_KEY:
        return []

    sid   = series["id"]
    name  = series["name"]
    unit  = series["unit"]
    last  = state.fred_last_date(sid)
    today = datetime.date.today()

    start = (today - datetime.timedelta(days=FRED_INITIAL_LOOKBACK_YEARS * 365)
             if (force_full or last is None)
             else last - datetime.timedelta(days=7))

    try:
        resp = _get(f"{FRED_BASE}/series/observations", params={
            "series_id":         sid,
            "observation_start": start.isoformat(),
            "observation_end":   today.isoformat(),
            "api_key":           FRED_API_KEY,
            "file_type":         "json",
            "sort_order":        "asc",
        })
        observations = resp.json().get("observations", [])
    except Exception as exc:
        log.warning("  FRED %s: %s", sid, exc)
        return []

    records:  list[dict] = []
    max_date: datetime.date = start

    for obs in observations:
        date_str = obs.get("date", "")
        val_str  = obs.get("value", ".")
        if val_str == "." or not date_str:
            continue
        try:
            val  = float(val_str)
            date = datetime.date.fromisoformat(date_str)
        except ValueError:
            continue

        doc_id  = f"fred_{sid}_{date_str}"
        pub_str = f"{date_str}T00:00:00Z"

        article = {
            "doc_id":         doc_id,
            "source":         "fred",
            "source_type":    "macro",
            "ticker":         None,
            "tickers":        [],
            "company_name":   "",
            "published_at":   pub_str,
            "fetched_at":     datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "title":          f"{name}: {val:.4g} {unit} on {date_str}",
            "summary":        f"FRED {sid} ({name}): {val} {unit} as of {date_str}",
            "url":            f"https://fred.stlouisfed.org/series/{sid}",
            "content_path":   "",
            "category":       "macro",
            "sentiment":      None,
            "related_filing": "",
            "series_id":      sid,
            "value":          val,
            "unit":           unit,
            "tags":           ["macro", sid.lower()],
            "image":          "",
            "source_raw":     "fred",
        }
        dest = save_article(article)
        article["content_path"] = str(dest)
        records.append(article)
        if date > max_date:
            max_date = date

    if records:
        state.fred_update(sid, max_date)
    return records


# ══════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Incremental news + macro fetcher")
    p.add_argument(
        "--full", action="store_true",
        help=(
            "Ignore state and fetch full available history. "
            "Run ONCE on first setup only."
        )
    )
    p.add_argument(
        "--tickers", nargs="+", metavar="TICKER",
        help="Only process these tickers (default: all S&P 100)"
    )
    p.add_argument(
        "--sources", nargs="+",
        choices=["finnhub", "gdelt", "rss", "fred"],
        default=["finnhub", "gdelt", "rss", "fred"],
        help="Which sources to run (default: all four)"
    )
    return p.parse_args()


# ══════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════
def run() -> None:
    args = parse_args()

    if "finnhub" in args.sources and not FINNHUB_API_KEY:
        log.warning(
            "FINNHUB_API_KEY not set - skipping Finnhub.\n"
            "  Free key: https://finnhub.io  then: export FINNHUB_API_KEY=your_key"
        )
    if "fred" in args.sources and not FRED_API_KEY:
        log.warning(
            "FRED_API_KEY not set - skipping FRED.\n"
            "  Free key: https://fred.stlouisfed.org/docs/api/api_key.html"
            "  then: export FRED_API_KEY=your_key"
        )

    _init_dirs()
    state       = State(STATE_FILE, force_full=args.full)
    sp100       = load_sp100()
    all_tickers = {e["ticker"].upper() for e in sp100}

    if args.tickers:
        sp100 = [e for e in sp100
                 if e["ticker"].upper() in {t.upper() for t in args.tickers}]

    if args.full:
        n_chunks = len(_date_chunks(
            datetime.date.fromisoformat(GDELT_START_DATE),
            datetime.date.today(), GDELT_CHUNK_DAYS
        ))
        log.info(
            "FULL RUN - fetching complete history:\n"
            "  Finnhub: up to 1 year per ticker\n"
            "  GDELT:   2017-01-01 to today (%d x 30-day chunks per ticker)\n"
            "  FRED:    %d years of macro data\n"
            "  RSS:     skipped during --full (only has recent 30-90 days anyway)",
            n_chunks, FRED_INITIAL_LOOKBACK_YEARS
        )

    total_new = 0

    # ── Finnhub ───────────────────────────────────────────────────────────
    if "finnhub" in args.sources and FINNHUB_API_KEY:
        log.info("== Finnhub (%d tickers) ==", len(sp100))
        bar = tqdm(sp100, desc="Finnhub", unit="ticker")
        for entry in bar:
            ticker = entry["ticker"].upper()
            bar.set_postfix(ticker=ticker)
            recs = fetch_finnhub_ticker(ticker, _name(entry), state, args.full)
            if recs:
                update_company_index(ticker, recs)
                total_new += len(recs)
            state.save()
            time.sleep(REQ_SLEEP)

    # ── GDELT ─────────────────────────────────────────────────────────────
    if "gdelt" in args.sources:
        log.info("== GDELT (%d tickers, from %s) ==", len(sp100), GDELT_START_DATE)
        bar = tqdm(sp100, desc="GDELT", unit="ticker")
        for entry in bar:
            ticker = entry["ticker"].upper()
            bar.set_postfix(ticker=ticker)
            recs = fetch_gdelt_ticker(ticker, _name(entry), state, args.full)
            if recs:
                update_company_index(ticker, recs)
                update_global_index(recs)
                total_new += len(recs)
                log.info("  %s: +%d articles", ticker, len(recs))
            state.save()
            time.sleep(GDELT_SLEEP)

    # ── RSS (daily top-up only - no historical value) ─────────────────────
    if "rss" in args.sources:
        if args.full:
            log.info("== RSS - skipped during --full (only carries recent 30-90 days) ==")
        else:
            log.info("== RSS (%d feeds) ==", len(RSS_FEEDS))
            all_rss: list[dict] = []
            for feed_cfg in tqdm(RSS_FEEDS, desc="RSS", unit="feed"):
                recs = fetch_rss_feed(feed_cfg, state, all_tickers)
                all_rss.extend(recs)
                if recs:
                    by_ticker: dict[str, list] = {}
                    for r in recs:
                        for t in r.get("tickers", []):
                            if t in all_tickers:
                                by_ticker.setdefault(t, []).append(r)
                    for t, trecs in by_ticker.items():
                        update_company_index(t, trecs)
                state.save()
                time.sleep(REQ_SLEEP)
            if all_rss:
                update_global_index(all_rss)
                total_new += len(all_rss)

    # ── FRED ──────────────────────────────────────────────────────────────
    if "fred" in args.sources and FRED_API_KEY:
        log.info("== FRED (%d series) ==", len(FRED_SERIES))
        for series in tqdm(FRED_SERIES, desc="FRED", unit="series"):
            recs = fetch_fred_series(series, state, args.full)
            if recs:
                update_macro_index(series["id"], recs)
                total_new += len(recs)
                log.info("  %s: +%d observations", series["id"], len(recs))
            state.save()
            time.sleep(REQ_SLEEP)

    state.save()
    log.info("===========================================")
    log.info("Done. Total new items this run: %d", total_new)
    log.info("Output: %s", NEWS_DIR.resolve())


if __name__ == "__main__":
    run()