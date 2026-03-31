"""
sp100_tickers.py
================
Builds a definitive, enriched S&P 100 ticker list by combining:

  1. Wikipedia  — current S&P 100 constituents list (tickers + company names + sector/industry)
  2. SEC EDGAR  — company_tickers_exchange.json (exchange, EDGAR-canonical name)
  3. SEC EDGAR  — submissions API  (SIC code, SIC description, state of incorporation,
                                    fiscal year end, entity type, phone, address)

Output:
  data/sp100_tickers.json   — full enriched list (used by edgar_metadata.py)
  data/sp100_tickers.csv    — flat CSV (for quick inspection / spreadsheet use)

Why not just Wikipedia?
  Wikipedia is human-maintained and can lag index changes by days. Enriching with
  the SEC's own data gives you stable identifiers (CIK, SIC) and official company
  attributes you'll need for the Knowledge Graph layer.

Why not a paid data provider?
  The three sources above are free, authoritative, and sufficient for our pipeline.
  iShares OEF holdings (CBOE-maintained) would be the gold standard but requires
  scraping a fund fact sheet; Wikipedia + EDGAR covers 99% of the same information.

Usage:
  python sp100_tickers.py

Requirements:
  pip install requests beautifulsoup4 tqdm
"""

import csv
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import requests
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except ImportError as exc:
    sys.exit(f"Missing dependency: {exc}\nRun: pip install requests beautifulsoup4 tqdm")

# ══════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════
UA: str = os.environ.get(
    "EDGAR_USER_AGENT",
    "Your Name (your.email@example.com)",   # ← change this
)
OUTPUT_DIR: Path = Path("data")
MAX_RETRIES: int = 5
BACKOFF_BASE: float = 2.0
SEC_SLEEP: float = 0.15   # stay under 10 req/s

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
        _SESSION.headers.update({
            "User-Agent": UA,
            "Accept-Encoding": "gzip, deflate",
        })
    return _SESSION


def _get(url: str, timeout: int = 30, extra_headers: dict = None) -> requests.Response:
    headers = {}
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session().get(url, timeout=timeout, headers=headers)
            if resp.status_code in (429, 503):
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
                log.warning("HTTP %s attempt %d — waiting %.1fs …", resp.status_code, attempt, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as exc:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            log.warning("Connection error attempt %d — waiting %.1fs: %s", attempt, wait, exc)
            time.sleep(wait)
    raise RuntimeError(f"Failed GET {url} after {MAX_RETRIES} attempts")


# ══════════════════════════════════════════════
#  Source 1: Wikipedia S&P 100 page
# ══════════════════════════════════════════════
WIKI_URL = "https://en.wikipedia.org/wiki/S%26P_100"


def fetch_wikipedia_sp100() -> list[dict]:
    """
    Scrape the Wikipedia S&P 100 constituents table.
    Returns list of {ticker, company_name, gics_sector, gics_sub_industry}.

    Wikipedia is the most up-to-date free source for index membership.
    The table has been stable in structure since 2018.
    """
    log.info("Fetching S&P 100 constituents from Wikipedia …")
    resp = _get(WIKI_URL, extra_headers={"Accept": "text/html"})
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the constituents table — it's the wikitable containing "Symbol" header
    target_table = None
    for table in soup.find_all("table", {"class": "wikitable"}):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "Symbol" in headers:
            target_table = table
            break

    if not target_table:
        raise RuntimeError("Could not find S&P 100 constituents table on Wikipedia")

    headers = [th.get_text(strip=True) for th in target_table.find_all("th")]
    # Typical columns: Symbol, Company, GICS Sector, GICS Sub-Industry
    col_idx = {h: i for i, h in enumerate(headers)}

    results = []
    for row in target_table.find_all("tr")[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        def cell_text(key: str) -> str:
            idx = col_idx.get(key, -1)
            if idx < 0 or idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        ticker = cell_text("Symbol").replace(".", "-")   # BRK.B → BRK-B
        if not ticker:
            continue

        results.append({
            "ticker": ticker,
            "company_name_wiki": cell_text("Company"),
            "gics_sector": cell_text("GICS Sector"),
            "gics_sub_industry": cell_text("GICS Sub-Industry"),
        })

    log.info("  → %d tickers from Wikipedia", len(results))
    return results


# ══════════════════════════════════════════════
#  Source 2: SEC company_tickers_exchange.json
# ══════════════════════════════════════════════
EXCHANGE_JSON_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
TICKERS_JSON_URL  = "https://www.sec.gov/files/company_tickers.json"


def fetch_sec_ticker_map(cache_path: Path) -> dict[str, dict]:
    """
    Download (or load cached) company_tickers_exchange.json.
    Returns {TICKER: {cik, name, exchange}}.

    This file contains exchange info (Nasdaq, NYSE) which company_tickers.json lacks.
    """
    if not cache_path.exists():
        log.info("Downloading company_tickers_exchange.json from SEC …")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        resp = _get(EXCHANGE_JSON_URL)
        cache_path.write_bytes(resp.content)
        time.sleep(SEC_SLEEP)

    with open(cache_path, encoding="utf-8") as fh:
        raw = json.load(fh)

    # Structure: {"fields": ["cik","name","ticker","exchange"], "data": [[...],...]}
    fields = raw["fields"]
    result: dict[str, dict] = {}
    for row in raw["data"]:
        rec = dict(zip(fields, row))
        ticker = str(rec.get("ticker", "")).upper().replace(".", "-")
        if ticker:
            result[ticker] = {
                "cik": str(rec.get("cik", "")).zfill(10),
                "company_name_sec": rec.get("name", ""),
                "exchange": rec.get("exchange", ""),
            }
    return result


def fetch_cik_map(cache_path: Path) -> dict[str, str]:
    """
    Download (or load cached) company_tickers.json.
    Returns {TICKER: zero-padded-CIK}.
    Used as fallback when a ticker isn't in company_tickers_exchange.json.
    """
    if not cache_path.exists():
        log.info("Downloading company_tickers.json from SEC …")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        resp = _get(TICKERS_JSON_URL)
        cache_path.write_bytes(resp.content)
        time.sleep(SEC_SLEEP)

    with open(cache_path, encoding="utf-8") as fh:
        raw = json.load(fh)

    return {
        rec["ticker"].upper(): str(rec["cik_str"]).zfill(10)
        for rec in raw.values()
    }


# ══════════════════════════════════════════════
#  Source 3: data.sec.gov submissions API
# ══════════════════════════════════════════════
def fetch_company_profile(cik10: str) -> dict:
    """
    Fetch company-level metadata from the submissions endpoint.
    Returns a flat dict with SIC, state, fiscal year end, entity type, etc.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    try:
        resp = _get(url)
        data = resp.json()
    except Exception as exc:
        log.warning("  Could not fetch profile for CIK %s: %s", cik10, exc)
        return {}

    addresses = data.get("addresses", {})
    biz = addresses.get("business", {})

    return {
        "sic": data.get("sic", ""),
        "sic_description": data.get("sicDescription", ""),
        "state_of_inc": data.get("stateOfIncorporation", ""),
        "fiscal_year_end": data.get("fiscalYearEnd", ""),   # e.g. "0930" = Sept 30
        "entity_type": data.get("entityType", ""),          # "operating", "investment"
        "phone": data.get("phone", ""),
        "city": biz.get("city", ""),
        "state_or_country": biz.get("stateOrCountry", ""),
        "zip": biz.get("zipCode", ""),
        "former_names": [fn.get("name", "") for fn in data.get("formerNames", [])],
        "ein": data.get("ein", ""),
        "category": data.get("category", ""),               # "Large accelerated filer"
    }


# ══════════════════════════════════════════════
#  Merge and output
# ══════════════════════════════════════════════
def build_sp100_list() -> list[dict]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Wikipedia → base list
    wiki_rows = fetch_wikipedia_sp100()

    # Step 2: SEC exchange map
    exchange_map = fetch_sec_ticker_map(OUTPUT_DIR / "company_tickers_exchange.json")
    cik_map = fetch_cik_map(OUTPUT_DIR / "company_tickers.json")

    # Step 3: Merge Wikipedia + SEC static data
    merged: list[dict] = []
    for row in wiki_rows:
        ticker = row["ticker"]
        sec_info = exchange_map.get(ticker, {})

        # CIK: prefer exchange map (has CIK too), fall back to tickers map
        cik10 = sec_info.get("cik") or cik_map.get(ticker, "")

        merged.append({
            "ticker": ticker,
            "cik": cik10,
            "company_name_wiki": row["company_name_wiki"],
            "company_name_sec": sec_info.get("company_name_sec", ""),
            "exchange": sec_info.get("exchange", ""),
            "gics_sector": row["gics_sector"],
            "gics_sub_industry": row["gics_sub_industry"],
            # To be filled in step 4:
            "sic": "",
            "sic_description": "",
            "state_of_inc": "",
            "fiscal_year_end": "",
            "entity_type": "",
            "phone": "",
            "city": "",
            "state_or_country": "",
            "zip": "",
            "former_names": [],
            "ein": "",
            "category": "",
        })

    # Step 4: Enrich with SEC submissions profile (one API call per ticker)
    log.info("Enriching with SEC submissions profiles …")
    bar = tqdm(merged, desc="Profiles", unit="ticker")
    for entry in bar:
        ticker = entry["ticker"]
        cik10 = entry["cik"]
        bar.set_postfix(ticker=ticker)

        if not cik10:
            log.warning("  No CIK for %s — skipping profile", ticker)
            continue

        profile = fetch_company_profile(cik10)
        entry.update(profile)
        time.sleep(SEC_SLEEP)

    return merged


def save_outputs(records: list[dict]) -> None:
    # JSON
    json_path = OUTPUT_DIR / "sp100_tickers.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2, ensure_ascii=False)
    log.info("Saved JSON → %s", json_path)

    # CSV (flatten lists for spreadsheet compatibility)
    csv_path = OUTPUT_DIR / "sp100_tickers.csv"
    if not records:
        return

    flat_keys = [k for k in records[0] if k != "former_names"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=flat_keys + ["former_names_str"])
        writer.writeheader()
        for rec in records:
            row = {k: rec[k] for k in flat_keys}
            row["former_names_str"] = " | ".join(rec.get("former_names", []))
            writer.writerow(row)
    log.info("Saved CSV  → %s", csv_path)


def run() -> None:
    if UA.startswith("Your Name"):
        log.warning("⚠  Set EDGAR_USER_AGENT to your real name + email before running.")

    records = build_sp100_list()
    save_outputs(records)

    # Quick summary
    missing_cik = [r["ticker"] for r in records if not r["cik"]]
    if missing_cik:
        log.warning("Tickers with no CIK found: %s", missing_cik)

    log.info(
        "Done. %d tickers, %d with SIC codes, %d with exchange info.",
        len(records),
        sum(1 for r in records if r["sic"]),
        sum(1 for r in records if r["exchange"]),
    )


if __name__ == "__main__":
    run()
