"""
edgar_metadata.py
=================
Phase 1 — Fetch and enrich filing metadata for S&P 100 companies.

Flags:
  --force          Re-fetch every ticker, overwriting all existing files.
  --force TICKER   Re-fetch only that ticker (e.g. --force AAPL).

Data source: data.sec.gov/submissions/CIK##########.json  (official SEC REST API)
This is the correct API to use — NOT the legacy Atom/RSS feed.

Why this is better than the Atom feed:
  • Returns ALL historical filings, not just recent (via paginated supplemental files)
  • Rich fields: reportDate, acceptanceDateTime, isXBRL, items (8-K event codes),
    fileNumber, filmNumber, size (bytes), primaryDocument filename
  • Stable, structured JSON — no HTML/feedparser parsing needed
  • Also returns company-level metadata: SIC code, exchange, state of incorporation,
    entityType, phone, address, former names

Output per ticker:  data/edgar_documents/{TICKER}_filings.json
Schema (list of dicts):
  {
    "ticker":               "AAPL",
    "cik":                  "0000320193",
    "company_name":         "Apple Inc.",
    "sic":                  "3571",
    "sic_description":      "Electronic Computers",
    "exchange":             "Nasdaq",
    "state_of_inc":         "CA",
    "fiscal_year_end":      "0924",
    "entity_type":          "operating",

    "accession_number":     "0000320193-24-000123",
    "form":                 "10-K",
    "filed":                "2024-11-01",
    "report_date":          "2024-09-28",
    "accepted_at":          "2024-11-01T06:01:36.000Z",
    "act":                  "34",
    "items":                "",          # populated for 8-K (e.g. "2.02,9.01")
    "is_xbrl":              1,
    "is_inline_xbrl":       1,
    "file_number":          "001-36743",
    "film_number":          "241392654",
    "size":                 15423881,    # bytes
    "primary_document":     "aapl-20240928.htm",
    "primary_doc_desc":     "10-K",
    "filing_index_url":     "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123-index.htm",
    "primary_doc_url":      "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm"
  }

Usage:
  python edgar_metadata.py

Requirements:
  pip install requests tqdm
"""

import argparse
import datetime
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
    from tqdm import tqdm
except ImportError as exc:
    sys.exit(f"Missing dependency: {exc}\nRun: pip install requests tqdm")

# ══════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════
UA: str = os.environ.get(
    "EDGAR_USER_AGENT",
    "Your Name (your.email@example.com)",  # ← change this (SEC policy requires it)
)
OUTPUT_DIR: Path = Path(os.environ.get("EDGAR_OUTPUT_DIR", "data/edgar_documents/edgar_metadata"))
CUTOFF_DATE: datetime.date = datetime.date.fromisoformat(
    os.environ.get("EDGAR_CUTOFF_DATE", "1993-01-01")
)
SP100_JSON: Path = Path("data/sp100_tickers.json")  # produced by sp100_tickers.py
CIK_JSON: Path = Path("data/company_tickers.json")

# Filter: only keep these form types (empty set = keep everything)
FORM_TYPES: set[str] = {
    "10-K", "10-Q", "8-K", "DEF 14A", "4",
    "S-1", "SC 13G", "SC 13D", "13F-HR", "20-F",
}

# HTTP
MAX_RETRIES: int = 5
BACKOFF_BASE: float = 2.0
INTER_TICKER_SLEEP: float = 0.4   # stay well under 10 req/s SEC limit

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
#  HTTP helpers
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


def _get(url: str, timeout: int = 30) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session().get(url, timeout=timeout)
            if resp.status_code in (429, 503):
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 1.5)
                log.warning("HTTP %s (attempt %d) — waiting %.1fs …", resp.status_code, attempt, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as exc:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            log.warning("Connection error (attempt %d): %s — waiting %.1fs …", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed GET {url} after {MAX_RETRIES} attempts")


# ══════════════════════════════════════════════
#  CIK loader  (auto-downloads if missing)
# ══════════════════════════════════════════════
def load_ticker_to_cik(path: Path) -> dict[str, str]:
    """Returns {TICKER: '0000320193'} (zero-padded 10-digit strings)."""
    if not path.exists():
        log.info("Downloading company_tickers.json from SEC …")
        path.parent.mkdir(parents=True, exist_ok=True)
        resp = _get("https://www.sec.gov/files/company_tickers.json")
        path.write_bytes(resp.content)
        log.info("Saved → %s", path)

    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    return {
        rec["ticker"].upper(): str(rec["cik_str"]).zfill(10)
        for rec in raw.values()
    }


# ══════════════════════════════════════════════
#  S&P 100 loader
# ══════════════════════════════════════════════
def load_sp100(path: Path) -> list[dict]:
    """Load the enriched ticker list produced by sp100_tickers.py."""
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run sp100_tickers.py first to generate it."
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ══════════════════════════════════════════════
#  Core: fetch all submissions for one CIK
# ══════════════════════════════════════════════
SUBMISSIONS_BASE = "https://data.sec.gov/submissions"


def _fetch_submissions_json(cik10: str) -> dict:
    """Fetch the primary CIK submissions JSON from data.sec.gov."""
    url = f"{SUBMISSIONS_BASE}/CIK{cik10}.json"
    resp = _get(url)
    return resp.json()


def _fetch_supplemental_file(filename: str) -> dict:
    """Fetch a supplemental pagination file (older filings overflow here)."""
    url = f"{SUBMISSIONS_BASE}/{filename}"
    resp = _get(url)
    return resp.json()


def _columnar_to_rows(recent: dict) -> list[dict]:
    """
    The submissions API returns filings as columnar arrays (parallel lists),
    not as a list of objects. Convert to a list of dicts.

    Keys present: accessionNumber, filingDate, reportDate, acceptanceDateTime,
                  act, form, fileNumber, filmNumber, items, size,
                  isXBRL, isInlineXBRL, primaryDocument, primaryDocDescription
    """
    keys = list(recent.keys())
    if not keys:
        return []
    n = len(recent[keys[0]])
    rows = []
    for i in range(n):
        rows.append({k: recent[k][i] for k in keys})
    return rows


def fetch_all_filings(cik10: str) -> tuple[dict, list[dict]]:
    """
    Fetch every filing row for a CIK by following pagination files.

    Returns (company_info_dict, [filing_row, ...])
    company_info contains: name, sic, sicDescription, exchanges, tickers,
                           stateOfIncorporation, fiscalYearEnd, entityType, etc.
    """
    data = _fetch_submissions_json(cik10)
    time.sleep(0.12)

    # Company-level metadata
    company_info = {
        "name": data.get("name", ""),
        "sic": data.get("sic", ""),
        "sic_description": data.get("sicDescription", ""),
        "exchanges": data.get("exchanges", []),
        "tickers": data.get("tickers", []),
        "state_of_inc": data.get("stateOfIncorporation", ""),
        "fiscal_year_end": data.get("fiscalYearEnd", ""),
        "entity_type": data.get("entityType", ""),
        "phone": data.get("phone", ""),
        "business_address": data.get("addresses", {}).get("business", {}),
        "former_names": data.get("formerNames", []),
        "ein": data.get("ein", ""),
    }

    # Recent filings (columnar → rows)
    all_rows: list[dict] = _columnar_to_rows(data["filings"]["recent"])

    # Paginated supplemental files (older filings)
    for file_entry in data["filings"].get("files", []):
        filename = file_entry["name"]          # e.g. "CIK0000320193-submissions-001.json"
        sup = _fetch_supplemental_file(filename)
        all_rows.extend(_columnar_to_rows(sup))
        time.sleep(0.12)

    return company_info, all_rows


# ══════════════════════════════════════════════
#  Build enriched filing record
# ══════════════════════════════════════════════
_ARCHIVE_BASE = "https://www.sec.gov/Archives/edgar/data"


def _build_record(
    ticker: str,
    cik10: str,
    company_info: dict,
    row: dict,
) -> Optional[dict]:
    """
    Convert a raw columnar row + company info into a rich, flat filing record.
    Returns None if the filing is filtered out (wrong form type or before cutoff).
    """
    form = row.get("form", "").strip()
    if FORM_TYPES and form not in FORM_TYPES:
        return None

    filed_str = row.get("filingDate", "")
    if not filed_str:
        return None
    try:
        filed = datetime.date.fromisoformat(filed_str)
    except ValueError:
        return None

    if filed < CUTOFF_DATE:
        return None

    # Build URLs from accession number
    acc = row.get("accessionNumber", "")            # "0000320193-24-000123"
    acc_nodash = acc.replace("-", "")               # "000032019324000123"
    cik_int = str(int(cik10))                       # "320193" (no leading zeros for URL)
    primary_doc = row.get("primaryDocument", "")

    index_url = (
        f"{_ARCHIVE_BASE}/{cik_int}/{acc_nodash}/{acc}-index.htm"
        if acc else ""
    )
    primary_doc_url = (
        f"{_ARCHIVE_BASE}/{cik_int}/{acc_nodash}/{primary_doc}"
        if acc and primary_doc else index_url
    )

    return {
        # ── Company identifiers ──
        "ticker":           ticker,
        "cik":              cik10,
        "company_name":     company_info.get("name", ""),
        "sic":              company_info.get("sic", ""),
        "sic_description":  company_info.get("sic_description", ""),
        "exchange":         ", ".join(company_info.get("exchanges", [])),
        "state_of_inc":     company_info.get("state_of_inc", ""),
        "fiscal_year_end":  company_info.get("fiscal_year_end", ""),
        "entity_type":      company_info.get("entity_type", ""),

        # ── Filing identifiers ──
        "accession_number": acc,
        "form":             form,
        "filed":            filed_str,
        "report_date":      row.get("reportDate", ""),        # period of report
        "accepted_at":      row.get("acceptanceDateTime", ""),
        "act":              row.get("act", ""),               # Securities Act (e.g. "34")
        "items":            row.get("items", ""),             # 8-K item codes, e.g. "2.02,9.01"
        "file_number":      row.get("fileNumber", ""),
        "film_number":      row.get("filmNumber", ""),
        "size":             row.get("size", 0),               # bytes
        "is_xbrl":         row.get("isXBRL", 0),
        "is_inline_xbrl":  row.get("isInlineXBRL", 0),
        "primary_document": primary_doc,
        "primary_doc_desc": row.get("primaryDocDescription", ""),

        # ── URLs ──
        "filing_index_url": index_url,
        "primary_doc_url":  primary_doc_url,
    }


# ══════════════════════════════════════════════
#  Main pipeline
# ══════════════════════════════════════════════
def process_ticker(ticker: str, cik10: str, company_extra: dict) -> list[dict]:
    """Fetch, filter, and enrich all filings for one ticker."""
    company_info, raw_rows = fetch_all_filings(cik10)

    # Merge in any extra info from the S&P 100 enriched list
    company_info.update({k: v for k, v in company_extra.items() if v})

    records = []
    for row in raw_rows:
        rec = _build_record(ticker, cik10, company_info, row)
        if rec:
            records.append(rec)

    # Sort newest → oldest
    records.sort(key=lambda r: r["filed"], reverse=True)
    return records


def _metadata_path(ticker: str) -> Path:
    return OUTPUT_DIR / f"{ticker}_filings.json"


def _is_stale(path: Path, cutoff: datetime.date) -> bool:
    """
    Return True if the existing file should be re-fetched.
    A file is stale if:
      (a) it doesn't exist or is empty, OR
      (b) its oldest filing date is later than cutoff  ← the original bug:
          files written with CUTOFF=2023 will have no records before 2023.
    """
    if not path.exists() or path.stat().st_size == 0:
        return True
    try:
        with open(path, encoding="utf-8") as fh:
            records = json.load(fh)
        if not records:
            return True
        # Records are sorted newest→oldest, so last record is the oldest
        oldest_filed = datetime.date.fromisoformat(records[-1]["filed"])
        # If oldest record is newer than cutoff + 1 year buffer, file is stale
        # (buffer avoids re-fetching when the company simply didn't file before cutoff)
        stale_threshold = cutoff + datetime.timedelta(days=365)
        if oldest_filed > stale_threshold:
            log.info("  ⚠  %s is stale (oldest filing %s > cutoff %s) — will re-fetch",
                     path.name, oldest_filed, cutoff)
            return True
        return False
    except (json.JSONDecodeError, KeyError, ValueError):
        return True  # corrupt file → re-fetch


def run() -> None:
    parser = argparse.ArgumentParser(description="Fetch EDGAR filing metadata for S&P 100")
    parser.add_argument(
        "--force", nargs="?", const="ALL", metavar="TICKER",
        help="Force re-fetch: omit value = all tickers, or pass a ticker e.g. --force AAPL"
    )
    args = parser.parse_args()
    force_ticker = args.force  # "ALL", a specific ticker string, or None

    if UA.startswith("Your Name"):
        log.warning(
            "⚠  Set EDGAR_USER_AGENT env var to your real name + email "
            "(SEC fair-use policy requires this)."
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ticker_to_cik = load_ticker_to_cik(CIK_JSON)

    # Load S&P 100 list (from sp100_tickers.py)
    try:
        sp100 = load_sp100(SP100_JSON)
    except FileNotFoundError as exc:
        log.warning("%s — falling back to ticker-only mode", exc)
        _FALLBACK = [
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
        sp100 = [{"ticker": t} for t in _FALLBACK]

    if force_ticker == "ALL":
        log.info("--force ALL: re-fetching every ticker regardless of existing files")
    elif force_ticker:
        log.info("--force %s: re-fetching only that ticker", force_ticker.upper())

    log.info("Processing %d tickers | cutoff=%s", len(sp100), CUTOFF_DATE)

    skipped = refetched = fetched = failed = 0

    bar = tqdm(sp100, desc="Metadata", unit="ticker")
    for entry in bar:
        ticker = entry["ticker"].upper()
        bar.set_postfix(ticker=ticker)
        out_path = _metadata_path(ticker)

        # ── Decide whether to skip ──────────────────────────────────────────
        if force_ticker == "ALL":
            should_fetch = True
        elif force_ticker and force_ticker.upper() == ticker:
            should_fetch = True
        else:
            should_fetch = _is_stale(out_path, CUTOFF_DATE)

        if not should_fetch:
            log.debug("Up-to-date — skipping %s", ticker)
            skipped += 1
            continue

        if ticker not in ticker_to_cik:
            log.warning("No CIK for %s — skipping", ticker)
            failed += 1
            continue

        cik10 = ticker_to_cik[ticker]
        action = "Re-fetching" if out_path.exists() else "Fetching"
        log.info("%s %s (CIK %s) …", action, ticker, cik10)

        try:
            records = process_ticker(ticker, cik10, entry)
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(records, fh, indent=2, ensure_ascii=False)
            log.info("  ✓ %s — %d filings saved", ticker, len(records))
            refetched += 1 if out_path.exists() else 0
            fetched += 1
        except Exception as exc:
            log.error("  ✗ %s — %s", ticker, exc)
            failed += 1

        time.sleep(INTER_TICKER_SLEEP)

    log.info(
        "Done. fetched=%d  skipped=%d  failed=%d | Output: %s",
        fetched, skipped, failed, OUTPUT_DIR.resolve()
    )


if __name__ == "__main__":
    run()