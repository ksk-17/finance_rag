"""
edgar_data_retrieval.py
=======================
Phase 2 — Download actual filing documents using metadata from Phase 1.

Reads:   data/edgar_documents/{TICKER}_filings.json  (from edgar_metadata.py)
Writes:  data/edgar_documents/{TICKER}/{FORM}_{FILED}_{ACCESSION}.{ext}

No re-fetching of metadata. No HTML scraping. Every URL needed is already
in the metadata file (primary_doc_url, filing_index_url). This script
purely downloads and organises documents on disk.

Key design decisions:
  - primary_doc_url is used first (direct document link from metadata)
  - filing_index_url is used as fallback (scrapes index page to find doc)
  - Files are skipped if already downloaded and non-empty (safe to resume)
  - Form 4 (insider trades) are skipped by default — high volume, low text value
    for RAG. Toggle SKIP_FORM4 = False to include them.
  - Very large files (> MAX_FILE_MB) are skipped with a warning
  - A download manifest (download_manifest.json) is written per ticker
    so downstream pipeline knows exactly what's on disk

Flags:
  --tickers AAPL MSFT     Download only these tickers
  --forms   10-K 10-Q     Download only these form types
  --force                 Re-download even if file already exists
  --dry-run               Print what would be downloaded, don't download

Usage:
  python edgar_data_retrieval.py
  python edgar_data_retrieval.py --tickers AAPL MSFT NVDA
  python edgar_data_retrieval.py --forms 10-K 10-Q 8-K
  python edgar_data_retrieval.py --tickers AAPL --force

Requirements:
  pip install requests beautifulsoup4 tqdm
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin


import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ══════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════
UA: str = os.environ.get(
    "EDGAR_USER_AGENT",
    "Your Name (your.email@example.com)",   # ← required by SEC policy
)

# Directories — must match edgar_metadata.py
METADATA_DIR: Path = Path(os.environ.get("EDGAR_OUTPUT_DIR", "data/edgar_documents/edgar_metadata"))
DOCS_DIR: Path = Path(os.environ.get("EDGAR_OUTPUT_DIR", "data/edgar_documents/edgar_fillings"))

# Form types to download (subset of what metadata contains)
# Removing Form 4 by default — 1000s of tiny XML files, low RAG value
DOWNLOAD_FORMS: set[str] = {
    "10-K",     # Annual report        ← highest priority
    "10-Q",     # Quarterly report
    "8-K",      # Material events
    "DEF 14A",  # Proxy statement
    "S-1",      # IPO registration
    "SC 13G",   # Passive large-stake
    "SC 13D",   # Active large-stake
}

SKIP_FORM4: bool = True          # set False to also download Form 4 insider trades
MAX_FILE_MB: int = 150           # skip files larger than this (avoids huge XBRL packages)
MAX_RETRIES: int = 5
BACKOFF_BASE: float = 2.0
INTER_DOC_SLEEP: float = 0.12   # stay under SEC's 10 req/s limit
INTER_TICKER_SLEEP: float = 0.5

SEC_BASE = "https://www.sec.gov"

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
#  HTTP session
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


def _get(url: str, timeout: int = 45, stream: bool = False) -> requests.Response:
    """GET with exponential back-off on 429/503/connection errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session().get(url, timeout=timeout, stream=stream)
            if resp.status_code == 404:
                raise FileNotFoundError(f"404 Not Found: {url}")
            if resp.status_code in (429, 503):
                wait = BACKOFF_BASE ** attempt + random.uniform(0, 1.5)
                log.warning("  HTTP %s attempt %d — waiting %.1fs …",
                            resp.status_code, attempt, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            log.warning("  Connection error attempt %d — %.1fs: %s", attempt, wait, exc)
            time.sleep(wait)
    raise RuntimeError(f"Failed GET {url} after {MAX_RETRIES} attempts")


# ══════════════════════════════════════════════
#  Metadata loader
# ══════════════════════════════════════════════
def load_metadata(ticker: str) -> list[dict]:
    path = METADATA_DIR / f"{ticker}_filings.json"
    if not path.exists():
        raise FileNotFoundError(
            f"No metadata for {ticker} at {path}. Run edgar_metadata.py first."
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def list_available_tickers() -> list[str]:
    """Return all tickers that have a metadata file."""
    return sorted(
        p.stem.replace("_filings", "")
        for p in METADATA_DIR.glob("*_filings.json")
    )


# ══════════════════════════════════════════════
#  URL resolution
# ══════════════════════════════════════════════
def _resolve_url_from_index(index_url: str) -> Optional[str]:
    """
    Fallback: visit the EDGAR filing index page and extract the primary
    document URL. Only called when primary_doc_url is missing or fails.
    """
    try:
        resp = _get(index_url, timeout=30)
    except Exception as exc:
        log.debug("  Index fetch failed (%s): %s", index_url, exc)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Strategy 1: tableFile → first data row
    for table in soup.select("table.tableFile"):
        for row in table.select("tr")[1:]:
            link = row.find("a", href=True)
            if link:
                href = link["href"]
                if "doc=" in href:
                    href = href.split("doc=")[1]
                return urljoin(SEC_BASE, href)

    # Strategy 2: any Archives link ending in a document extension
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/Archives/edgar/data/" in href and href.lower().endswith(
            (".htm", ".html", ".txt", ".xml")
        ):
            return urljoin(SEC_BASE, href)

    return None


def resolve_document_url(filing: dict) -> Optional[str]:
    """
    Return the best available URL for the actual document content.
    primary_doc_url from metadata is already correct in 99% of cases —
    only fall back to scraping the index page if it's missing.
    """
    url = filing.get("primary_doc_url", "").strip()
    if url and url != filing.get("filing_index_url", "").strip():
        return url  # metadata has a direct doc URL — use it

    # primary_doc_url is empty or same as index → scrape the index
    index_url = filing.get("filing_index_url", "").strip()
    if index_url:
        log.debug("  Resolving via index page for %s/%s",
                  filing.get("ticker"), filing.get("accession_number"))
        return _resolve_url_from_index(index_url)

    return None


# ══════════════════════════════════════════════
#  File path construction
# ══════════════════════════════════════════════
def _safe(s: str) -> str:
    """Make a string safe for use in a filename."""
    return s.replace(" ", "-").replace("/", "_").replace("\\", "_")


def doc_path(ticker: str, filing: dict) -> Path:
    """
    Stable, descriptive path for a downloaded document.

    Pattern: {TICKER}/{FORM}_{FILED}_{ACCESSION}.{ext}
    Example: AAPL/10-K_2024-11-01_0000320193-24-000123.htm
    """
    form    = _safe(filing.get("form", "UNKNOWN"))
    filed   = filing.get("filed", "0000-00-00")
    acc     = filing.get("accession_number", "").replace("-", "")[:20]
    primary = filing.get("primary_document", "")
    ext     = Path(primary).suffix.lower() if primary else ".txt"
    if not ext or ext not in {".htm", ".html", ".txt", ".xml", ".pdf"}:
        ext = ".txt"

    filename = f"{form}_{filed}_{acc}{ext}"
    return DOCS_DIR / ticker / filename


# ══════════════════════════════════════════════
#  Downloader
# ══════════════════════════════════════════════
def download_filing(filing: dict, dest: Path, force: bool = False) -> str:
    """
    Download one filing document.

    Returns one of: "ok" | "skipped" | "too_large" | "no_url" | "error"
    """
    if not force and dest.exists() and dest.stat().st_size > 0:
        return "skipped"

    url = resolve_document_url(filing)
    if not url:
        log.debug("  No URL resolved for %s %s",
                  filing.get("form"), filing.get("accession_number"))
        return "no_url"

    # Check file size before downloading large files
    expected_bytes = filing.get("size", 0)
    if expected_bytes and expected_bytes > MAX_FILE_MB * 1_000_000:
        log.debug("  Skipping oversized file (%.1f MB): %s",
                  expected_bytes / 1_000_000, url)
        return "too_large"

    try:
        resp = _get(url, stream=True)

        # Double-check actual Content-Length if metadata size was missing
        content_length = int(resp.headers.get("Content-Length", 0))
        if content_length > MAX_FILE_MB * 1_000_000:
            log.debug("  Skipping oversized file (%.1f MB via header): %s",
                      content_length / 1_000_000, url)
            return "too_large"

        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)
        return "ok"

    except FileNotFoundError:
        return "no_url"
    except Exception as exc:
        log.warning("  Download error for %s: %s", url, exc)
        if dest.exists():
            dest.unlink()   # remove partial file
        return "error"


# ══════════════════════════════════════════════
#  Manifest writer
# ══════════════════════════════════════════════
def write_manifest(ticker: str, results: list[dict]) -> None:
    """
    Write a per-ticker download manifest so downstream pipeline (chunker,
    embedder, KG builder) knows exactly which files are on disk and what
    metadata they carry.

    Schema: list of {dest_path, status, ...all filing metadata fields}
    """
    path = DOCS_DIR / ticker / "download_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)


# ══════════════════════════════════════════════
#  Per-ticker orchestration
# ══════════════════════════════════════════════
def process_ticker(
    ticker: str,
    form_filter: Optional[set[str]],
    force: bool,
    dry_run: bool,
) -> dict:
    """Download all filtered filings for one ticker. Returns summary stats."""
    try:
        filings = load_metadata(ticker)
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return {"ticker": ticker, "ok": 0, "skipped": 0,
                "too_large": 0, "no_url": 0, "error": 0, "total": 0}

    # Apply form filter
    effective_forms = form_filter or DOWNLOAD_FORMS
    if SKIP_FORM4:
        effective_forms = effective_forms - {"4", "4/A"}

    filings = [f for f in filings if f.get("form", "") in effective_forms]

    stats = {"ticker": ticker, "ok": 0, "skipped": 0,
             "too_large": 0, "no_url": 0, "error": 0, "total": len(filings)}

    manifest_rows = []

    for filing in filings:
        dest = doc_path(ticker, filing)

        if dry_run:
            url = resolve_document_url(filing)
            log.info("  [DRY-RUN] %s %s → %s", filing["form"], filing["filed"], dest.name)
            stats["ok"] += 1
            continue

        status = download_filing(filing, dest, force=force)
        stats[status] += 1

        manifest_rows.append({
            **filing,
            "local_path": str(dest),
            "download_status": status,
        })

        time.sleep(INTER_DOC_SLEEP)

    if not dry_run:
        write_manifest(ticker, manifest_rows)

    return stats


# ══════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Phase 2: Download EDGAR filing documents from metadata"
    )
    p.add_argument(
        "--tickers", nargs="+", metavar="TICKER",
        help="Only process these tickers (default: all with metadata files)"
    )
    p.add_argument(
        "--forms", nargs="+", metavar="FORM",
        help=f"Only download these form types (default: {sorted(DOWNLOAD_FORMS)})"
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-download files that already exist on disk"
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be downloaded without downloading anything"
    )
    return p.parse_args()


def run() -> None:
    args = parse_args()

    if UA.startswith("Your Name"):
        log.warning("⚠  Set EDGAR_USER_AGENT to your real name + email (SEC policy).")

    # Resolve tickers
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
        missing = [t for t in tickers
                   if not (METADATA_DIR / f"{t}_filings.json").exists()]
        if missing:
            log.warning("No metadata files found for: %s — run edgar_metadata.py first", missing)
            tickers = [t for t in tickers if t not in missing]
    else:
        tickers = list_available_tickers()

    if not tickers:
        log.error("No tickers to process. Run edgar_metadata.py first.")
        sys.exit(1)

    # Resolve form filter
    form_filter = set(args.forms) if args.forms else None

    log.info(
        "Downloading documents for %d tickers | forms=%s | force=%s | dry-run=%s",
        len(tickers),
        sorted(form_filter) if form_filter else f"default ({len(DOWNLOAD_FORMS)} types)",
        args.force,
        args.dry_run,
    )

    # ── Main loop ──────────────────────────────────────────────────────────
    all_stats = []
    total_ok = total_skip = total_err = 0

    ticker_bar = tqdm(tickers, desc="Tickers", unit="ticker")
    for ticker in ticker_bar:
        ticker_bar.set_postfix(ticker=ticker)
        log.info("── %s ──────────────────────", ticker)

        stats = process_ticker(ticker, form_filter, args.force, args.dry_run)
        all_stats.append(stats)

        total_ok   += stats["ok"]
        total_skip += stats["skipped"]
        total_err  += stats["error"] + stats["no_url"]

        log.info(
            "  %s done: %d downloaded, %d skipped, %d too-large, %d no-url, %d errors",
            ticker, stats["ok"], stats["skipped"],
            stats["too_large"], stats["no_url"], stats["error"],
        )
        time.sleep(INTER_TICKER_SLEEP)

    # ── Summary ────────────────────────────────────────────────────────────
    log.info("═" * 60)
    log.info(
        "COMPLETE: %d downloaded | %d skipped | %d errors/no-url",
        total_ok, total_skip, total_err,
    )
    log.info("Output: %s", DOCS_DIR.resolve())

    # Write overall run summary
    summary_path = DOCS_DIR / "retrieval_summary.json"
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump({
            "tickers_processed": len(tickers),
            "total_downloaded": total_ok,
            "total_skipped": total_skip,
            "total_errors": total_err,
            "per_ticker": all_stats,
        }, fh, indent=2)
    log.info("Run summary → %s", summary_path)


if __name__ == "__main__":
    run()