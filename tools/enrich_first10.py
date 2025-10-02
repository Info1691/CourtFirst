#!/usr/bin/env python3
"""
Heartbeat enrichment (first 10 rows) for CourtFirst.

Goal
----
- Read the first N rows from data/cases.csv.
- Build *search URLs only* (no network fetch) for JerseyLaw, BAILII, and DuckDuckGo.
- Use smarter queries: title + year + citation (when present).
- Print a heartbeat line per case with running success/skip counters.
- Emit preview artifacts:
    out/preview-enrichment/urls_preview.json
    out/preview-enrichment/skipped_preview.json
    out/preview-enrichment/cases_preview.csv

Inputs
------
- CSV: data/cases.csv
  Expected headers (case-insensitive subset is enough):
    Title, Year, Citation, Jurisdiction, Line
  Extra columns are ignored.

CLI
---
  python tools/enrich_first10.py [--limit 10] [--out out/preview-enrichment] [--abort-after 8]

Exit codes
----------
0: ran successfully
2: aborted due to too many consecutive skips (no-verified-match or empty title)

Notes
-----
- This script *builds* URLs; it does not verify them by requesting the pages.
- The “skip” reason will be “no-title” or “no-verified-match” (the latter means we refused
  to generate a query for obviously non-case headings like roman numeral folios).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple
from urllib.parse import quote_plus


# ---------- Config ----------

CSV_PATH = Path("data/cases.csv")

DEFAULT_LIMIT = 10
DEFAULT_OUT_DIR = Path("out/preview-enrichment")
DEFAULT_ABORT_AFTER = 8  # abort if we see this many consecutive “skips”


# ---------- Helpers ----------

ROMAN_RE = re.compile(r"^(?i)(?:[ivxlcdm]+)\.?$")         # e.g. xxxvii
BLANKISH_RE = re.compile(r"^\s*$")
SECTIONY_RE = re.compile(r"^[A-Z]\.\s")                   # e.g. "B. The Rule ..."
NONCASE_LEADERS = (
    "Table of Cases",
    "Index",
    "Cases after Pitt v Holt",
)

def norm_header(name: str) -> str:
    return name.strip().lower().replace(" ", "_")

def load_csv_rows(csv_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return [], {}
        idx = {norm_header(h): i for i, h in enumerate(header)}
        rows = []
        for r in reader:
            rows.append(r)
    return [dict(_row=r, _rowno=i+2) for i, r in enumerate(rows)], idx

def cell(row: Dict[str, Any], index: Dict[str, int], key: str) -> str:
    i = index.get(key)
    if i is None:
        return ""
    r = row["_row"]
    return r[i].strip() if i < len(r) else ""

def looks_like_non_case_title(title: str) -> bool:
    t = title.strip()
    if BLANKISH_RE.match(t):
        return True
    if ROMAN_RE.match(t):
        return True
    if any(t.startswith(prefix) for prefix in NONCASE_LEADERS):
        return True
    if SECTIONY_RE.match(t):
        return True
    # things like "7-34" page-range leftovers etc
    if re.match(r"^\d+(\s*[-–]\s*\d+)?$", t):
        return True
    return False

def build_query(title: str, year: str, citation: str) -> str:
    """
    Smarter search query:
      - always start with the exact title in quotes
      - then append year (if numeric) and raw citation (if present)
    """
    parts = []
    if title:
        parts.append(f"\"{title}\"")
    y = year.strip()
    if y.isdigit() and len(y) == 4:
        parts.append(y)
    cit = citation.strip()
    if cit:
        parts.append(cit)
    return " ".join(parts).strip()

def pick_primary_engine(citation: str) -> str:
    cit = citation.upper()
    if "JRC" in cit or "JLR" in cit:
        return "jerseylaw"
    return "bailii"

def build_search_urls(query: str, citation: str) -> Dict[str, str]:
    """
    Construct non-fetch search URLs for each engine.
    """
    q = quote_plus(query)
    urls = {
        "jerseylaw_search": f"https://www.jerseylaw.je/search/Pages/Results.aspx?k={q}",
        "bailii_search":    f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={q}",
        "ddg_site_jersey":  f"https://duckduckgo.com/?q={quote_plus(query + ' site:jerseylaw.je')}",
        "ddg_site_bailii":  f"https://duckduckgo.com/?q={quote_plus(query + ' site:bailii.org')}",
        "ddg_open":         f"https://duckduckgo.com/?q={q}",
    }
    urls["primary_suggested"] = urls["jerseylaw_search"] if pick_primary_engine(citation) == "jerseylaw" else urls["bailii_search"]
    return urls

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# ---------- Main ----------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--abort-after", type=int, default=DEFAULT_ABORT_AFTER)
    args = ap.parse_args()

    if not CSV_PATH.exists():
        print(f"[error] CSV not found: {CSV_PATH}", file=sys.stderr)
        return 2

    rows, idx = load_csv_rows(CSV_PATH)
    if not rows:
        print("[error] CSV is empty", file=sys.stderr)
        return 2

    # Map column names we care about (case-insensitive)
    title_key = None
    year_key = None
    citation_key = None
    for want in ("title", "case_title"):
        if want in idx:
            title_key = want
            break
    for want in ("year",):
        if want in idx:
            year_key = want
            break
    for want in ("citation", "cite", "report"):
        if want in idx:
            citation_key = want
            break

    if title_key is None:
        print("[error] CSV must contain a Title column", file=sys.stderr)
        return 2

    limit = max(1, args.limit)
    out_dir = args.out
    ensure_dir(out_dir)

    urls_preview: Dict[str, Any] = {}
    skipped_preview: Dict[str, Any] = {}
    cases_preview_rows: List[List[str]] = [["Title", "Citation", "url"]]

    ok = 0
    skips = 0
    consec_skips = 0

    for i, row in enumerate(rows[:limit], start=1):
        title = cell(row, idx, title_key)
        year = cell(row, idx, year_key) if year_key else ""
        citation = cell(row, idx, citation_key) if citation_key else ""

        status = ""
        if not title or looks_like_non_case_title(title):
            skips += 1
            consec_skips += 1
            skipped_preview[str(i)] = {"title": title, "reason": "no-verified-match" if title else "no-title"}
            status = "skip"
        else:
            # Build smarter query
            query = build_query(title, year, citation)
            urls = build_search_urls(query, citation)
            urls_preview[str(i)] = {
                "title": title,
                "year": year,
                "citation": citation,
                "query": query,
                "urls": urls,
            }
            # prefer the “primary_suggested” for CSV preview
            cases_preview_rows.append([title, citation, urls["primary_suggested"]])
            ok += 1
            consec_skips = 0
            status = "ok"

        rate = f"{(ok+skips)/max(1,i):.2f} cases/s"  # fake “speed” just to keep the same shape
        print(f"[heartbeat] case {i}/{limit} | ok:{ok} skip:{skips} | {rate} | title='{title[:80]}' | {status}")

        if consec_skips >= args.abort_after:
            print(f"!! aborting: {consec_skips} consecutive failures (max {args.abort_after})")
            break

    # Write artifacts
    with (out_dir / "urls_preview.json").open("w", encoding="utf-8") as f:
        json.dump(urls_preview, f, indent=2, ensure_ascii=False)

    with (out_dir / "skipped_preview.json").open("w", encoding="utf-8") as f:
        json.dump(skipped_preview, f, indent=2, ensure_ascii=False)

    with (out_dir / "cases_preview.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(cases_preview_rows)

    print(f"Done. Success={ok} Skipped={skips}")
    return 0 if consec_skips < args.abort_after else 2


if __name__ == "__main__":
    sys.exit(main())
