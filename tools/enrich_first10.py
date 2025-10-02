#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enrich the first N cases from data/cases.csv:
- Build conservative search queries (Title + optional Citation year).
- Try JerseyLaw and Bailii search URLs for transparency.
- Also fetch the Bailii search result page and pick a direct document URL if found.
- Write:
  out/preview-enrichment/urls_preview.json
  out/preview-enrichment/skipped_preview.json
  out/preview-enrichment/cases_preview.csv

This script is intentionally self-contained (no repo-local util imports) to avoid
module path problems in Actions. It is *polite* (sleep+jitter) and aborts after
a configurable number of consecutive misses so you can stop early if matching
is failing.

CLI:
  python tools/enrich_first10.py \
      --input data/cases.csv \
      --outdir out/preview-enrichment \
      --limit 10 \
      --abort-after 6 \
      --sleep-min 1.6 \
      --sleep-max 2.6
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
from html import unescape

try:
    import requests
    from bs4 import BeautifulSoup
except Exception as e:
    print("Missing dependencies. Ensure 'requests' and 'beautifulsoup4' are installed.", file=sys.stderr)
    raise

# ---------------------------------------------------------------------------
# Helpers (self-contained)
# ---------------------------------------------------------------------------

def sleep_jitter(min_s: float, max_s: float):
    """Polite sleep with jitter."""
    delay = random.uniform(min_s, max_s)
    time.sleep(delay)

def safe_query(s: str) -> str:
    """Very conservative query string to avoid over-aggressive quoting."""
    s = s.strip()
    # Collapse spaces
    s = re.sub(r"\s+", " ", s)
    # Remove trailing commas/semicolons/dangling punctuation
    s = re.sub(r"[,\.;:\-–—\s]+$", "", s)
    return s

def extract_year_from_citation(cit: str) -> str | None:
    if not cit:
        return None
    m = re.search(r"\[?(\d{4})\]?", cit)
    return m.group(1) if m else None

def build_queries(title: str, citation: str | None):
    """Return a dict of search URLs and a primary suggestion (a Bailii query URL)."""
    q_title = safe_query(title)
    yr = extract_year_from_citation(citation or "")
    q = q_title if not yr else f'{q_title} {yr}'

    # JerseyLaw and Bailii search pages (not direct documents)
    jersey_q = f'https://www.jerseylaw.je/search/Pages/results.aspx?k={requests.utils.quote(q)}'
    bailii_q = f'https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={requests.utils.quote(q)}'
    # DDG fallbacks
    ddg_site_jl   = f'https://duckduckgo.com/?q={requests.utils.quote(q + " site:jerseylaw.je")}'
    ddg_site_bi   = f'https://duckduckgo.com/?q={requests.utils.quote(q + " site:bailii.org")}'
    ddg_open      = f'https://duckduckgo.com/?q={requests.utils.quote(q)}'

    urls = {
        "jerseylaw_search": jersey_q,
        "bailii_search": bailii_q,
        "ddg_site_jl": ddg_site_jl,
        "ddg_site_bailii": ddg_site_bi,
        "ddg_open": ddg_open,
    }
    return urls, bailii_q

def pick_bailii_doc_from_results(bailii_search_url: str, title: str, timeout=20) -> str | None:
    """
    Fetch Bailii search results and pick the most plausible document link.
    Heuristics:
      - Prefer links under /je/cases/ (Jersey) or /ew/cases/ etc. (UK) or /other-LLI paths.
      - If nothing matches, return None (we do NOT fabricate).
    """
    try:
        r = requests.get(bailii_search_url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        # Bailii lists results as <a href="/..."> within an ordered list or similar.
        candidates = []
        for a in soup.find_all("a", href=True):
            href = unescape(a["href"])
            # Normalize to absolute
            if href.startswith("/"):
                href = "https://www.bailii.org" + href
            # Heuristic filters for judgments
            if re.search(r"/(je|ew|uk|sc|ni|ie)/cases/", href, re.I) or "/jud" in href.lower():
                # Light title check (loose)
                candidates.append(href)

        if candidates:
            return candidates[0]  # first plausible document
        return None
    except requests.RequestException:
        return None

def write_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def append_csv(path: str, rows: list[tuple[str,str,str]]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    header_needed = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if header_needed:
            w.writerow(["Title", "Citation", "Url"])
        for row in rows:
            w.writerow(row)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV with at least Title,Citation")
    ap.add_argument("--outdir", required=True, help="Output folder for preview artifacts")
    ap.add_argument("--limit", type=int, default=10, help="How many rows to process from the top")
    ap.add_argument("--abort-after", type=int, default=6, help="Abort after this many consecutive misses")
    ap.add_argument("--sleep-min", type=float, default=1.6)
    ap.add_argument("--sleep-max", type=float, default=2.6)
    args = ap.parse_args()

    in_csv  = args.input
    outdir  = args.outdir
    limit   = args.limit
    max_fail= args.abort_after

    urls_preview_path    = os.path.join(outdir, "urls_preview.json")
    skipped_preview_path = os.path.join(outdir, "skipped_preview.json")
    cases_preview_csv    = os.path.join(outdir, "cases_preview.csv")

    urls_preview = {}
    skipped      = {}
    consecutive_fail = 0

    # Read input
    with open(in_csv, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)

    total = min(limit, len(rows))
    print(f"[start] limit={limit} total_available={len(rows)} processing={total}")

    written_rows: list[tuple[str,str,str]] = []

    for idx in range(total):
        row = rows[idx]
        title = (row.get("Title") or row.get("title") or "").strip()
        citation = (row.get("Citation") or row.get("citation") or "").strip()

        hb = f"[{time.strftime('%H:%M:%S')}] case {idx+1}/{total}"
        print(f"{hb} | title={title!r}")

        if not title:
            skipped[str(idx)] = {"title": title, "reason": "missing-title"}
            consecutive_fail += 1
            if consecutive_fail >= max_fail:
                print(f"!! aborting: {consecutive_fail} consecutive failures (max {max_fail})")
                break
            continue

        # Build queries and try to resolve a direct Bailii document link
        url_set, bailii_search_url = build_queries(title, citation)
        direct = pick_bailii_doc_from_results(bailii_search_url, title)

        urls_preview[str(idx)] = {
            "title": title,
            "citation": citation,
            "query": safe_query(f"{title} {citation}".strip()),
            "urls": {
                **url_set,
                "primary_suggested": bailii_search_url,
                "primary_doc": direct
            }
        }

        if direct:
            written_rows.append((title, citation, direct))
            consecutive_fail = 0
        else:
            # Keep search URL as a fallback in the CSV so you can click *something*
            written_rows.append((title, citation, bailii_search_url))
            consecutive_fail += 1
            if consecutive_fail >= max_fail:
                print(f"!! aborting: {consecutive_fail} consecutive failures (max {max_fail})")
                break

        # Polite pause
        sleep_jitter(args.sleep_min, args.sleep_max)

    # Persist outputs
    if written_rows:
        append_csv(cases_preview_csv, written_rows)
        print(f"WROTE: {cases_preview_csv} ({len(written_rows)} rows)")

    write_json(urls_preview_path, urls_preview)
    write_json(skipped_preview_path, skipped)
    print(f"WROTE: {urls_preview_path}, {skipped_preview_path}")

if __name__ == "__main__":
    sys.exit(main())
