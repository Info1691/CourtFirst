#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enrich first N cases (preview + heartbeat).

Reads the first N (default 10) rows from data/cases.csv (columns: Title, Year, Citation, …),
builds search queries, opens the Jerseylaw/Bailli site **search pages**, parses the
results, follows the first result to the **actual case page**, and writes a preview CSV
with columns: Title, Citation, url.

No writes to data/cases.csv – this is a safe preview pass.

Usage examples:
  python tools/enrich_first10.py --input data/cases.csv --outdir out/preview-enrichment
  python tools/enrich_first10.py --input data/cases.csv --outdir out/preview-enrichment --limit 20
  python tools/enrich_first10.py --input data/cases.csv --outdir out/preview-enrichment --start 10 --limit 20

Heartbeat: prints per-case progress and rate.
"""

from __future__ import annotations
import argparse
import csv
import os
import random
import sys
import time
from typing import Dict, Optional, Tuple
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# ----------- HTTP helpers -----------

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"})
    s.timeout = 30
    return s

def sleep_jitter(min_s: float, max_s: float):
    time.sleep(random.uniform(min_s, max_s))

def absolute(base: str, href: str) -> str:
    return urljoin(base, href)

# ----------- site resolvers -----------

def jl_search_url(query: str) -> str:
    # Current JLIB search endpoint for judgments (works with “k=” keyword param)
    # They have moved things a few times; this variant is robust.
    return f"https://www.jerseylaw.je/judgments/Pages/results.aspx?k={quote_plus(query)}"

def bailii_search_url(query: str) -> str:
    return f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={quote_plus(query)}"

def pick_best_url(candidate_urls: Dict[str, Optional[str]]) -> Optional[str]:
    # Preference order: confirmed case page on JLIB, then confirmed BAILII case page,
    # finally the search pages (as last resort).
    for key in ("jlib_case", "bailii_case", "jlib_search", "bailii_search"):
        u = candidate_urls.get(key)
        if u:
            return u
    return None

def resolve_jlib_case(s: requests.Session, title: str, citation: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (case_page_url, search_url).
    We search first, then try to click the top result (judgment page).
    """
    query_bits = [title.strip()]
    if citation:
        query_bits.append(citation.strip())
    q = " ".join(query_bits)

    search_u = jl_search_url(q)
    try:
        r = s.get(search_u)
        r.raise_for_status()
    except Exception:
        return (None, search_u)

    soup = BeautifulSoup(r.text, "html.parser")

    # New JLIB search result layout: list of results with anchor tags under .results or similar.
    # Heuristic: prefer anchors whose href contains '/judgments/' and NOT 'results.aspx'
    link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = " ".join(a.get_text(" ", strip=True).split())
        if not href:
            continue
        # Prefer direct judgment pages
        if "/judgments/" in href and "results.aspx" not in href:
            link = absolute(search_u, href)
            break
    # Fallback: none found – keep only the search page
    return (link, search_u)

def resolve_bailii_case(s: requests.Session, title: str, citation: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (case_page_url, search_url). We hit sino_search_1 and then pick first result link.
    """
    qbits = [title.strip()]
    if citation:
        qbits.append(citation.strip())
    q = " ".join(qbits)

    search_u = bailii_search_url(q)
    try:
        r = s.get(search_u)
        r.raise_for_status()
    except Exception:
        return (None, search_u)

    soup = BeautifulSoup(r.text, "html.parser")
    # BAILII lists results as numbered <a> links near the middle of the page.
    # Grab first anchor that looks like a result (heuristic: contains a court/year path or ends with .html/.htm/.php)
    link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = " ".join(a.get_text(" ", strip=True).split())
        if not href or "sino_search" in href.lower():
            continue
        if href.lower().endswith((".html", ".htm", ".php")) or "/ew/" in href.lower() or "/uk/" in href.lower() or "/je/" in href.lower():
            link = absolute(search_u, href)
            break
    return (link, search_u)

# ----------- CSV I/O -----------

REQ_COLS_CASES = ("Title", "Citation")

def read_cases(input_csv: str, start: int, limit: int) -> list[Dict[str, str]]:
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = [h.strip() for h in reader.fieldnames or []]
        missing = [c for c in REQ_COLS_CASES if c not in headers]
        if missing:
            raise ValueError(f"{input_csv} missing columns: {missing}. Present: {headers}")
        rows = list(reader)
    end = len(rows) if limit <= 0 else min(len(rows), start + limit)
    return rows[start:end]

def write_preview_csv(out_path: str, rows: list[Dict[str, str]]):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Title", "Citation", "url"])
        w.writeheader()
        for r in rows:
            w.writerow({"Title": r.get("Title", ""),
                        "Citation": r.get("Citation", ""),
                        "url": r.get("url", "")})

def write_json(path: str, obj: Dict):
    import json
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ----------- main -----------

def enrich_block(input_csv: str, outdir: str, start: int, limit: int,
                 sleep_min: float, sleep_max: float, abort_after: int) -> None:
    s = _session()
    cases = read_cases(input_csv, start, limit)
    enriched: list[Dict[str, str]] = []
    urls_debug: Dict[int, Dict[str, Optional[str]]] = {}
    skipped: Dict[int, Dict[str, str]] = {}

    n = len(cases)
    ok = 0
    consec_fail = 0
    t0 = time.time()

    print(f"Processing {n} cases starting from row {start}")
    for i, row in enumerate(cases, start=1):
        title = (row.get("Title") or "").strip()
        citation = (row.get("Citation") or "").strip()

        # Heartbeat line
        rate = (ok / max(1.0, (time.time() - t0)))
        print(f"[{i}/{n}] Processing: {title[:92]} | ok={ok} | {rate:.2f} cases/s")

        candidate_urls: Dict[str, Optional[str]] = {"jlib_case": None, "bailii_case": None,
                                                    "jlib_search": None, "bailii_search": None}

        # 1) JLIB (Jersey)
        try:
            case_u, search_u = resolve_jlib_case(s, title, citation)
            candidate_urls["jlib_case"] = case_u
            candidate_urls["jlib_search"] = search_u
        except Exception:
            pass

        # 2) BAILII
        try:
            case_u, search_u = resolve_bailii_case(s, title, citation)
            candidate_urls["bailii_case"] = case_u
            candidate_urls["bailii_search"] = search_u
        except Exception:
            pass

        best = pick_best_url(candidate_urls)
        urls_debug[i] = {"title": title, "citation": citation, **candidate_urls, "primary_suggested": best}

        if best:
            enriched.append({"Title": title, "Citation": citation, "url": best})
            ok += 1
            consec_fail = 0
        else:
            skipped[i] = {"title": title, "citation": citation, "reason": "no-verified-match"}
            consec_fail += 1

        # Abort if too many consecutive failures
        if consec_fail >= abort_after:
            print(f"!! aborting: {consec_fail} consecutive failures (max {abort_after})")
            break

        sleep_jitter(sleep_min, sleep_max)

    # Persist preview files
    write_preview_csv(os.path.join(outdir, "cases_preview.csv"), enriched)
    write_json(os.path.join(outdir, "urls_preview.json"), urls_debug)
    write_json(os.path.join(outdir, "skipped_preview.json"), skipped)

    elapsed = time.time() - t0
    print(f"Done. Success={ok}  Skipped={len(skipped)}  Elapsed={elapsed:.1f}s")


def main():
    ap = argparse.ArgumentParser(description="Enrich first N cases with real case-page URLs (preview only).")
    ap.add_argument("--input", required=True, help="Path to data/cases.csv")
    ap.add_argument("--outdir", required=True, help="Where to write preview artifacts")
    ap.add_argument("--start", type=int, default=0, help="Row offset into CSV (default 0)")
    ap.add_argument("--limit", type=int, default=10, help="How many rows to process (default 10)")
    ap.add_argument("--sleep-min", type=float, default=1.8)
    ap.add_argument("--sleep-max", type=float, default=3.6)
    ap.add_argument("--abort-after", type=int, default=8)
    args = ap.parse_args()

    enrich_block(
        input_csv=args.input,
        outdir=args.outdir,
        start=args.start,
        limit=args.limit,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        abort_after=args.abort_after,
    )


if __name__ == "__main__":
    main()
