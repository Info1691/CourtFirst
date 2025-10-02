#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enrich the first N (or a given window) of cases with *candidate* source URLs
without performing any HTML fetching. Pure standard library; safe for GHA.

Outputs three artifacts under --outdir:
- cases_preview.csv        (Title, Citation, Url)               <- single chosen URL per row
- urls_preview.json        (all candidate URLs & query per row) <- for inspection
- skipped_preview.json     (rows we skipped and why)

Heartbeat logs progress per row (rate, ok/skips).

Usage examples:
  python tools/enrich_first10.py --input data/cases.csv --outdir out/preview-enrichment --limit 10
  python tools/enrich_first10.py --input data/cases.csv --outdir out/preview-enrichment --start 10 --limit 20
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
from typing import Dict, List, Tuple, Optional


# --------------------------
# Utilities (stdlib only)
# --------------------------

def sleep_jitter(min_s: float, max_s: float) -> None:
    """Light randomized backoff between iterations (optional, can be zeroed)."""
    if max_s <= 0:
        return
    if min_s < 0:
        min_s = 0.0
    if max_s < min_s:
        max_s = min_s
    # deterministic-ish jitter that doesn't need random module
    now = time.time()
    frac = now - int(now)
    delay = min_s + (max_s - min_s) * frac
    time.sleep(delay)


def norm(s: str) -> str:
    return (s or "").strip()


def build_query(title: str, citation: str) -> str:
    """
    Build a search query string. Keep it conservative (no fabrication).
    Prefer "Title" + (year/citation tokens if present).
    """
    t = norm(title)
    c = norm(citation)
    # bail out if no title
    if not t:
        return ""
    # include citation only if looks like a bracketed year or reporter token
    parts = [t]
    if c:
        # keep brief; many citations in your CSV already hold page ranges — harmless in search
        parts.append(c)
    return " ".join(parts)


def bailii_search(query: str) -> str:
    base = "https://www.bailii.org/cgi-bin/sino_search_1.cgi"
    q = {"query": query}
    return f"{base}?{urllib.parse.urlencode(q)}"


def jerseylaw_search(query: str) -> str:
    # JL recently changed URLs; result search page still supports 'k=' parameter
    base = "https://www.jerseylaw.je/search/Pages/Results.aspx"
    q = {"k": query}
    return f"{base}?{urllib.parse.urlencode(q)}"


def ddg_open(query: str) -> str:
    # general web search (open)
    base = "https://duckduckgo.com/"
    q = {"q": query}
    return f"{base}?{urllib.parse.urlencode(q)}"


def ddg_site(query: str, site: str) -> str:
    base = "https://duckduckgo.com/"
    q = {"q": f"site:{site} {query}"}
    return f"{base}?{urllib.parse.urlencode(q)}"


def choose_primary(title: str, citation: str, urls: Dict[str, str]) -> str:
    """
    Heuristic choice for the single CSV Url column, **without fetching**:
    - If citation suggests Jersey reports (JLR/JRC/JCA/JCT), prefer JerseyLaw search.
    - Otherwise prefer BAILII search.
    - Fallback to DDG site:Bailii, then open search.
    """
    c = (citation or "").upper()
    looks_jersey = any(tok in c for tok in ("JLR", "JRC", "JCA", "JCT", "ROYAL COURT", "JERSEY"))
    if looks_jersey and urls.get("jerseylaw_search"):
        return urls["jerseylaw_search"]
    if urls.get("bailii_search"):
        return urls["bailii_search"]
    if urls.get("ddg_site_bailii"):
        return urls["ddg_site_bailii"]
    if urls.get("ddg_open"):
        return urls["ddg_open"]
    # as a final fallback: try ddg site on jersey
    if urls.get("ddg_site_jl"):
        return urls["ddg_site_jl"]
    return ""


def read_cases_csv(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Read the input CSV. Accept common headings (case-insensitive):
      - Title (required)
      - Citation (optional)
      - Year (optional)
      - Line / Line_no (optional – we don’t use it here, but preserve for inspection if present)

    Returns: (rows, normalized_header_list)
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV not found: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        sniffer = csv.reader(f)
        rows = list(sniffer)

    if not rows:
        return [], []

    header = rows[0]
    data_rows = rows[1:]

    # map header -> lowercase canonical
    canon = [h.strip() for h in header]
    lower = [h.lower() for h in canon]

    def get(row: List[str], key_variants: List[str]) -> str:
        for kv in key_variants:
            if kv in lower:
                idx = lower.index(kv)
                if idx < len(row):
                    return row[idx]
        return ""

    out: List[Dict[str, str]] = []
    for r in data_rows:
        title = get(r, ["title"])
        citation = get(r, ["citation"])
        year = get(r, ["year"])
        line_no = get(r, ["line", "line_no", "source_line"])
        out.append({
            "Title": title,
            "Citation": citation,
            "Year": year,
            "Line": line_no,
        })

    return out, canon


# --------------------------
# Main
# --------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Preview enrichment (first N / window) with candidate URLs (no fetch).")
    ap.add_argument("--input", required=True, help="Path to cases.csv (must contain Title column).")
    ap.add_argument("--outdir", required=True, help="Directory to write preview artifacts into.")
    ap.add_argument("--start", type=int, default=0, help="Start row index (0-based).")
    ap.add_argument("--limit", type=int, default=10, help="Max rows to process from start.")
    ap.add_argument("--sleep-min", type=float, default=0.0, help="Min sleep between rows (seconds).")
    ap.add_argument("--sleep-max", type=float, default=0.0, help="Max sleep between rows (seconds).")
    ap.add_argument("--abort-after", type=int, default=8, help="Abort after N consecutive failures.")
    args = ap.parse_args()

    src = args.input
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    # Output files
    cases_out = os.path.join(outdir, "cases_preview.csv")
    urls_out = os.path.join(outdir, "urls_preview.json")
    skipped_out = os.path.join(outdir, "skipped_preview.json")

    rows, header = read_cases_csv(src)

    if not rows:
        print("No rows in input CSV; nothing to do.", flush=True)
        # still create empty artifacts so user sees them
        with open(cases_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Title", "Citation", "Url"])
            writer.writeheader()
        json.dump({}, open(urls_out, "w", encoding="utf-8"), indent=2)
        json.dump({}, open(skipped_out, "w", encoding="utf-8"), indent=2)
        return 0

    start = max(0, args.start)
    end = len(rows) if args.limit <= 0 else min(len(rows), start + args.limit)
    window = rows[start:end]

    print(f"Processing {len(window)} cases starting from row {start}", flush=True)

    results: List[Dict[str, str]] = []
    urls_index: Dict[str, Dict[str, str]] = {}
    skipped: Dict[str, Dict[str, str]] = {}

    ok = 0
    ko = 0
    consec_fail = 0
    t0 = time.time()

    for i, row in enumerate(window, 1):
        title = norm(row.get("Title", ""))
        citation = norm(row.get("Citation", ""))
        # safety: bail if no title
        if not title:
            ko += 1
            consec_fail += 1
            skipped_key = str(start + (i - 1))
            skipped[skipped_key] = {"title": title, "reason": "no-title"}
            # heartbeat
            elapsed = max(1e-6, time.time() - t0)
            rate = (ok + ko) / elapsed
            print(f"[{i}/{len(window)}] SKIP (no title) | ok:{ok} skip:{ko} | {rate:.2f} cases/s", flush=True)
            if consec_fail >= args.abort_after:
                print(f"!! aborting: {consec_fail} consecutive failures (max {args.abort_after})", flush=True)
                break
            continue

        try:
            query = build_query(title, citation)
            if not query:
                raise ValueError("empty-query")

            u = {
                "jerseylaw_search": jerseylaw_search(query),
                "bailii_search": bailii_search(query),
                "ddg_site_jl": ddg_site(query, "jerseylaw.je"),
                "ddg_site_bailii": ddg_site(query, "bailii.org"),
                "ddg_open": ddg_open(query),
            }
            primary = choose_primary(title, citation, u)

            # save candidates for inspection
            urls_index[str(start + (i - 1))] = {
                "title": title,
                "year": row.get("Year", ""),
                "citation": citation,
                "query": query,
                "urls": u,
                "primary_suggested": primary,
            }

            results.append({
                "title": title,
                "citation": citation,
                "url": primary,
            })

            ok += 1
            consec_fail = 0

        except Exception as e:
            ko += 1
            consec_fail += 1
            skipped_key = str(start + (i - 1))
            skipped[skipped_key] = {"title": title, "reason": str(e)}

            if consec_fail >= args.abort_after:
                print(f"!! aborting: {consec_fail} consecutive failures (max {args.abort_after})", flush=True)
                break

        # heartbeat
        elapsed = max(1e-6, time.time() - t0)
        rate = (ok + ko) / elapsed
        print(f"[{i}/{len(window)}] case {start + (i - 1)} | ok:{ok} skip:{ko} | {rate:.2f} cases/s | title='{title[:80]}'", flush=True)

        # gentle pacing (optional)
        sleep_jitter(args.sleep_min, args.sleep_max)

    # --------------------------
    # WRITE OUTPUT FILES
    # --------------------------
    with open(cases_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Citation", "Url"])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "Title": r.get("title", ""),
                "Citation": r.get("citation", ""),
                "Url": r.get("url", ""),
            })

    with open(urls_out, "w", encoding="utf-8") as f:
        json.dump(urls_index, f, indent=2)

    with open(skipped_out, "w", encoding="utf-8") as f:
        json.dump(skipped, f, indent=2)

    print(f"Done. Success={ok}  Skipped={ko}  Elapsed={time.time()-t0:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
