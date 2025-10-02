#!/usr/bin/env python3
"""
Create a small, safe preview:
- Read data/cases.csv
- Take first N rows with a non-empty Title
- Build search URLs for JerseyLaw & BAILII (+ DuckDuckGo site queries)
- Emit:
  - out/preview-enrichment/cases_preview.csv   (Title,Citation,url)
  - out/preview-enrichment/urls_preview.json   (per-row candidate links)
  - out/preview-enrichment/skipped_preview.json (reasons for skip)
Also prints a heartbeat every row so you can see progress in Actions logs.
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from urllib.parse import quote_plus

def heartbeat(i, total, title, ok_count, skip_count, started_at):
    elapsed = max(time.time() - started_at, 0.001)
    rate = (i + 1) / elapsed
    print(f"[hb] {i+1}/{total} | ok:{ok_count} skip:{skip_count} | {rate:0.2f} cases/s | title='{title[:80]}'", flush=True)

def clean_title(raw):
    if raw is None:
        return ""
    t = raw.strip()
    # collapse repeated whitespace
    t = " ".join(t.split())
    # strip stray quotes
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        t = t[1:-1].strip()
    return t

def build_queries(title, citation):
    """
    We don’t hit the sites; we only construct *search* URLs that
    a human (or a later scraper with a session) can click.
    """
    q_base = title
    if citation:
        q_full = f"{title} {citation}"
    else:
        q_full = title

    # Encode
    q_title = quote_plus(q_base)
    q_full_q = quote_plus(q_full)

    urls = {
        # JerseyLaw’s site search (they recently changed some paths; this query falls back to /search)
        "jerseylaw_search": f"https://www.jerseylaw.je/search/Pages/results.aspx?k={q_title}",
        # BAILII title search (sino_search)
        "bailii_search": f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={q_full_q}",
        # DuckDuckGo helpers with site filters
        "ddg_site_jl": f"https://duckduckgo.com/?q={quote_plus(title)}+site%3Ajerseylaw.je",
        "ddg_site_bailii": f"https://duckduckgo.com/?q={quote_plus(title)}+site%3Abailii.org",
        "ddg_open": f"https://duckduckgo.com/?q={quote_plus(q_full)}",
    }

    # A conservative "primary_suggested": prefer BAILII (widest coverage of UK cases),
    # else JerseyLaw for Jersey cases; we can’t know jurisdiction yet, so default to BAILII.
    urls["primary_suggested"] = urls["bailii_search"]
    return urls

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV with headers including Title and Citation")
    ap.add_argument("--outdir", required=True, help="Output folder for artifacts")
    ap.add_argument("--limit", type=int, default=10, help="How many rows to process")
    ap.add_argument("--sleep-min", type=float, default=0.5)
    ap.add_argument("--sleep-max", type=float, default=1.2)
    ap.add_argument("--max-consec-fail", type=int, default=8)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    rows = []
    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # normalise headers (Title/Citation common in your file)
        fieldnames = {k.lower(): k for k in reader.fieldnames or []}
        get = lambda d, key: d.get(fieldnames.get(key, key), "").strip()
        for r in reader:
            title = clean_title(get(r, "title"))
            citation = get(r, "citation")
            if title:
                rows.append({"title": title, "citation": citation})

    total = min(args.limit, len(rows))
    if total == 0:
        print("No rows found with a non-empty Title.", file=sys.stderr)
        sys.exit(1)

    out_csv = os.path.join(args.outdir, "cases_preview.csv")
    out_urls = os.path.join(args.outdir, "urls_preview.json")
    out_skipped = os.path.join(args.outdir, "skipped_preview.json")

    ok = []
    urls_blob = {}
    skipped = {}
    consec_fail = 0
    start = time.time()

    for i in range(total):
        title = rows[i]["title"]
        citation = rows[i]["citation"]
        try:
            urls = build_queries(title, citation)
            urls_blob[str(i)] = {
                "title": title,
                "year": "",          # (left blank until we parse it later)
                "citation": citation or "",
                "query": title if not citation else f"{title} {citation}",
                "urls": urls,
            }
            ok.append({
                "Title": title,
                "Citation": citation or "",
                "url": urls["primary_suggested"],
            })
            consec_fail = 0
        except Exception as e:
            skipped[str(i)] = {"title": title, "reason": f"error: {e}"}
            consec_fail += 1
            if consec_fail >= args.max_consec_fail:
                print(f"!! aborting: {consec_fail} consecutive failures (max {args.max_consec_fail})", file=sys.stderr)
                break

        heartbeat(i, total, title, len(ok), len(skipped), start)
        time.sleep(random.uniform(args.sleep_min, args.sleep_max))

    # Write files
    with open(out_urls, "w", encoding="utf-8") as f:
        json.dump(urls_blob, f, ensure_ascii=False, indent=2)

    with open(out_skipped, "w", encoding="utf-8") as f:
        json.dump(skipped, f, ensure_ascii=False, indent=2)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Title", "Citation", "url"])
        w.writeheader()
        for r in ok:
            w.writerow(r)

    print(f"Done. Success={len(ok)} Skipped={len(skipped)} Elapsed={time.time()-start:0.1f}s", flush=True)

if __name__ == "__main__":
    main()
