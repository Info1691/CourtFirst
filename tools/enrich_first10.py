# tools/enrich_first10.py
"""
Enrich the first N rows of data/cases.csv by writing a *pure* case URL
into the 'url' column (creating it if missing). Also emits a small preview
CSV + JSON so you can inspect in the Actions artifacts.

Usage (in Actions or local):
  python tools/enrich_first10.py --input data/cases.csv --out data/cases.csv --limit 10 \
      --sleep-min 1.8 --sleep-max 3.4

Notes:
- We DO NOT fabricate. We only resolve links via JerseyLaw/Bailii (or leave empty).
- Heartbeat logs every row so you can see rates and abort in Actions.
"""

import argparse, csv, json, os, sys, time
from typing import Dict, List, Tuple

from util import (
    read_cases_csv, write_cases_csv, ensure_url_column,
    sleep_jitter, pick_best_url
)

def build_candidate_urls(title: str, citation: str) -> Dict[str,str]:
    """
    Build site searches and (rare) direct guesses.
    We keep this minimal & safe: it only constructs searches.
    """
    qs_title = title.replace(" ", "+")
    qs_cite  = (citation or "").replace(" ", "+")
    # JerseyLaw search endpoint (site moved; search page is the stable entry)
    jlib_search = f"https://www.jerseylaw.je/search/Pages/Results.aspx?k={qs_title}"
    if citation:
        jlib_search += f"+{qs_cite}"

    # Bailii “sino_search” with quoted title
    bailii_search = f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query=%22{qs_title}%22"
    if citation:
        bailii_search += f"+{qs_cite}"

    # No direct case guesses here (URLs vary a lot); rely on resolver.
    return {
        "jlib_case": None,
        "bailii_case": None,
        "jlib_search": jlib_search,
        "bailii_search": bailii_search,
        "primary_suggested": bailii_search,  # harmless fallback
    }

def enrich_rows(rows: List[Dict[str,str]], limit: int, sleep_min: float, sleep_max: float) -> Tuple[List[Dict[str,str]], List[Dict[str,str]], List[Dict[str,str]]]:
    """
    Returns (updated_rows, preview_rows, debug_json).
    preview_rows has columns Title,Citation,url for the first N processed.
    debug_json is a per-row dict of url candidates and decisions.
    """
    ensure_url_column(rows)

    preview: List[Dict[str,str]] = []
    debug: List[Dict[str,str]] = []

    start = time.time()
    processed = 0
    for idx, row in enumerate(rows[:limit], start=1):
        title = (row.get("Title") or "").strip()
        citation = (row.get("Citation") or "").strip()
        hb = f"[{time.strftime('%H:%M:%S')}] case {idx}/{limit}"

        if not title:
            print(f"{hb} | skip (empty title)")
            continue

        # Build candidates
        urls = build_candidate_urls(title, citation)
        chosen, plan = pick_best_url(urls)

        # Heartbeat
        rate = processed / max(0.001, (time.time()-start))
        print(f"{hb} | {rate:.2f} cases/s | title='{title[:60]}' | chosen={'(none)' if not chosen else chosen[:80]} | decision={plan.get('decision')}")

        # Update CSV row (only if we found something real; else leave empty)
        if chosen:
            row["url"] = chosen

        preview.append({"Title": title, "Citation": citation, "url": row.get("url","")})
        dbg = {"row": idx, "title": title, "citation": citation, "urls": urls, "decision": plan}
        debug.append(dbg)

        processed += 1
        sleep_jitter(sleep_min, sleep_max)

    return rows, preview, debug

def save_preview(outdir: str, preview_rows: List[Dict[str,str]], debug_json: List[Dict[str,str]]) -> None:
    os.makedirs(outdir, exist_ok=True)
    # CSV
    csvpath = os.path.join(outdir, "cases_preview.csv")
    with open(csvpath, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["Title","Citation","url"])
        wr.writeheader()
        for r in preview_rows:
            wr.writerow(r)
    # URLs debug JSON
    with open(os.path.join(outdir, "urls_preview.json"), "w", encoding="utf-8") as f:
        json.dump(debug_json, f, indent=2, ensure_ascii=False)
    # Skipped list (here: none, but file helps consistency)
    with open(os.path.join(outdir, "skipped_preview.json"), "w", encoding="utf-8") as f:
        f.write("{}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to data/cases.csv")
    ap.add_argument("--out", required=True, help="Where to write updated data/cases.csv")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--sleep-min", type=float, default=1.8)
    ap.add_argument("--sleep-max", type=float, default=3.4)
    ap.add_argument("--outdir", default="out/preview-enrichment")
    args = ap.parse_args()

    rows = read_cases_csv(args.input)
    updated, preview, debug = enrich_rows(rows, args.limit, args.sleep_min, args.sleep_max)

    # Save preview bundle (artifacts)
    save_preview(args.outdir, preview, debug)

    # Write back the CSV with url column preserved/updated for the first N
    write_cases_csv(args.out, updated)

if __name__ == "__main__":
    main()
