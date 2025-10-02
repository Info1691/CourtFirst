#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
enrich_preview.py
Small, polite URL-enrichment runner with heartbeat + dry-run artifacts.
- Reads CSV with columns: Title, Year, Citation   (other columns ignored)
- Takes a slice [start, end) and/or --limit
- Tries JerseyLaw, then BAILII, then DuckDuckGo (polite sleeps)
- Writes preview CSV + JSON into out/preview/
- Never overwrites the main CSV (dry-run only)
"""

import csv
import json
import os
import re
import sys
import time
import random
import argparse
from pathlib import Path
from urllib.parse import quote_plus

# Optional dependency: duckduckgo_search
_DDG_OK = False
try:
    from duckduckgo_search import DDGS  # type: ignore
    _DDG_OK = True
except Exception:
    _DDG_OK = False


# --------------------------- Utils ---------------------------

def mk_outdir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def norm_title(title: str) -> str:
    """Normalize a case title for searching: trim, collapse spaces, drop trailing section/page hints."""
    if not title:
        return ""
    t = title.strip()
    # kill trailing "… , 12-23" or "(Ch) , 12, 24" page/section tails that slip through
    t = re.sub(r"[,\s]+(\d{1,3}([\-–]\d{1,3})?(,\s*\d{1,3}([\-–]\d{1,3})?)*)\s*$", "", t)
    t = re.sub(r"\s+", " ", t)
    return t


def jersey_law_query(title: str, year: str | None, citation: str | None) -> str | None:
    """
    Build a JerseyLaw search URL (we don't log in; just a public site query).
    We return a *search* URL, not a scraped result, to avoid auth walls.
    """
    base = "https://www.jerseylaw.je/search/Pages/Results.aspx"
    q = title
    bits = []
    if citation:
        bits.append(citation)
    if year and re.fullmatch(r"\d{4}", year or ""):
        bits.append(year)
    if bits:
        q = f'{title} {" ".join(bits)}'
    return f"{base}?k={quote_plus(q)}"


def bailii_query(title: str, year: str | None, citation: str | None) -> str | None:
    base = "https://www.bailii.org/cgi-bin/sino_search_1.cgi"
    q = title
    bits = []
    if citation:
        bits.append(citation)
    if year and re.fullmatch(r"\d{4}", year or ""):
        bits.append(year)
    if bits:
        q = f'{title} {" ".join(bits)}'
    # bailii’s query interface uses 'q' param; we give a simple phrase
    return f"{base}?q={quote_plus(q)}"


def ddg_top_result(title: str, year: str | None, citation: str | None, tmo: float = 10.0) -> str | None:
    """
    Use duckduckgo_search if available; otherwise fall back to a simple DDG HTML endpoint
    and return the first plausible link.
    """
    query = norm_title(title)
    if citation:
        query = f'{query} "{citation}"'
    if year and re.fullmatch(r"\d{4}", year or ""):
        query = f"{query} {year}"

    # Prefer DDGS library (politer)
    if _DDG_OK:
        try:
            with DDGS(timeout=tmo) as ddgs:
                for r in ddgs.text(query, max_results=3, region="uk-en", safesearch="Moderate"):
                    url = (r or {}).get("href") or (r or {}).get("link") or (r or {}).get("url")
                    if url:
                        return url
        except Exception:
            return None

    # Fallback to DDG "lite" html (still just a link to click; we don’t parse aggressively)
    try:
        import requests  # local import to keep optional
        u = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
        resp = requests.get(u, timeout=tmo, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            # naive first link pattern
            m = re.search(r'<a rel="nofollow" class="result__a" href="([^"]+)"', resp.text)
            if m:
                return m.group(1)
    except Exception:
        return None

    return None


def heartbeat(i: int, total: int, every: int = 1):
    if i % every == 0:
        rate = os.environ.get("HEART_RATE", "")
        msg = f"[{time.strftime('%H:%M:%S')}] preview: {i}/{total}"
        if rate:
            msg += f" ~{rate}/s"
        print(msg, flush=True)


# --------------------------- Main ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Preview enrichment for a small slice with heartbeat.")
    ap.add_argument("--input", default="data/cases.csv")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=5, help="end index (exclusive)")
    ap.add_argument("--limit", type=int, default=None, help="limit rows (overrides end if set)")
    ap.add_argument("--sleep-min", type=float, default=2.0)
    ap.add_argument("--sleep-max", type=float, default=4.0)
    ap.add_argument("--heartbeat-every", type=int, default=1)
    ap.add_argument("--outdir", default="out/preview")
    args = ap.parse_args()

    In = Path(args.input)
    Out = Path(args.outdir)
    mk_outdir(Out)

    rows = []
    with In.open(newline="", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh)
        # be permissive on header names
        def get(row, *names):
            for n in names:
                if n in row:
                    return row[n].strip()
            return ""

        for r in rdr:
            rows.append({
                "Title": get(r, "Title", "title"),
                "Year": get(r, "Year", "year"),
                "Citation": get(r, "Citation", "citation"),
            })

    total = len(rows)
    s = max(0, args.start)
    e = min(total, args.end if args.limit is None else s + args.limit)
    work = rows[s:e]

    print(f"Loaded {total} rows; preview slice {s}:{e} ({len(work)} rows).", flush=True)
    print(f"DDG library available: {_DDG_OK}", flush=True)

    preview_csv = Out / "cases_preview.csv"
    preview_json = Out / "urls_preview.json"
    skipped_json = Out / "skipped_preview.json"

    out_rows = []
    url_map = {}
    skipped = []

    for i, r in enumerate(work, start=1):
        heartbeat(i, len(work), every=args.heartbeat_every)
        title = norm_title(r["Title"])
        year = (r["Year"] or "").strip()
        citation = (r["Citation"] or "").strip()

        if not title:
            skipped.append({"idx": s + i - 1, "reason": "empty_title", **r})
            continue

        # priority: JerseyLaw, then BAILII, then DDG
        jl = jersey_law_query(title, year, citation)
        bl = bailii_query(title, year, citation)
        dd = ddg_top_result(title, year, citation)

        # we store the clickable search links + first DDG guess, without scraping the destination
        url_map[str(s + i - 1)] = {
            "title": title,
            "year": year,
            "citation": citation,
            "jerseylaw_search": jl,
            "bailii_search": bl,
            "ddg_top": dd,
        }

        out_rows.append({
            "Title": title,
            "Year": year,
            "Citation": citation,
            "JerseyLaw": jl or "",
            "BAILII": bl or "",
            "TopResult": dd or "",
        })

        # polite sleep
        time.sleep(random.uniform(args.sleep_min, args.sleep_max))

    # write artifacts
    with preview_csv.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["Title", "Year", "Citation", "JerseyLaw", "BAILII", "TopResult"])
        w.writeheader()
        w.writerows(out_rows)

    with preview_json.open("w", encoding="utf-8") as fh:
        json.dump(url_map, fh, ensure_ascii=False, indent=2)

    with skipped_json.open("w", encoding="utf-8") as fh:
        json.dump(skipped, fh, ensure_ascii=False, indent=2)

    print(f"✅ Wrote preview CSV → {preview_csv}")
    print(f"✅ Wrote URL map JSON → {preview_json}")
    if skipped:
        print(f"⚠️ Skipped {len(skipped)} rows → {skipped_json}")
    else:
        print("✅ No skips")
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
