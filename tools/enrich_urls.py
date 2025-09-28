#!/usr/bin/env python3
import argparse, csv, sys, time, html, re
from pathlib import Path
from typing import List, Tuple, Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

HEADERS = {"User-Agent": "CourtFirst/0.1 (+GitHub Actions)"}

def ddg_site_search(site: str, query: str, max_results: int = 10) -> List[Tuple[str, str]]:
    """DuckDuckGo HTML search with site: filter. Returns [(title, url), ...]."""
    url = "https://duckduckgo.com/html/"
    params = {"q": f"site:{site} {query}"}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[ddg] ERR {site}: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    out: List[Tuple[str, str]] = []
    # Result anchors usually in .result__a, but fall back to generic list
    for a in soup.select("a.result__a, a[href]"):
        title = a.get_text(" ", strip=True)
        href = a.get("href")
        if not title or not href:
            continue
        # ddg uses redirect links sometimes; we keep as-is since it resolves in browser
        if site in href:
            out.append((title, href))
        if len(out) >= max_results:
            break
    return out

def normalise_title(s: str) -> str:
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    # Drop common suffix noise
    s = re.sub(r"\s*\[[0-9]{4}\].*?$", "", s)   # trim trailing [2019].. segment if present
    return s

def best_match(title: str, candidates: List[Tuple[str, str]]) -> Optional[Tuple[str, str, int]]:
    """Return (title, url, score) of best fuzzy match or None."""
    title_norm = normalise_title(title).lower()
    best = None
    for cand_title, cand_url in candidates:
        cand_norm = normalise_title(cand_title).lower()
        score = fuzz.token_sort_ratio(title_norm, cand_norm)
        if best is None or score > best[2]:
            best = (cand_title, cand_url, score)
    return best

def read_cases(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        needed = {"case_id", "jurisdiction", "title", "year", "source_url"}
        missing = needed - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} missing columns: {sorted(missing)}")
        for row in reader:
            rows.append(row)
    return rows

def write_cases(path: Path, rows: List[dict]) -> None:
    fieldnames = ["case_id", "jurisdiction", "title", "year", "source_url", "match_site", "match_score"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    ap = argparse.ArgumentParser(description="Search-only URL enrichment for cases.csv")
    ap.add_argument("--cases", required=True, help="Input CSV: case_id,jurisdiction,title,year,source_url")
    ap.add_argument("--out", required=True, help="Output CSV with source_url filled where found")
    ap.add_argument("--min-score", type=int, default=86, help="Min fuzzy score to accept")
    ap.add_argument("--max-results-per-site", type=int, default=10, help="Max results/site to consider")
    args = ap.parse_args()

    cases = read_cases(Path(args.cases))
    enriched: List[dict] = []
    for i, row in enumerate(cases, 1):
        cid = row.get("case_id", "").strip()
        title = row.get("title", "").strip()
        year = (row.get("year") or "").strip()
        existing = (row.get("source_url") or "").strip()

        if not title:
            enriched.append({**row, "match_site": "", "match_score": ""})
            continue

        if existing:
            # Keep existing link, just copy through
            enriched.append({**row, "match_site": "existing", "match_score": "100"})
            continue

        query = f"{title} {year}".strip()
        best_pick = None

        # JerseyLaw first (preferred)
        jl = ddg_site_search("www.jerseylaw.je", query, max_results=args.max_results_per_site)
        bm = best_match(title, jl)
        if bm and bm[2] >= args.min_score:
            best_pick = ("jerseylaw.je", bm[1], bm[2])

        # If nothing strong on JerseyLaw, try BAILII
        if not best_pick:
            bl = ddg_site_search("www.bailii.org", query, max_results=args.max_results_per_site)
            bm = best_match(title, bl)
            if bm and bm[2] >= args.min_score:
                best_pick = ("bailii.org", bm[1], bm[2])

        if best_pick:
            site, url, score = best_pick
            print(f"[{i}/{len(cases)}] {cid}: matched {site} ({score}) -> {url}")
            row["source_url"] = url
            row["match_site"] = site
            row["match_score"] = str(score)
        else:
            print(f"[{i}/{len(cases)}] {cid}: no confident match")
            row["match_site"] = ""
            row["match_score"] = ""

        enriched.append(row)
        time.sleep(0.6)  # gentle rate-limit

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_cases(out_path, enriched)
    print(f"Wrote {out_path} ({len(enriched)} rows)")

if __name__ == "__main__":
    main()
