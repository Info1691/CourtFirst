#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enrich the first N (default 10) cases in data/cases.csv with source URLs.
- Providers (in order): JerseyLaw → BAILII → DuckDuckGo (HTML endpoint)
- Heartbeat/progress every case (prints rate, successes, skips)
- Polite throttling (default 2.0–3.5s between provider requests)
- Fail-fast: abort after --max-consec-fail consecutive provider failures
- Outputs (preview only; never overwrites your main CSV):
    out/preview-enrichment/cases_preview.csv
    out/preview-enrichment/urls_preview.json
    out/preview-enrichment/skipped_preview.json

CSV columns expected (header names, case-insensitive):
    Title, Year, Citation, Jurisdiction, Line
Only Title and Line are strictly required.

Run locally:
    python tools/enrich_first10.py --limit 10

In CI (workflow below) this is called with safe defaults.
"""

import csv
import json
import os
import re
import time
import random
import argparse
from html import unescape
from urllib.parse import urlencode, quote_plus

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 25

# Provider endpoints (GET)
JERSEYLAW_SEARCH = "https://www.jerseylaw.je/search/Pages/Results.aspx"
BAILII_SEARCH = "https://www.bailii.org/cgi-bin/sino_search_1.cgi"
DDG_HTML = "https://duckduckgo.com/html/"

# Acceptable result domains
ALLOW_DOMAINS = (
    "jerseylaw.je",
    "bailii.org",
    "casemine.com",
    "lawtel.thomsonreuters.co.uk",
    "vlex.co.uk",
)

# -------- Utilities --------
def sleep_between(min_s: float, max_s: float):
    time.sleep(random.uniform(min_s, max_s))

def read_cases_csv(path: str):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # Normalize keys (case-insensitive)
    norm_rows = []
    for r in rows:
        norm = {k.strip().lower(): (r[k] or "").strip() for k in r}
        norm_rows.append(norm)
    return norm_rows

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def tokens(s: str):
    s = re.sub(r"[\s\u00A0]+", " ", s or "").strip()
    s = re.sub(r"[\u2018\u2019]", "'", s)  # curly quotes
    return re.split(r"[\s,;:]+", s.lower())

def score_title_match(query_title: str, candidate_text: str) -> float:
    """Very light similarity: token overlap / query tokens."""
    q = set(t for t in tokens(query_title) if t not in {"v", "re"})
    c = set(tokens(candidate_text))
    if not q:
        return 0.0
    return len(q & c) / float(len(q))

def best_link(links, query_title):
    """Pick the best-looking link by (domain priority, title score, length)."""
    scored = []
    for href, text in links:
        domain_score = (
            3 if "jerseylaw.je" in href
            else 2 if "bailii.org" in href
            else 1 if any(d in href for d in ALLOW_DOMAINS)
            else 0
        )
        tscore = score_title_match(query_title, text or href)
        scored.append((domain_score, tscore, -len(href), href, text))
    scored.sort(reverse=True)
    return scored[0][3] if scored else None

def fetch(url, params=None):
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return r

def parse_links_from_html(html, base_filter=None):
    soup = BeautifulSoup(html, "lxml")
    out = []
    for a in soup.find_all("a", href=True):
        href = unescape(a["href"]).strip()
        text = a.get_text(" ", strip=True)
        if base_filter and not base_filter(href):
            continue
        out.append((href, text))
    return out

# -------- Providers --------
def search_jerseylaw(title, year):
    # https://www.jerseylaw.je/search/Pages/Results.aspx?k=...
    q = f'"{title}" {year or ""}'.strip()
    params = {"k": q}
    r = fetch(JERSEYLAW_SEARCH, params=params)
    # Limit to judgments pages and PDFs
    def filt(href):
        return "jerseylaw.je" in href and (
            "/judgments/" in href or href.lower().endswith(".pdf") or "/lawreports/" in href
        )
    links = parse_links_from_html(r.text, filt)
    return links

def search_bailii(title, year):
    # https://www.bailii.org/cgi-bin/sino_search_1.cgi?query=...
    q = f'"{title}" {year or ""}'.strip()
    params = {
        "query": q,
        "method": "boolean",
        "highlight": 1,
        "sort": "relevance",
    }
    r = fetch(BAILII_SEARCH, params=params)
    def filt(href):
        return "bailii.org" in href
    links = parse_links_from_html(r.text, filt)
    return links

def search_ddg(title, year):
    # Light HTML endpoint, not the JS app
    q = f'"{title}" {year or ""}'.strip()
    params = {"q": q}
    r = fetch(DDG_HTML, params=params)
    def filt(href):
        return any(d in href for d in ALLOW_DOMAINS)
    links = parse_links_from_html(r.text, filt)
    return links

def resolve_url_for_case(title, year, min_sleep, max_sleep):
    """Try providers in order; return best single URL or None."""
    providers = [
        ("jerseylaw", search_jerseylaw),
        ("bailii", search_bailii),
        ("ddg", search_ddg),
    ]
    all_links = []
    for name, fn in providers:
        try:
            links = fn(title, year)
            # Polite delay *between* providers
            sleep_between(min_sleep, max_sleep)
        except requests.HTTPError as e:
            # 429 / 5xx: bubble up; caller may fail-fast
            raise
        except Exception:
            links = []
        all_links.extend(links)
        # If we already have strong links from a high-priority provider, break
        if name in {"jerseylaw", "bailii"} and links:
            break
    return best_link(all_links, title)

# -------- Main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/cases.csv")
    ap.add_argument("--outdir", default="out/preview-enrichment")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--sleep-min", type=float, default=2.0)
    ap.add_argument("--sleep-max", type=float, default=3.5)
    ap.add_argument("--max-consec-fail", type=int, default=8)
    args = ap.parse_args()

    ensure_dir(args.outdir)
    rows = read_cases_csv(args.input)
    # Figure out header names present
    def get(row, name):
        return row.get(name.lower(), "")

    # Take first N with a non-empty Title
    work = []
    for r in rows:
        title = get(r, "Title") or get(r, "title")
        if title:
            work.append({
                "title": title,
                "year": (get(r, "Year") or get(r, "year")),
                "citation": (get(r, "Citation") or get(r, "citation")),
                "jurisdiction": (get(r, "Jurisdiction") or get(r, "jurisdiction")),
                "line": (get(r, "Line") or get(r, "line")),
            })
        if len(work) >= args.limit:
            break

    urls_preview = {}
    skipped = {}
    out_csv_path = os.path.join(args.outdir, "cases_preview.csv")
    consec_fail = 0
    t0 = time.time()
    success = 0
    total = len(work)

    # Write CSV header
    with open(out_csv_path, "w", newline="", encoding="utf-8") as fout:
        w = csv.writer(fout)
        w.writerow(["Title", "Citation", "Year", "Line", "url"])

        for idx, c in enumerate(work, start=1):
            title, year, cit, line = c["title"], c["year"], c["citation"], c["line"]
            try:
                url = resolve_url_for_case(title, year, args.sleep_min, args.sleep_max)
                if url:
                    success += 1
                    consec_fail = 0
                    urls_preview[str(idx)] = {
                        "title": title, "year": year, "citation": cit, "line": line, "url": url
                    }
                    w.writerow([title, cit, year, line, url])
                else:
                    consec_fail += 1
                    skipped[str(idx)] = {"title": title, "year": year, "citation": cit, "line": line,
                                         "reason": "no-verified-match"}
            except requests.HTTPError as e:
                consec_fail += 1
                skipped[str(idx)] = {"title": title, "year": year, "citation": cit, "line": line,
                                     "reason": f"http-error:{e.response.status_code}"}
            except Exception as e:
                consec_fail += 1
                skipped[str(idx)] = {"title": title, "year": year, "citation": cit, "line": line,
                                     "reason": f"exception:{type(e).__name__}"}

            # Heartbeat
            elapsed = time.time() - t0
            rate = idx / max(1.0, elapsed)
            print(f"[{time.strftime('%H:%M:%S')}] case {idx}/{total} | "
                  f"ok:{success} skip:{len(skipped)} | {rate:.2f} cases/s | title='{title[:70]}'")

            # Fail-fast if clearly going wrong
            if consec_fail >= args.max_consec_fail:
                print(f"!! aborting: {consec_fail} consecutive failures (max {args.max_consec_fail})")
                break

    # Save previews
    with open(os.path.join(args.outdir, "urls_preview.json"), "w", encoding="utf-8") as f:
        json.dump(urls_preview, f, ensure_ascii=False, indent=2)
    with open(os.path.join(args.outdir, "skipped_preview.json"), "w", encoding="utf-8") as f:
        json.dump(skipped, f, ensure_ascii=False, indent=2)

    # Final summary line
    print(f"Done. Success={success} Skipped={len(skipped)} "
          f"Elapsed={time.time()-t0:.1f}s")
    # Non-zero exit if zero success (useful to catch total failure early)
    if success == 0:
        raise SystemExit(2)

if __name__ == "__main__":
    main()
