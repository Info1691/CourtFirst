#!/usr/bin/env python3
import argparse, csv, json, time, html
from pathlib import Path
from urllib.parse import quote_plus
import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64)"}
TIMEOUT = 15

def jerseylaw_search_urls(title, citation, year):
    q = quote_plus(f'{title} {year or ""} {citation or ""}'.strip())
    return [
        f"https://www.jerseylaw.je/search/Pages/Results.aspx?k={q}",
        f"https://www.jerseylaw.je/search/Pages/Results.aspx?k={quote_plus(title)}"
    ]

def bailii_search_urls(title, citation, year):
    # Bailii’s search is basic; use DuckDuckGo site filter to find BAILII pages.
    q1 = quote_plus(f'site:bailii.org "{title}" {year or ""}')
    q2 = quote_plus(f'site:bailii.org "{title}"')
    return [
        f"https://duckduckgo.com/html/?q={q1}",
        f"https://duckduckgo.com/html/?q={q2}",
    ]

def ddg_search_urls(title, citation, year):
    q = quote_plus(f'"{title}" {year or ""} {citation or ""}')
    return [f"https://duckduckgo.com/html/?q={q}"]

def first_href_from_ddg(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    for a in soup.select("a.result__a"):
        href = a.get("href")
        if href:
            return href
    # Fallback older markup
    a = soup.find("a", class_="result__a")
    return a.get("href") if a else None

def try_get(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and r.text:
            return r
    except requests.RequestException:
        return None
    return None

def resolve_url(title, citation, year, jurisdiction):
    # 1) If Jersey likely, try JerseyLaw search page, then follow first result if simple
    for url in jerseylaw_search_urls(title, citation, year):
        r = try_get(url)
        if not r: 
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        hit = soup.select_one("a.ms-srch-item-link, a[href*='/judgments/']")
        if hit:
            href = hit.get("href")
            if href and href.startswith("/"):
                return "https://www.jerseylaw.je" + href
            if href and href.startswith("http"):
                return href

    # 2) Bailii via DDG site filtered
    for url in bailii_search_urls(title, citation, year):
        r = try_get(url)
        if not r:
            continue
        href = first_href_from_ddg(r.text)
        if href and "bailii.org" in href:
            return href

    # 3) General DDG
    for url in ddg_search_urls(title, citation, year):
        r = try_get(url)
        if not r:
            continue
        href = first_href_from_ddg(r.text)
        if href and href.startswith("http"):
            return href

    return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--limit", type=int, default=0, help="limit rows for a run (0 = all)")
    args = ap.parse_args()

    rows = []
    with open(args.inp, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)

    enriched = []
    found = 0
    tried = 0
    for i, row in enumerate(rows, 1):
        if args.limit and i > args.limit:
            enriched.extend(rows[i-1:])
            break
        tried += 1
        title = row.get("title","")
        citation = row.get("citation","")
        year = row.get("year","")
        juris = row.get("jurisdiction","")
        url = resolve_url(title, citation, year, juris)
        if url:
            found += 1
            row["url"] = url
        enriched.append(row)
        time.sleep(0.6)  # be kind

    # Write output
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=enriched[0].keys())
        w.writeheader()
        w.writerows(enriched)

    # Report
    report = {
        "input": args.inp,
        "output": args.out,
        "rows": len(rows),
        "tried": tried,
        "found": found
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
