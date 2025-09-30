#!/usr/bin/env python3
import csv, sys, time, random, urllib.parse as up
from pathlib import Path
from bs4 import BeautifulSoup
import urllib.request as ur

PREFERRED = ("jerseylaw.je", "bailii.org", "bailii.org.uk")
ACCEPTABLE = ("casemine.com", "vlex.co.uk", "lawtel", "westlaw")  # we won't fabricate; we only save what we fetch

DDG_HTML = "https://duckduckgo.com/html/?q="

def http_get(url, timeout=15):
    req = ur.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; CourtFirst/1.0)"
    })
    with ur.urlopen(req, timeout=timeout) as r:
        return r.read()

def choose_url(links):
    # Prefer first JerseyLaw, then BAILII, then acceptable fallbacks
    for host in PREFERRED:
        for u in links:
            if host in u:
                return u
    for host in ACCEPTABLE:
        for u in links:
            if host in u:
                return u
    return links[0] if links else ""

def search_urls(title, citation):
    q = " ".join(x for x in [title, citation] if x).strip()
    if not q:
        return ""
    url = DDG_HTML + up.quote(q)
    try:
        html = http_get(url)
    except Exception:
        return ""

    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a.result__a, a.result__url, a[href]"):
        href = a.get("href","")
        if href.startswith("http"):
            links.append(href)
    links = [u for u in links if "duckduckgo" not in u and "adclick" not in u]
    pick = choose_url(links)
    return pick

def main():
    if len(sys.argv) != 2:
        print("Usage: enrich_urls.py data/cases.csv", file=sys.stderr)
        sys.exit(2)

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)

    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    changed = 0
    for r in rows:
        if r.get("URL"):
            continue
        title = (r.get("Title") or "").strip()
        citation = (r.get("Citation") or "").strip()
        if not title:
            continue
        url = search_urls(title, citation)
        if url:
            r["URL"] = url
            changed += 1
        time.sleep(0.8 + random.random()*0.7)  # polite

    if changed:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Title","Year","Citation","Jurisdiction","Line","URL"])
            w.writeheader()
            for r in rows:
                w.writerow(r)

    print(f"Updated {changed} URL(s).")

if __name__ == "__main__":
    main()
