#!/usr/bin/env python3
import argparse, csv, re, time, random
from duckduckgo_search import DDGS
import urllib.parse as ul

JERSEYLAW_ROOT = "https://www.jerseylaw.je"
BAILII_ROOT = "https://www.bailii.org"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    return ap.parse_args()

def heartbeat(n_done, total):
    if n_done % 50 == 0 or n_done == total:
        rate = f"{n_done}/{total} (~{(n_done/total)*100:.1f}%)"
        print(f"[enrich] {rate}")

def try_jerseylaw(title, year):
    # very light heuristic: if looks like "JRC 123" or "JCA 123"
    if "JRC" in title or "JCA" in title:
        q = f'site:jerseylaw.je "{title}"'
        return ddg_first(q)
    return ""

def try_bailii(title, year):
    q = f'site:bailii.org "{title}"'
    u = ddg_first(q)
    if u: return u
    # fallback: add year if we have one
    if year:
        return ddg_first(f'site:bailii.org "{title}" {year}')
    return ""

def ddg_first(query):
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=3):
            url = r.get("href") or r.get("url")
            if url: return url
    return ""

def polite_sleep():
    time.sleep(0.4 + random.random()*0.3)

def main():
    a = parse_args()
    rows = []
    with open(a.input, newline="", encoding="utf-8") as f:
        r = list(csv.DictReader(f))
    total = len(r)
    for i, row in enumerate(r, start=1):
        title = row["Title"]
        year = row.get("Year","").strip()
        url = ""

        # JerseyLaw first
        url = try_jerseylaw(title, year)
        polite_sleep()
        if not url:
            # BAILII then
            url = try_bailii(title, year)
            polite_sleep()
        if not url:
            # broad duckduckgo
            q = f'"{title}" {year}' if year else f'"{title}"'
            url = ddg_first(q)
            polite_sleep()

        row_out = {
            "Title": title,
            "Year": year,
            "Citation": row.get("Citation",""),
            "Jurisdiction": "Jersey" if "JRC" in title or "JCA" in title else "",
            "URL": url
        }
        rows.append(row_out)
        heartbeat(i, total)

    # write back into the main spreadsheet the UI uses
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Title","Year","Citation","Jurisdiction","URL"])
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    main()
