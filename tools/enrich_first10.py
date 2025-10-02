#!/usr/bin/env python3
import csv
import json
import time
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

# --------------------------
# CLI ARGUMENTS
# --------------------------
parser = argparse.ArgumentParser(description="Enrich first N cases with URLs")
parser.add_argument("--input", required=True, help="Path to cases.csv input")
parser.add_argument("--outdir", required=True, help="Output directory path")
parser.add_argument("--limit", type=int, default=10, help="How many rows to process")
parser.add_argument("--start", type=int, default=0, help="Start row index")
parser.add_argument("--sleep-min", type=float, default=2.0)
parser.add_argument("--sleep-max", type=float, default=3.5)
parser.add_argument("--max-consec-fail", type=int, default=8)
args = parser.parse_args()

# --------------------------
# BASIC SETTINGS
# --------------------------
input_file = args.input
out_dir = args.outdir
limit = args.limit
start = args.start
sleep_min = args.sleep_min
sleep_max = args.sleep_max
max_fails = args.max_consec_fail

cases_out = f"{out_dir}/cases_preview.csv"
urls_out = f"{out_dir}/urls_preview.json"
skipped_out = f"{out_dir}/skipped_preview.json"

# --------------------------
# READ INPUT CASES
# --------------------------
with open(input_file, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    cases = list(reader)

targets = cases[start:start + limit]
print(f"Processing {len(targets)} cases starting from row {start}")

# --------------------------
# HELPER: Fetch primary URL
# --------------------------
def find_primary_url(title, citation):
    query = quote(f"{title} {citation}".strip())
    bailii_search = f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={query}"
    jlaw_search = f"https://www.jerseylaw.je/search/Pages/Results.aspx?k={query}"
    # Try Bailii first
    try:
        r = requests.get(bailii_search, timeout=10)
        if r.status_code == 200 and "cgi-bin" in r.url:
            return r.url
    except Exception:
        pass
    # Fallback Jersey Law
    try:
        r = requests.get(jlaw_search, timeout=10)
        if r.status_code == 200:
            return r.url
    except Exception:
        pass
    return bailii_search

# --------------------------
# PROCESS LOOP
# --------------------------
results = []
skipped = []
fails = 0

for idx, case in enumerate(targets, start=1):
    title = case["Title"]
    citation = case.get("Citation", "")
    print(f"[{idx}/{len(targets)}] Processing: {title}")

    try:
        url = find_primary_url(title, citation)
        results.append({"title": title, "citation": citation, "url": url})
        fails = 0
    except Exception as e:
        print(f"⚠️ Failed: {title} - {e}")
        skipped.append({"title": title, "error": str(e)})
        fails += 1
        if fails >= max_fails:
            print("❌ Too many consecutive failures, aborting.")
            break

    # Heartbeat sleep
    time.sleep(sleep_min)

# --------------------------
# WRITE OUTPUT FILES
# --------------------------
with open(cases_out, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["Title", "Citation", "Url"])
    writer.writeheader()
    for r in results:
        writer.writerow(r)

with open(urls_out, "w", encoding='utf-8') as f:
    json.dump(results, f, indent=2)

with open(skipped_out, "w", encoding='utf-8') as f:
    json.dump(skipped, f, indent=2)

print("✅ Enrichment complete!")
print(f"📁 Results: {cases_out}, {urls_out}, {skipped_out}")
