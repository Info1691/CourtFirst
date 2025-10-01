#!/usr/bin/env python3
import argparse, json, re, csv

CASE_LINE = re.compile(r'"text"\s*:\s*"(?P<title>.+?)"\s*}\s*[,}]')
YEAR = re.compile(r'\[(\d{4})\]')
CITE = re.compile(r'(\[[0-9]{4}\].+?|[A-Z]{2,4}\s?[0-9]{3,5}.+?)$')

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--end", type=int, default=10**9)
    return ap.parse_args()

def main():
    a = parse_args()
    with open(a.lines, "r", encoding="utf-8") as f:
        lines = f.readlines()

    rows = []
    for i, line in enumerate(lines, start=1):
        if i < a.start or i > a.end: continue
        m = CASE_LINE.search(line)
        if not m: continue
        title_raw = m.group("title").strip()
        year = ""
        y = YEAR.search(title_raw)
        if y: year = y.group(1)
        citation = ""
        c = CITE.search(title_raw)
        if c: citation = c.group(1).strip().rstrip('",')
        rows.append({
            "Title": title_raw,
            "Year": year,
            "Citation": citation
        })

    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Title","Year","Citation"])
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    main()
