#!/usr/bin/env python3
import argparse, csv, re

PAGE_TRAILER = re.compile(r'(,\s*\d+(?:-\d+)?(?:,\s*\d+(?:-\d+)?)*)$')
EXTRA_SPACE = re.compile(r'\s{2,}')

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    return ap.parse_args()

def clean_title(t: str) -> str:
    # drop trailing page ranges like ", 12-23, 45-50"
    t = PAGE_TRAILER.sub("", t)
    # compact spaces
    t = EXTRA_SPACE.sub(" ", t).strip().rstrip(",")
    return t

def main():
    a = parse_args()
    rows = []
    with open(a.input, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            row["Title"] = clean_title(row["Title"])
            rows.append(row)
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Title","Year","Citation"])
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    main()
