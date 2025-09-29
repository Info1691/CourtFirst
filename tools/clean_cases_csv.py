#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, re
from pathlib import Path

IN = Path("data/cases.csv")
OUT = IN

CASE_COLS = ["case_id", "title", "citation", "jurisdiction", "url", "source_line"]
NUMERIC_INDEX_RE = re.compile(r"^\s*(\d+(?:\s*-\s*\d+)?)(\s*,\s*\d+(?:\s*-\s*\d+)?)*\s*$")
TRAILING_PAGES_RE = re.compile(r"[,;]\s*(?:pp?\.\s*)?\d+(?:-\d+)?(?:\s*,\s*\d+(?:-\d+)?)*\s*$", re.IGNORECASE)

def keep(title: str) -> bool:
    if NUMERIC_INDEX_RE.match(title.strip()): return False
    if not re.search(r"[A-Za-z]", title):     return False
    return True

def main():
    if not IN.exists():
        print("data/cases.csv not found.")
        return
    rows = []
    with IN.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            t = (row.get("title") or "").strip()
            if not t or not keep(t): 
                continue
            t = TRAILING_PAGES_RE.sub("", t).rstrip(" ,;").strip()
            row["title"] = t
            rows.append({c: row.get(c, "") for c in CASE_COLS})
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CASE_COLS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Cleaned {len(rows)} rows -> {OUT}")

if __name__ == "__main__":
    main()
