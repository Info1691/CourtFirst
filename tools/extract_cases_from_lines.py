#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable, List, Dict, Any, Tuple

CASE_COLS = ["case_id", "title", "citation", "jurisdiction", "url", "source_line"]

CITATION_RE = re.compile(r"\[[0-9]{4}[^]]*\]")  # captures things like [1999] ... , [2014] (1) ...
TRAILING_PAGES_RE = re.compile(r"[,;]\s*(pp?\.\s*\d+|\d+(?:-\d+)?)\s*$", re.IGNORECASE)

def load_lines(path: Path) -> List[Dict[str, Any]]:
    """
    Load LTJ-ui/out/LTJ.lines.json.
    Accepts either:
      - a JSON array of objects with keys like {"line": 1276, "text": "..."}
      - an object containing {"lines": [ ... ] }
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        sys.exit(f"ERROR: LTJ lines file not found: {path}")
    except json.JSONDecodeError as e:
        sys.exit(f"ERROR: Cannot parse JSON in {path}: {e}")

    if isinstance(data, dict) and "lines" in data:
        data = data["lines"]

    if not isinstance(data, list):
        sys.exit("ERROR: Unexpected format in LTJ.lines.json (expected list or object with 'lines').")

    # Normalize: ensure each item has "line" (int) and "text" (str)
    norm = []
    for item in data:
        if isinstance(item, dict):
            line_no = item.get("line") or item.get("line_no") or item.get("lineno")
            text = item.get("text") or item.get("content") or item.get("line_text")
            if isinstance(line_no, int) and isinstance(text, str):
                norm.append({"line": line_no, "text": text})
    if not norm:
        sys.exit("ERROR: No usable line objects found in LTJ.lines.json.")
    return norm

def slice_lines(lines: List[Dict[str, Any]], start: int, end: int) -> List[Dict[str, Any]]:
    return [row for row in lines if start <= int(row["line"]) <= end]

def parse_case_row(text: str, line_no: int) -> Dict[str, str]:
    """
    Minimal, safe parsing:
      - title: full line text with trailing plain page refs trimmed
      - citation: first [YYYY ...] chunk if present; else ""
      - jurisdiction, case_id, url left blank (we will enrich later)
    We avoid guessing to keep data factual.
    """
    citation = ""
    m = CITATION_RE.search(text)
    if m:
        citation = m.group(0)

    # Strip trailing obvious page hints like ", 2418-84" or "; 54-55" etc
    cleaned = TRAILING_PAGES_RE.sub("", text).strip()

    return {
        "case_id": "",          # to be filled later by resolvers
        "title": cleaned,
        "citation": citation,
        "jurisdiction": "",     # do not guess
        "url": "",              # to be populated by enrichers later
        "source_line": str(line_no),
    }

def read_existing_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            # keep only known cols; fill missing as empty
            fixed = {k: row.get(k, "") for k in CASE_COLS}
            rows.append(fixed)
    return rows

def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CASE_COLS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CASE_COLS})

def merge_rows(existing: List[Dict[str, str]], new_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Merge on (title, citation) as a stable key; prefer existing non-empty fields.
    """
    key = lambda r: (r.get("title","").strip(), r.get("citation","").strip())
    index: Dict[Tuple[str,str], Dict[str,str]] = {key(r): r for r in existing}
    for r in new_rows:
        k = key(r)
        if k in index:
            base = index[k]
            # fill blanks only
            for col in CASE_COLS:
                if not base.get(col) and r.get(col):
                    base[col] = r[col]
        else:
            index[k] = r
    # return in a stable order by title
    return sorted(index.values(), key=lambda r: (r.get("title",""), r.get("citation","")))

def main():
    ap = argparse.ArgumentParser(description="Extract LTJ case lines into data/cases.csv")
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--start", type=int, required=True, help="Start line (inclusive)")
    ap.add_argument("--end", type=int, required=True, help="End line (inclusive)")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g., data/cases.csv)")
    ap.add_argument("--merge", action="store_true", help="Merge into existing CSV instead of overwriting")
    args = ap.parse_args()

    ltj_path = Path(args.ltj_lines)
    out_path = Path(args.out)

    lines = load_lines(ltj_path)
    sliced = slice_lines(lines, args.start, args.end)

    new_rows = [parse_case_row(item["text"], item["line"]) for item in sliced]

    if args.merge:
        existing = read_existing_csv(out_path)
        merged = merge_rows(existing, new_rows)
        write_csv(out_path, merged)
    else:
        write_csv(out_path, new_rows)

    print(f"Wrote {len(new_rows)} rows to {out_path} "
          f"({'merged' if args.merge else 'fresh'}).")

if __name__ == "__main__":
    main()
