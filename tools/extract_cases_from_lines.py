#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Output schema
CASE_COLS = ["case_id", "title", "citation", "jurisdiction", "url", "source_line"]

# Heuristics
CITATION_RE = re.compile(r"\[[0-9]{4}[^]]*\]")   # e.g. [2010] EWHC ...
# obvious trailing page snippets like ", 2418-84" / "; 54-55" / ", 1-12, 13-14"
TRAILING_PAGES_RE = re.compile(r"[,;]\s*(?:pp?\.\s*)?\d+(?:-\d+)?(?:\s*,\s*\d+(?:-\d+)?)*\s*$", re.IGNORECASE)

# Looks like a pure numeric/index line? (no letters, only digits/commas/hyphens)
NUMERIC_INDEX_RE = re.compile(r"^\s*(\d+(?:\s*-\s*\d+)?)(\s*,\s*\d+(?:\s*-\s*\d+)?)*\s*$")

# Require at least one of these "case-ish" hints if present
CASE_HINTS = (
    " v ",            # Versus pattern
    " v. ",           # Sometimes with a dot
    " in re ",        # In re
    " In re ",        # Capitalized
    " re ",           # re Something
    "JLR", "JRC",     # Jersey reports/citations in our corpus
    "EWHC", "EWCA", "UKSC", "UKPC", "WLR", "All ER",
    "Court", "Tribunal",
    "Ltd", "plc", "LLP", "Inc", "Company", "Trust", "Trustee",
)

def load_lines(path: Path) -> List[Dict[str, Any]]:
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
        sys.exit("ERROR: Unexpected LTJ.lines.json structure (need list or {'lines': [...]})")

    out = []
    for item in data:
        if isinstance(item, dict):
            line_no = item.get("line") or item.get("line_no") or item.get("lineno")
            text = item.get("text") or item.get("content") or item.get("line_text")
            if isinstance(line_no, int) and isinstance(text, str):
                out.append({"line": line_no, "text": text})
    if not out:
        sys.exit("ERROR: No usable line objects found.")
    return out

def is_probable_case(raw: str) -> bool:
    """
    Filter out index-only numeric rows, keep lines that look like an actual case entry.
    Rules:
      - Must contain at least one alphabetic letter.
      - Must NOT match a pure numeric/index list like '10-21, 10-28'.
      - Prefer lines with a citation [YYYY ...] OR containing any 'case-ish' cue.
    """
    s = raw.strip()
    if not s:
        return False

    # Pure numeric/index page lists?
    if NUMERIC_INDEX_RE.match(s):
        return False

    # Needs at least one alphabetic character
    if not re.search(r"[A-Za-z]", s):
        return False

    # If it has a bracketed year, accept.
    if CITATION_RE.search(s):
        return True

    # Otherwise require one of our cues
    lower = " " + s + " "   # pad to match " v " safely
    for hint in CASE_HINTS:
        if hint in lower:
            return True

    # Otherwise, too ambiguous—skip
    return False

def parse_case_row(text: str, line_no: int) -> Dict[str, str]:
    # Extract citation if present
    m = CITATION_RE.search(text)
    citation = m.group(0) if m else ""

    # Strip trailing page-like tails
    cleaned = TRAILING_PAGES_RE.sub("", text).rstrip(" ,;").strip()

    return {
        "case_id": "",
        "title": cleaned,
        "citation": citation,
        "jurisdiction": "",
        "url": "",
        "source_line": str(line_no),
    }

def slice_lines(lines: List[Dict[str, Any]], start: int, end: int) -> List[Dict[str, Any]]:
    return [r for r in lines if start <= int(r["line"]) <= end]

def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({col: row.get(col, "") for col in CASE_COLS})
    return rows

def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CASE_COLS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in CASE_COLS})

def merge(existing: List[Dict[str, str]], new_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    key = lambda r: (r.get("title","").strip(), r.get("citation","").strip())
    idx: Dict[Tuple[str,str], Dict[str,str]] = {key(r): r for r in existing}
    for r in new_rows:
        k = key(r)
        if k in idx:
            base = idx[k]
            for col in CASE_COLS:
                if not base.get(col) and r.get(col):
                    base[col] = r[col]
        else:
            idx[k] = r
    return sorted(idx.values(), key=lambda r: (r.get("title",""), r.get("citation","")))

def main():
    ap = argparse.ArgumentParser(description="Extract case-like lines from LTJ.lines.json")
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--start", type=int, required=True, help="Start line (inclusive)")
    ap.add_argument("--end", type=int, required=True, help="End line (inclusive)")
    ap.add_argument("--out", required=True, help="Output CSV (e.g., data/cases.csv)")
    ap.add_argument("--merge", action="store_true", help="Merge into existing CSV instead of overwrite")
    args = ap.parse_args()

    lines = load_lines(Path(args.ltj_lines))
    sliced = slice_lines(lines, args.start, args.end)

    # Filter + parse
    filtered = [row for row in sliced if is_probable_case(row["text"])]
    new_rows = [parse_case_row(row["text"], row["line"]) for row in filtered]

    out_path = Path(args.out)
    if args.merge:
        existing = read_csv(out_path)
        final = merge(existing, new_rows)
    else:
        final = new_rows

    write_csv(out_path, final)
    print(f"Kept {len(new_rows)} case-like rows out of {len(sliced)} lines. Wrote {len(final)} total rows to {out_path}.")

if __name__ == "__main__":
    main()
