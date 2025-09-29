#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Minimal extractor:
- Input:  LTJ-ui/out/LTJ.lines.json (or LTK.lines.json)
- Output: data/cases.csv  with columns: line_no, raw, title
'raw'  = original line text
'title' = 'raw' stripped of trailing page ranges like "…, 12-23, 45-51"
If a proper case citation like "[2010] EWHC 123" is present, we keep up to the
end of that citation, then trim any trailing comma/range noise.
"""

import argparse, json, csv, re
from pathlib import Path

# detects a law-style citation like [2014] EWCA Civ 123, [1996] 1 AC 123, etc.
CITATION = re.compile(r"\[[0-9]{4}[^\]]*\]")

# trims trailing index/page blobs like ", 1-2, 3-4" or "; 12-23"
TRAILING_RANGES = re.compile(r"[,;]\s*(pp?\.\s*)?\d+(?:-\d+)?(?:\s*,\s*\d+(?:-\d+)?)*\s*$", re.IGNORECASE)

def load_lines(path: Path):
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # accept either {"lines":[...]} or raw list
    if isinstance(data, dict) and "lines" in data:
        data = data["lines"]
    return [
        {"line_no": int(item.get("line") or item.get("line_no") or item.get("lineno")),
         "text": (item.get("text") or item.get("content") or item.get("line_text") or "").strip()}
        for item in data
        if isinstance(item, dict)
    ]

def to_title(raw: str) -> str:
    if not raw:
        return ""
    # If a citation exists, keep up to the end of citation
    m = CITATION.search(raw)
    title = raw[: m.end()].strip() if m else raw
    # Remove trailing page/index ranges
    title = TRAILING_RANGES.sub("", title).rstrip(" ,;").strip()
    return title

def main():
    ap = argparse.ArgumentParser(description="Extract clean case titles (column B) from LTJ lines JSON.")
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json (or LTK.lines.json)")
    ap.add_argument("--out", default="data/cases.csv", help="Output CSV path")
    ap.add_argument("--start", type=int, default=None, help="Start line_no (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End line_no (inclusive)")
    args = ap.parse_args()

    src = Path(args.ltj_lines)
    rows = load_lines(src)

    # Optional slicing by line_no
    if args.start is not None:
        rows = [r for r in rows if r["line_no"] >= args.start]
    if args.end is not None:
        rows = [r for r in rows if r["line_no"] <= args.end]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["line_no", "raw", "title"])
        for r in rows:
            raw = r["text"]
            title = to_title(raw)
            # Skip blank/obvious junk (pure index numbers)
            if not title or title.isdigit():
                continue
            w.writerow([r["line_no"], raw, title])

    print(f"✓ Wrote {out_path} ({sum(1 for _ in out_path.open())-1} rows)")

if __name__ == "__main__":
    main()
