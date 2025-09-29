#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, json, re, sys
from pathlib import Path
from typing import Dict, List, Any, Tuple

CASE_COLS = ["case_id", "title", "citation", "jurisdiction", "url", "source_line"]

# Signals a *real* case line
CITATION_RE = re.compile(r"\[[0-9]{4}[^]]*\]")  # e.g. [2014] EWCA Civ 123
NUMERIC_INDEX_RE = re.compile(r"^\s*(\d+(?:\s*-\s*\d+)?)(\s*,\s*\d+(?:\s*-\s*\d+)?)*\s*$")
TRAILING_PAGES_RE = re.compile(r"[,;]\s*(?:pp?\.\s*)?\d+(?:-\d+)?(?:\s*,\s*\d+(?:-\d+)?)*\s*$", re.IGNORECASE)

CASE_HINTS = (
    " v ", " v. ", " in re ", " In re ", " re ",
    "JLR", "JRC", "EWHC", "EWCA", "UKSC", "UKPC", "WLR", "All ER",
    "Court", "Tribunal", "Ch", "QB", "CA", "PC",
    " Ltd", " plc", " LLP", " Inc", " Company", " Trust", " Trustee",
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
        sys.exit("ERROR: Unexpected LTJ.lines.json structure.")

    out = []
    for it in data:
        if not isinstance(it, dict): 
            continue
        line_no = it.get("line") or it.get("line_no") or it.get("lineno")
        text = it.get("text") or it.get("content") or it.get("line_text")
        if isinstance(line_no, int) and isinstance(text, str):
            out.append({"line": line_no, "text": text})
    if not out:
        sys.exit("ERROR: No usable line objects found.")
    return out

def looks_like_case(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    # reject pure numeric/page-range lists
    if NUMERIC_INDEX_RE.match(s):
        return False
    # must contain at least one letter
    if not re.search(r"[A-Za-z]", s):
        return False
    # accept if citation present
    if CITATION_RE.search(s):
        return True
    # otherwise require a case-ish cue
    padded = f" {s} "
    return any(hint in padded for hint in CASE_HINTS)

def parse_case(text: str, line_no: int) -> Dict[str, str]:
    m = CITATION_RE.search(text)
    citation = m.group(0) if m else ""
    title = TRAILING_PAGES_RE.sub("", text).rstrip(" ,;").strip()
    return {
        "case_id": "",
        "title": title,
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
            rows.append({c: row.get(c, "") for c in CASE_COLS})
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
    idx = {key(r): r for r in existing}
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
    ap = argparse.ArgumentParser(description="Extract case entries from LTJ lines (filters index/page rows).")
    ap.add_argument("--ltj-lines", required=True)
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--out", required=True)                   # e.g. data/cases.csv
    ap.add_argument("--merge", action="store_true")
    ap.add_argument("--report", default="out/extract_report.json")
    args = ap.parse_args()

    lines = load_lines(Path(args.ltj_lines))
    window = slice_lines(lines, args.start, args.end)

    kept, dropped = [], []
    for row in window:
        txt = row["text"]
        if looks_like_case(txt):
            kept.append(parse_case(txt, row["line"]))
        else:
            dropped.append({"line": row["line"], "text": txt})

    out_path = Path(args.out)
    existing = read_csv(out_path) if args.merge else []
    final = merge(existing, kept)

    write_csv(out_path, final)

    # Write a small report so you can audit exactly what was filtered
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        json.dump({
            "range": {"start": args.start, "end": args.end},
            "sliced": len(window),
            "kept": len(kept),
            "dropped": len(dropped),
            "sample_dropped": dropped[:20],  # small sample
            "output_csv": str(out_path)
        }, f, ensure_ascii=False, indent=2)

    print(f"[extract] sliced={len(window)} kept={len(kept)} dropped={len(dropped)} -> {out_path}")
    print(f"[extract] report -> {report_path}")

if __name__ == "__main__":
    main()
