#!/usr/bin/env python3
"""
Rebuild (or verify) the cases list from LTJ-ui/out/LTJ.lines.json
for a given line range, with ZERO silent drops.

Outputs:
- data/cases_from_ltj.csv  (canonical rows)
- out/rebuild_report.json  (counts & sample)
- out/rebuild_missing.csv  (if anything odd happens)

Usage:
  python tools/rebuild_cases_from_ltj_lines.py \
      --ltj-lines LTJ-ui/out/LTJ.lines.json \
      --start 1276 --end 3083 \
      --out data/cases_from_ltj.csv
"""

import argparse, csv, json, re
from pathlib import Path

CASE_ROW_COLS = ["case_id","Title","Year","Citation","Jurisdiction","Line"]

# very broad reporter tokens; extend as needed
REPORTER_TOKENS = r"(JLR|JRC|EWHC|EWCA|UKPC|AC|WLR|All ER|JCA|PC|JCPC)"
YEAR = r"\[[12][0-9]{3}\]"
# Grab title up to a year/citation; leave page refs intact for now (we'll clean later)
TITLE_PAT = re.compile(r"^(?P<title>.+?\s" + YEAR + r"(?:\s[^,]*)?)", re.IGNORECASE)

def guess_jurisdiction(text):
    t = text.upper()
    if "JRC" in t or "JLR" in t or "JERSEY" in t:
        return "Jersey"
    if "GUERNSEY" in t or "GRC" in t:
        return "Guernsey"
    if "EWHC" in t or "EWCA" in t or "UK" in t:
        return "UK"
    return ""

def parse_line(line_no, text):
    """
    Return dict with Title/Year/Citation/Jurisdiction/Line if it looks like a case line,
    else None.
    """
    # Common junk to skip
    tt = text.strip().strip("•–-·")
    if not tt:
        return None

    m = TITLE_PAT.match(tt)
    if not m:
        return None

    title = m.group("title").strip()
    # try to split out Year & Citation (loose heuristic)
    year_m = re.search(YEAR, title)
    year = year_m.group(0).strip("[]") if year_m else ""

    citation = ""
    # e.g. "... [2015] JRC 186" or "... [2014] JLR 305"
    cit_m = re.search(r"\[" + year + r"\]\s+([A-Z]{2,5})\s+[0-9A-Za-z/ ]+", title) if year else None
    if cit_m:
        idx = title.find("[" + year + "]")
        citation = title[idx:].strip()
        title = title[:idx].rstrip()

    jurisdiction = guess_jurisdiction(tt)

    # Build a stable-ish id: YEAR + first 30 chars normalized
    base = re.sub(r"[^A-Za-z0-9]+", "_", (title + "_" + (citation or "")))[:30].strip("_")
    case_id = f"{year}_{base}".lower() if year else base.lower()

    return {
        "case_id": case_id,
        "Title": f"{title} [{year}] {citation.split(' ',1)[1]}" if (year and citation) else (f"{title} [{year}]" if year else title),
        "Year": year,
        "Citation": citation,
        "Jurisdiction": jurisdiction,
        "Line": str(line_no),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--out", required=True, help="Output CSV (e.g., data/cases_from_ltj.csv)")
    ap.add_argument("--report", default="out/rebuild_report.json")
    ap.add_argument("--missing", default="out/rebuild_missing.csv")
    args = ap.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.missing).parent.mkdir(parents=True, exist_ok=True)

    with open(args.ltj_lines, "r", encoding="utf-8") as f:
        lines = json.load(f)

    # Expect list of {"line_no": int, "text": "..."}
    subset = [r for r in lines if args.start <= int(r.get("line_no", -1)) <= args.end]

    rows = []
    missed = []
    for r in subset:
        ln = int(r.get("line_no", -1))
        txt = r.get("text", "")
        parsed = parse_line(ln, txt)
        if parsed:
            rows.append(parsed)
        else:
            missed.append({"line_no": ln, "text": txt})

    # Write canonical CSV
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CASE_ROW_COLS)
        w.writeheader()
        for row in rows:
            w.writerow(row)

    # Missing (for manual inspect)
    with open(args.missing, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["line_no","text"])
        w.writeheader()
        for m in missed:
            w.writerow(m)

    # Report
    report = {
        "expected_lines": len(subset),
        "parsed_cases": len(rows),
        "unparsed": len(missed),
        "start": args.start,
        "end": args.end,
        "out_csv": args.out,
        "missing_csv": args.missing
    }
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"✅ Rebuilt {len(rows)} cases from LTJ lines {args.start}–{args.end}")
    print(f"ℹ️  Unparsed lines: {len(missed)} → {args.missing}")
    print(f"📄 Report: {args.report}")

if __name__ == "__main__":
    main()
