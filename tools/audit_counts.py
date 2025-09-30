#!/usr/bin/env python3
"""
Audit counts between:
- LTJ lines (ground truth by range)
- rebuilt CSV
- cleaned CSV
- your current data/cases.csv (if you want to compare)

Outputs a JSON report and a CSV of any titles missing from later stages.
"""

import argparse, csv, json
from pathlib import Path

def read_csv_titles(p, title_key="Title"):
    if not Path(p).exists():
        return set(), []
    rows = []
    with open(p, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    titles = set([row.get(title_key,"").strip() for row in rows if row.get(title_key,"").strip()])
    return titles, rows

def read_ltj_count(report_json):
    if not Path(report_json).exists():
        return None
    with open(report_json, "r", encoding="utf-8") as f:
        rep = json.load(f)
    return rep.get("parsed_cases")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild-report", default="out/rebuild_report.json")
    ap.add_argument("--rebuilt", default="data/cases_from_ltj.csv")
    ap.add_argument("--clean", default="data/cases_clean.csv")
    ap.add_argument("--final", default="data/cases.csv")
    ap.add_argument("--out", default="out/audit_report.json")
    ap.add_argument("--missing", default="out/audit_missing.csv")
    args = ap.parse_args()

    Path("out").mkdir(exist_ok=True)

    ltj_parsed = read_ltj_count(args.rebuild_report) or 0
    rebuilt_titles, _ = read_csv_titles(args.rebuilt)
    clean_titles, _   = read_csv_titles(args.clean)
    final_titles, _   = read_csv_titles(args.final)

    missing_in_clean = sorted(rebuilt_titles - clean_titles)
    missing_in_final = sorted(rebuilt_titles - final_titles)

    with open(args.missing, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MissingStage","Title"])
        for t in missing_in_clean:
            w.writerow(["clean", t])
        for t in missing_in_final:
            w.writerow(["final", t])

    report = {
        "ltj_parsed_cases": ltj_parsed,
        "rebuilt_csv": {"path": args.rebuilt, "count": len(rebuilt_titles)},
        "clean_csv":   {"path": args.clean,   "count": len(clean_titles), "missing_from_clean": len(missing_in_clean)},
        "final_csv":   {"path": args.final,   "count": len(final_titles), "missing_from_final": len(missing_in_final)},
        "missing_csv": args.missing
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"✅ Audit written → {args.out}")
    print(f"🔎 Missing list → {args.missing}")

if __name__ == "__main__":
    main()
