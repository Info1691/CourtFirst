#!/usr/bin/env python3
"""
Safely clean titles (strip page refs etc.) WITHOUT losing rows.
Adds original/provenance columns.

Input:  data/cases_from_ltj.csv
Output: data/cases_clean.csv + out/clean_report.json
"""

import csv, json, re
from pathlib import Path

INP = "data/cases_from_ltj.csv"
OUT = "data/cases_clean.csv"
REPORT = "out/clean_report.json"

COLS = ["case_id","Title","Year","Citation","Jurisdiction","Line"]
OUT_COLS = COLS + ["OriginalTitle","CleanNote"]

def clean_title(title):
    if not title:
        return title, ""
    orig = title
    # remove trailing page refs / number lists at end
    cleaned = re.sub(r"[-, ]*\d+(?:[-, ]*\d+)*\s*$", "", title).strip()
    note = "stripped trailing page refs" if cleaned != orig else ""
    # normalize spaces
    cleaned2 = re.sub(r"\s{2,}", " ", cleaned)
    if cleaned2 != cleaned and not note:
        note = "normalized spaces"
    return cleaned2, note

def main():
    Path("out").mkdir(exist_ok=True)
    total = 0
    changed = 0
    out_rows = []

    with open(INP, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            total += 1
            title = row.get("Title","")
            new_title, note = clean_title(title)
            new_row = {k: row.get(k,"") for k in COLS}
            new_row["OriginalTitle"] = title
            new_row["Title"] = new_title
            new_row["CleanNote"] = note
            if note:
                changed += 1
            out_rows.append(new_row)

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS)
        w.writeheader()
        w.writerows(out_rows)

    with open(REPORT, "w", encoding="utf-8") as f:
        json.dump({"input_rows": total, "cleaned_titles": changed, "out": OUT}, f, indent=2)

    print(f"✅ Cleaned: {changed}/{total} titles → {OUT}")
    print(f"📄 Report: {REPORT}")

if __name__ == "__main__":
    main()
