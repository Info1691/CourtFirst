#!/usr/bin/env python3
import csv, re, sys
from pathlib import Path

CLEAN_COLS = ["Title","Year","Citation","Jurisdiction","Line","URL"]

def clean_title(raw):
    """Strip trailing page references and clean citation."""
    # Remove trailing commas/hyphens/numbers
    clean = re.sub(r"[-, ]*\d+([-, ]*\d+)*$", "", raw).strip()
    # Fix spacing before citation bracket if needed
    clean = re.sub(r"\s+\[", " [", clean)
    return clean

def main():
    if len(sys.argv) != 3:
        print("Usage: clean_cases.py <input.csv> <output.csv>")
        sys.exit(1)

    infile = Path(sys.argv[1])
    outfile = Path(sys.argv[2])

    with infile.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    cleaned = []
    for r in rows:
        new_row = {}
        for col in CLEAN_COLS:
            new_row[col] = r.get(col, "").strip()
        if new_row["Title"]:
            new_row["Title"] = clean_title(new_row["Title"])
        cleaned.append(new_row)

    with outfile.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CLEAN_COLS)
        writer.writeheader()
        writer.writerows(cleaned)

    print(f"✅ Cleaned {len(cleaned)} cases → {outfile}")

if __name__ == "__main__":
    main()
