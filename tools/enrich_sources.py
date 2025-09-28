#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pass-through URL enricher.

- Reads an input CSV of cases (any header style).
- Accepts 'url' OR 'source_url' (or link/href), normalizes to 'source_url'.
- Writes OUT/cases_with_urls.csv with a guaranteed 'source_url' column.
- Also writes OUT/urls.json for debugging/inspection.

No guessing, no fabrication: if there is no URL, the cell stays blank.
"""

import argparse
from pathlib import Path
from tools.util import read_cases_csv, write_cases_csv, save_json

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_csv", required=True, help="Path to input cases CSV")
    p.add_argument("--outdir", dest="outdir", required=True, help="Output directory")
    return p.parse_args()

def main():
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    cases = read_cases_csv(args.in_csv)

    # make sure 'source_url' exists (copy from 'url' already handled in util.read_cases_csv)
    for c in cases:
        c.setdefault("source_url", c.get("url", ""))

    # Write normalized CSV
    out_csv = outdir / "cases_with_urls.csv"
    write_cases_csv(out_csv, cases)

    # Small JSON for quick auditing
    urls = [{"case_id": c.get("case_id", ""), "source_url": c.get("source_url", "")} for c in cases]
    save_json(outdir / "urls.json", urls)

    print(f"Wrote {len(cases)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
