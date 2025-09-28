#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pass-through URL normaliser.

Input CSV (UTF-8) at --in must contain:
  - case_id
  - url   (the original source URL; we do NOT invent anything)

Output:
  out/cases_with_urls.csv  (case_id, source_url)
  out/urls.json            (list of {case_id, source_url})

We do not guess or fabricate. If a row lacks 'url', it is skipped and recorded.
"""

from pathlib import Path
import argparse
from typing import List, Dict

from tools.util import read_csv, write_csv, save_json, ensure_dir

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", required=True, help="Input CSV with 'case_id' and 'url'")
    ap.add_argument("--outdir", dest="outdir", required=True, help="Output folder")
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    outdir = Path(args.outdir)
    ensure_dir(outdir)

    hmap, rows = read_csv(in_path)
    miss = [k for k in ("case_id", "url") if k not in hmap]
    if miss:
        raise ValueError(f"Input CSV is missing required columns (case-insensitive): {miss}")

    out_rows: List[List[str]] = []
    url_list: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []

    for row in rows:
        case_id = row[hmap["case_id"]].strip()
        url = row[hmap["url"]].strip()
        if not case_id:
            continue
        if not url:
            skipped.append({"case_id": case_id, "reason": "no url"})
            continue
        out_rows.append([case_id, url])
        url_list.append({"case_id": case_id, "source_url": url})

    write_csv(["case_id", "source_url"], out_rows, outdir / "cases_with_urls.csv")
    save_json(url_list, outdir / "urls.json")
    save_json(skipped, outdir / "skipped.json")

if __name__ == "__main__":
    main()
