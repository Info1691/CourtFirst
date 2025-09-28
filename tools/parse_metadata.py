#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse minimal metadata from fetched HTML files.

Inputs:
  --html DIR/         (files saved by fetch_cases.py)
Outputs:
  --out JSON          (array of dicts per case_id)

For each file we extract ONLY what is present in the HTML:
  - title (from <title> if present)
  - neutral citation (simple regex from title/body if present)
  - court/site (simple domain from saved report if available)
We DO NOT fill in anything we cannot find; missing fields are omitted.
"""

from pathlib import Path
import argparse
import re
import json
from bs4 import BeautifulSoup

from tools.util import load_json, save_json

CITE_RE = re.compile(r"\[(\d{4})\]\s*[A-Z]{2,}[A-Za-z]*\s*\d+|\bJRC\s*\d{2,4}\b")

def extract_fields(html_text: str):
    soup = BeautifulSoup(html_text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else None

    # Try to locate a neutral citation anywhere in the doc
    cite = None
    in_title = title or ""
    m = CITE_RE.search(in_title)
    if not m:
        body_text = soup.get_text(separator=" ", strip=True)
        m = CITE_RE.search(body_text[:2000])  # first chunk only for speed
    if m:
        cite = m.group(0)

    return {"title": title, "neutral_citation": cite}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", dest="html_dir", required=True)
    ap.add_argument("--out", dest="out_json", required=True)
    ap.add_argument("--report", dest="report_json", default="out/fetch_report.json",
                    help="fetch report for mapping filenames to URLs (optional)")
    args = ap.parse_args()

    html_dir = Path(args.html_dir)
    out_path = Path(args.out_json)

    # Optionally load report to map URLs
    url_by_file = {}
    try:
        report = load_json(Path(args.report_json))
        for ok in report.get("ok", []):
            html_file = ok.get("html_file")
            if html_file:
                url_by_file[Path(html_file).name] = ok.get("url")
    except Exception:
        pass

    records = []
    for p in sorted(html_dir.glob("*.html")):
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
            fields = extract_fields(text)
            rec = {"case_file": p.name}
            if url_by_file.get(p.name):
                rec["source_url"] = url_by_file[p.name]
            # Only include fields that exist
            for k, v in fields.items():
                if v:
                    rec[k] = v
            records.append(rec)
        except Exception as e:
            records.append({"case_file": p.name, "error": str(e)})

    save_json(records, out_path)

if __name__ == "__main__":
    main()
