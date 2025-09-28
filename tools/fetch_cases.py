#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch HTML for cases that have 'source_url'.
Strictly copies what's online; no rewriting or invented text.
"""

import argparse
import urllib.request
from pathlib import Path
from tools.util import read_cases_csv, sleep_jitter, safe_filename, save_json

UA = "Mozilla/5.0 (compatible; CourtFirstBot/0.1; +https://example.invalid/bot)"

def http_get(url: str, timeout: int = 25) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_csv", required=True, help="CSV from enrich_sources (cases_with_urls.csv)")
    p.add_argument("--html", dest="html_dir", required=True, help="Directory to save raw HTML")
    p.add_argument("--report", dest="report_json", required=True, help="Path to save fetch report JSON")
    return p.parse_args()

def main():
    args = parse_args()
    html_dir = Path(args.html_dir)
    html_dir.mkdir(parents=True, exist_ok=True)

    cases = read_cases_csv(args.in_csv)
    report = []
    for c in cases:
        cid = c.get("case_id", "").strip() or "unknown"
        url = (c.get("source_url") or "").strip()
        if not url:
            report.append({"case_id": cid, "status": "skipped", "reason": "no_source_url"})
            continue
        out_file = html_dir / f"{safe_filename(cid)}.html"
        try:
            data = http_get(url)
            out_file.write_bytes(data)
            report.append({"case_id": cid, "status": "ok", "bytes": len(data), "path": str(out_file), "url": url})
        except Exception as e:
            report.append({"case_id": cid, "status": "error", "error": str(e), "url": url})
        sleep_jitter()

    save_json(args.report_json, report)
    print(f"Fetched {sum(1 for r in report if r['status']=='ok')} pages; report -> {args.report_json}")

if __name__ == "__main__":
    main()
