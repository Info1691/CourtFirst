#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch HTML for each case source_url.

Input:
  --in  out/cases_with_urls.csv  (case_id, source_url)

Outputs:
  --html DIR/     (one .html per case; filename=case_id.html)
  --report JSON   (exact HTTP results; no fabrication)

We never invent content: we save exactly the server response text on 200;
non-200 (or exceptions) are recorded in the report and that case is skipped.
"""

from pathlib import Path
import argparse
import requests
from typing import Dict, Any

from tools.util import read_csv, ensure_dir, safe_filename, http_get, sleep_jitter, save_json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", required=True)
    ap.add_argument("--html", dest="html_dir", required=True)
    ap.add_argument("--report", dest="report_json", required=True)
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    html_dir = Path(args.html_dir)
    ensure_dir(html_dir)
    report_path = Path(args.report_json)

    hmap, rows = read_csv(in_path)
    if not {"case_id", "source_url"}.issubset(hmap.keys()):
        raise ValueError("Input must have columns: case_id, source_url")

    session = requests.Session()
    results: Dict[str, Any] = {"ok": [], "failed": []}

    for row in rows:
        case_id = row[hmap["case_id"]].strip()
        url = row[hmap["source_url"]].strip()
        if not case_id or not url:
            continue

        rec = {"case_id": case_id, "url": url}
        try:
            status, text = http_get(url, session)
            if status == 200 and text:
                fname = safe_filename(f"{case_id}.html")
                outp = html_dir / fname
                with outp.open("w", encoding="utf-8", errors="ignore") as f:
                    f.write(text)
                rec.update({"status": status, "html_file": str(outp)})
                results["ok"].append(rec)
            else:
                rec.update({"status": status, "error": "non-200 or empty body"})
                results["failed"].append(rec)
        except Exception as e:
            rec.update({"status": None, "error": str(e)})
            results["failed"].append(rec)

        sleep_jitter(0.9)

    save_json(results, report_path)

if __name__ == "__main__":
    main()
