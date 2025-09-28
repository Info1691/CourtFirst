#!/usr/bin/env python3
"""
fetch_cases.py
Input : out/sources.json  (from enrich_sources.py)
Output: out/html/{case_id}.html (exact server output)
        out/fetch_log.json       (status per case)
"""

import json
import os
from typing import Dict, Any, List

from tools.util import repo_root, read_json, http_get, sleep_jitter, write_text, save_json, safe_filename, ensure_dir

IN_SOURCES = os.path.join(repo_root(), "out", "sources.json")
OUT_HTML_DIR = os.path.join(repo_root(), "out", "html")
OUT_LOG = os.path.join(repo_root(), "out", "fetch_log.json")


def main() -> None:
    ensure_dir(OUT_HTML_DIR)
    sources: List[Dict[str, Any]] = read_json(IN_SOURCES)
    log: List[Dict[str, Any]] = []

    for item in sources:
        case_id = item.get("case_id")
        url = item.get("resolved_url")
        if not url:
            log.append({"case_id": case_id, "status": "skipped_no_url"})
            continue

        try:
            resp = http_get(url)
            html = resp.text  # EXACT HTML as delivered
            fname = f"{safe_filename(case_id)}.html"
            out_path = os.path.join(OUT_HTML_DIR, fname)
            write_text(out_path, html)
            log.append({
                "case_id": case_id,
                "status": "ok",
                "request_url": url,
                "final_url": resp.url,
                "http_status": resp.status_code,
                "bytes": len(html.encode("utf-8"))
            })
        except Exception as e:
            log.append({
                "case_id": case_id,
                "status": "error",
                "request_url": url,
                "error": repr(e)
            })

        sleep_jitter()

    save_json(OUT_LOG, log)
    print(f"Saved HTML to {OUT_HTML_DIR} and log to {OUT_LOG}")


if __name__ == "__main__":
    main()
