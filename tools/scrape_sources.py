#!/usr/bin/env python3
"""
Read data/cases.csv, fetch public HTML (if local text not provided),
extract readable text, and write out/corpus.jsonl
(one JSON object per case).
"""
from __future__ import annotations
import os
from util import read_cases_csv, CORPUS_JSONL, append_jsonl, extract_readable_text, fetch_html, sleep_ms, normalize_ws

def main() -> None:
    # fresh output
    if os.path.exists(CORPUS_JSONL):
        os.remove(CORPUS_JSONL)

    cases = read_cases_csv()
    for row in cases:
        text = ""
        if row.local_text:
            # Allow pre-provided text blobs
            text = normalize_ws(row.local_text)
        elif row.source_url:
            try:
                html = fetch_html(row.source_url)
                text = extract_readable_text(html)
                # be polite to public sites
                sleep_ms(600)
            except Exception as e:
                text = f"[ERROR] fetching {row.source_url}: {e}"
        else:
            text = "[ERROR] no source_url or local_text provided"

        doc = {
            "case_id": row.case_id,
            "title": row.title or row.case_id,
            "url": row.source_url,
            "jurisdiction": row.jurisdiction,
            "text": text,
        }
        append_jsonl(doc, CORPUS_JSONL)

if __name__ == "__main__":
    main()
