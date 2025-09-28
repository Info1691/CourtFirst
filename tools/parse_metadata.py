#!/usr/bin/env python3
"""
parse_metadata.py
Input : out/html/*.html
Output: out/metadata.json  [{case_id, title?, court?, date?, citation?}]
Rules:
- Parse only what's present.
- Never synthesize values.
"""

import os
import re
from typing import Dict, Any, List, Optional

from tools.util import repo_root, save_json

HTML_DIR = os.path.join(repo_root(), "out", "html")
OUT_JSON = os.path.join(repo_root(), "out", "metadata.json")


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def pick(patterns: List[re.Pattern], text: str) -> Optional[str]:
    for p in patterns:
        m = p.search(text)
        if m:
            return m.group(1).strip()
    return None


def main() -> None:
    items: List[Dict[str, Any]] = []

    if not os.path.isdir(HTML_DIR):
        print(f"No HTML folder at {HTML_DIR}")
        save_json(OUT_JSON, items)
        return

    files = [f for f in os.listdir(HTML_DIR) if f.lower().endswith(".html")]
    for f in files:
        case_id = os.path.splitext(f)[0]
        html = read_file(os.path.join(HTML_DIR, f))

        # Try to find a title (very conservative)
        title = pick([
            re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL),
            re.compile(r'<meta\s+name=["\']citation_title["\']\s+content=["\'](.*?)["\']', re.IGNORECASE),
        ], html)

        # Try court
        court = pick([
            re.compile(r'<meta\s+name=["\']citation_journal_title["\']\s+content=["\'](.*?)["\']', re.IGNORECASE),
            re.compile(r'<meta\s+name=["\']DC.title["\']\s+content=["\'](.*?)["\']', re.IGNORECASE),
        ], html)

        # Try date
        date = pick([
            re.compile(r'<meta\s+name=["\']citation_date["\']\s+content=["\'](.*?)["\']', re.IGNORECASE),
            re.compile(r'<meta\s+name=["\']DC.date["\']\s+content=["\'](.*?)["\']', re.IGNORECASE),
        ], html)

        # Try neutral citation
        citation = pick([
            re.compile(r'<meta\s+name=["\']citation_reference["\']\s+content=["\'](.*?)["\']', re.IGNORECASE),
            re.compile(r'\[20\d{2}\]\s+[A-Z]{2,}.*?\d+', re.IGNORECASE),  # very generic fallback; only if found
        ], html)

        record: Dict[str, Any] = {"case_id": case_id}
        if title:   record["title"] = title
        if court:   record["court"] = court
        if date:    record["date"] = date
        if citation:record["citation"] = citation

        items.append(record)

    save_json(OUT_JSON, items)
    print(f"Wrote {len(items)} metadata rows -> {OUT_JSON}")


if __name__ == "__main__":
    main()
