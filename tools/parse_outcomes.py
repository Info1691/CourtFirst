#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse outcome-like snippets from fetched HTML (exact text only).

Inputs:
  --html DIR/             (HTML files saved by fetch_cases.py)
  --report out/fetch_report.json  (maps file -> URL)
Outputs:
  --out out/outcomes.json

Heuristics:
  - Extract the <title>.
  - Scan visible text for sections headed 'Held', 'Conclusion', 'Disposition',
    'Order', 'Result', 'Decision', 'Outcome' (case-insensitive).
  - Capture up to ~800 chars following each heading as a snippet.
We DO NOT fabricate anything. If nothing matches, the record has no 'snippets'.
"""

from pathlib import Path
import argparse
import re
from bs4 import BeautifulSoup

from tools.util import load_json, save_json

HEADERS = [
    r"\bHeld\b",
    r"\bConclusion\b",
    r"\bConclusions\b",
    r"\bDisposition\b",
    r"\bOrder\b",
    r"\bOrders\b",
    r"\bResult\b",
    r"\bDecision\b",
    r"\bOutcome\b",
]
HEADER_RE = re.compile("|".join(HEADERS), re.IGNORECASE)

def harvest_snippets(text: str, maxlen: int = 800):
    out = []
    for m in HEADER_RE.finditer(text):
        start = m.end()
        snippet = text[start:start + maxlen].strip()
        # stop at next header if present
        nxt = HEADER_RE.search(snippet)
        if nxt:
            snippet = snippet[:nxt.start()].strip()
        # collapse whitespace
        snippet = re.sub(r"\s+", " ", snippet)
        if snippet:
            out.append({"heading": m.group(0), "snippet": snippet})
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True, help="directory with fetched HTML")
    ap.add_argument("--report", required=True, help="fetch_report.json")
    ap.add_argument("--out", required=True, help="outcomes.json")
    args = ap.parse_args()

    html_dir = Path(args.html)
    report = load_json(Path(args.report))

    url_by_file = {}
    for ok in report.get("ok", []):
        fn = Path(ok.get("html_file","")).name
        if fn:
            url_by_file[fn] = ok.get("url")

    records = []
    for path in sorted(html_dir.glob("*.html")):
        try:
            html = path.read_text(encoding="utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            title = soup.title.get_text(strip=True) if soup.title else None
            text = soup.get_text(separator=" ", strip=True)
            snippets = harvest_snippets(text)
            rec = {"case_file": path.name}
            if title: rec["title"] = title
            if url_by_file.get(path.name): rec["source_url"] = url_by_file[path.name]
            if snippets: rec["snippets"] = snippets
            records.append(rec)
        except Exception as e:
            records.append({"case_file": path.name, "error": str(e)})

    save_json(records, Path(args.out))

if __name__ == "__main__":
    main()
