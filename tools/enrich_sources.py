#!/usr/bin/env python3
"""
enrich_sources.py

Reads a list of cases from a CSV and (1) resolves / normalizes a source URL if
present, (2) fetches the HTML, and (3) extracts light metadata (title, date,
neutral citation, court) where possible.

Outputs:
  - a JSON manifest summarizing each case and any parsed metadata
  - optional saved HTML files (one per case)

CSV format (header required):
  case_id, jurisdiction, url

Example:
  python tools/enrich_sources.py \
      --cases data/cases.csv \
      --out-json out/sources.json \
      --out-html out/html
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

# --- Make local `tools/` folder importable when run from GitHub Actions or anywhere ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from util import http_get, sleep_jitter, save_json, ensure_dir  # type: ignore


# --------------------------
# Metadata parsing utilities
# --------------------------

META_PATTERNS = {
    "neutral_citation": [
        # Jersey / UK neutral citation patterns
        r"\b(?:\[\d{4}\]\s*[A-Z]{2,}\s*[A-Za-z0-9\- ]+|\bJRC\s*\d{4}\s*\d+\b)",
        r"\b(?:\[\d{4}\]\s*[A-Z]{2,}\s*\d+)",
    ],
    "date": [
        r"\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
    ],
}

TITLE_CLEANERS = [
    # Remove site suffixes
    r"\s*\|\s*Jersey Law\s*$",
    r"\s*-\s*Jersey Law\s*$",
    r"\s*-\s*BAILII\s*$",
    r"\s*\|\s*BAILII\s*$",
]


def normalize_url(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if raw.startswith("//"):
        return "https:" + raw
    if not raw.startswith("http"):
        return "https://" + raw
    return raw


def parse_title(html: str) -> Optional[str]:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
    if not m:
        return None
    title = re.sub(r"\s+", " ", m.group(1)).strip()
    for pat in TITLE_CLEANERS:
        title = re.sub(pat, "", title, flags=re.I)
    return title.strip() or None


def parse_meta_generic(html: str) -> Dict[str, str]:
    meta = {}

    # Neutral citation
    for pat in META_PATTERNS["neutral_citation"]:
        m = re.search(pat, html, flags=re.I)
        if m:
            meta["neutral_citation"] = m.group(0).strip()
            break

    # Date
    for pat in META_PATTERNS["date"]:
        m = re.search(pat, html, flags=re.I)
        if m:
            meta["date"] = m.group(0).strip()
            break

    # Court (best-effort)
    # Look for simple signals first
    if "jerseylaw.je" in html.lower():
        meta.setdefault("court", "Royal Court of Jersey (inferred)")
    elif "bailii.org" in html.lower():
        meta.setdefault("court", "BAILII (inferred)")

    return meta


# --------------------------
# IO helpers
# --------------------------

def read_cases_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"case_id", "jurisdiction", "url"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"cases.csv missing required columns: {sorted(missing)}")

        return [dict(row) for row in reader]


def save_html(path: str, html: str) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


# --------------------------
# Core
# --------------------------

def enrich_cases(
    cases_csv: str,
    out_json: str,
    out_html_dir: str,
    user_agent: str = "CourtFirstBot/1.0 (+github-actions)"
) -> Dict[str, int]:
    cases = read_cases_csv(cases_csv)
    ensure_dir(os.path.dirname(out_json))
    ensure_dir(out_html_dir)

    results: List[Dict[str, object]] = []
    stats = {"total": 0, "fetched": 0, "skipped": 0, "errors": 0}

    for row in cases:
        stats["total"] += 1

        case_id = (row.get("case_id") or "").strip()
        jurisdiction = (row.get("jurisdiction") or "").strip()
        raw_url = (row.get("url") or "").strip()
        url = normalize_url(raw_url)

        entry: Dict[str, object] = {
            "case_id": case_id,
            "jurisdiction": jurisdiction,
            "source_url": url,
            "status": "skipped" if not url else "pending",
            "meta": {},
        }

        if not case_id:
            entry["status"] = "error"
            entry["error"] = "missing case_id"
            stats["errors"] += 1
            results.append(entry)
            continue

        if not url:
            # No URL supplied: we keep it as a placeholder; do not fail the run
            entry["status"] = "no_url"
            stats["skipped"] += 1
            results.append(entry)
            continue

        try:
            html = http_get(url, user_agent=user_agent, timeout=30)
            if not html:
                entry["status"] = "fetch_failed"
                stats["errors"] += 1
                results.append(entry)
                continue

            # Save HTML
            html_path = os.path.join(out_html_dir, f"{case_id}.html")
            save_html(html_path, html)

            # Parse metadata
            title = parse_title(html)
            meta = parse_meta_generic(html)

            if title:
                meta["title"] = title

            entry["meta"] = meta
            entry["status"] = "ok"
            stats["fetched"] += 1

            # Be nice to remote sites
            sleep_jitter(0.5, 1.25)

        except Exception as e:
            entry["status"] = "error"
            entry["error"] = str(e)
            stats["errors"] += 1

        results.append(entry)

    save_json(out_json, results)
    return stats


# --------------------------
# CLI
# --------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich cases with source HTML & metadata")
    parser.add_argument("--cases", default="data/cases.csv", help="Input CSV with cases")
    parser.add_argument("--out-json", default="out/sources.json", help="Output JSON manifest")
    parser.add_argument("--out-html", default="out/html", help="Directory to save fetched HTML")
    args = parser.parse_args()

    stats = enrich_cases(args.cases, args.out_json, args.out_html)
    print(f"Done. Stats: {json.dumps(stats, indent=2)}")

if __name__ == "__main__":
    main()
