#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Iterable

# ----------------------------
# Small helpers
# ----------------------------

def slugify(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    s = re.sub(r"[^\w\-\. ]+", "", s, flags=re.UNICODE)
    s = s.replace(" ", "_")
    return s[:200] or "untitled"

def sleep_jitter(base: float = 0.8, spread: float = 0.6) -> None:
    # polite throttle for scraping steps
    time.sleep(max(0.0, base + random.uniform(0.0, spread)))

def safe_filename(text: str) -> str:
    return slugify(text)

def save_json(path: os.PathLike, obj) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path: os.PathLike):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ----------------------------
# CSV handling
# ----------------------------

REQUIRED_CASE_KEYS = ["case_id"]  # we normalize URL columns below

ALIASES = {
    # normalize to "source_url"
    "url": "source_url",
    "link": "source_url",
    "href": "source_url",
}

def _normalize_header(name: str) -> str:
    return (name or "").strip().lower().replace("-", "_").replace(" ", "_")

def read_cases_csv(path: os.PathLike) -> List[Dict[str, str]]:
    """
    Reads a CSV of cases and normalizes headers.
    - Accepts 'url' or 'source_url' (or 'link'/'href'); produces 'source_url'.
    - Requires at least 'case_id' (case-insensitive).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"cases.csv not found: {path}")

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        raw_headers = reader.fieldnames or []
        norm_headers = [_normalize_header(h) for h in raw_headers]

        # map original -> normalized
        header_map = dict(zip(raw_headers, norm_headers))

        # build final headers with aliasing
        final_headers = []
        for h in norm_headers:
            final_headers.append(ALIASES.get(h, h))

        # validate minimum keys
        low_headers = set(final_headers)
        if "case_id" not in low_headers and "id" not in low_headers:
            raise ValueError("cases.csv must contain a 'case_id' (or 'id') column.")

        rows = []
        for row in reader:
            norm_row = {}
            for orig, norm in header_map.items():
                val = row.get(orig, "")
                target = ALIASES.get(norm, norm)
                norm_row[target] = val
            # upgrade 'id' -> 'case_id' if present
            if "case_id" not in norm_row and "id" in norm_row:
                norm_row["case_id"] = norm_row.pop("id")
            rows.append(norm_row)

    return rows

def write_cases_csv(path: os.PathLike, rows: Iterable[Dict[str, str]]) -> None:
    rows = list(rows)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # gather all keys
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    # prefer ordering
    fieldnames = [k for k in ["case_id", "jurisdiction", "source_url"] if k in all_keys] + \
                 sorted(list(all_keys - {"case_id", "jurisdiction", "source_url"}))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
