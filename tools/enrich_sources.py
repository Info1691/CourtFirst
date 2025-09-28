#!/usr/bin/env python3
"""
enrich_sources.py
Input : data/cases.csv  (columns: case_id, jurisdiction, source_url)
Output: out/sources.json  [{case_id, jurisdiction, resolved_url}]
Rule: No fabrication. If source_url is blank, resolved_url is null.
"""

import csv
import os
from typing import List, Dict, Optional

from tools.util import save_json, repo_root

IN_CSV = os.path.join(repo_root(), "data", "cases.csv")
OUT_JSON = os.path.join(repo_root(), "out", "sources.json")


def read_cases_csv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"case_id", "jurisdiction", "source_url"}
        missing = required - set(map(str.lower, r.fieldnames or []))
        if missing:
            raise ValueError(f"cases.csv missing required columns (case-insensitive): {sorted(missing)}")
        for row in r:
            # normalize keys to expected case
            item = {
                "case_id": row.get("case_id") or row.get("CASE_ID") or row.get("Case_ID"),
                "jurisdiction": row.get("jurisdiction") or row.get("JURISDICTION") or row.get("Jurisdiction"),
                "source_url": row.get("source_url") or row.get("SOURCE_URL") or row.get("Source_URL") or "",
            }
            rows.append(item)
    return rows


def main() -> None:
    cases = read_cases_csv(IN_CSV)
    out: List[Dict[str, Optional[str]]] = []
    for c in cases:
        url = (c.get("source_url") or "").strip() or None
        out.append({
            "case_id": c["case_id"],
            "jurisdiction": c["jurisdiction"],
            "resolved_url": url,   # pass-through only; no guessing
        })
    save_json(OUT_JSON, out)
    print(f"Wrote {len(out)} items -> {OUT_JSON}")


if __name__ == "__main__":
    main()
