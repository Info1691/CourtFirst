#!/usr/bin/env python3
"""
Convert breach candidates to Breach-ui's breaches.json schema with provenance.
Output: out/breaches.json
"""
from __future__ import annotations
import json, os, re
from collections import defaultdict
from util import CANDIDATES_JSON, BREACHES_JSON, OUT_DIR

# simple canonicalizer -> map regex to human tag + category
CANON_MAP = [
    (re.compile(r"breach of fiduciary duty", re.I), ("Breach of fiduciary duty", "Fiduciary Duty")),
    (re.compile(r"breach of trust", re.I), ("Breach of trust", "Trust Duty")),
    (re.compile(r"fiduciary breach", re.I), ("Breach of fiduciary duty", "Fiduciary Duty")),
    (re.compile(r"acted in self[- ]interest", re.I), ("Acting in self-interest", "Fiduciary Duty")),
    (re.compile(r"conflicted trustee", re.I), ("Conflicted trustee", "Fiduciary Duty")),
    (re.compile(r"misappropriation of (trust )?assets?", re.I), ("Misappropriation of trust assets", "Misappropriation")),
    (re.compile(r"failure to (disclose|account)", re.I), ("Failure to disclose", "Disclosure")),
    (re.compile(r"unauthori[sz]ed investment", re.I), ("Improper investment", "Investment Mismanagement")),
    (re.compile(r"\bnegligence\b", re.I), ("Negligence", "Duty of Care")),
    (re.compile(r"breach of (mandatory|regulatory) duty", re.I), ("Breach of mandatory duty", "Legal Duty")),
]

def canonicalize(match: str) -> tuple[str, str]:
    for pat, (tag, cat) in CANON_MAP:
        if pat.search(match):
            return tag, cat
    # Fallback
    cap = match.strip().capitalize()
    return cap, "Uncategorized"

def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    with open(CANDIDATES_JSON, encoding="utf-8") as f:
        data = json.load(f)
    cands = data.get("candidates", [])

    grouped = defaultdict(lambda: {"aliases": set(), "provenance": [] , "category": "Uncategorized"})
    for c in cands:
        tag, category = canonicalize(c.get("match", ""))
        key = tag
        grouped[key]["category"] = category
        # include raw match as alias; we can dedupe later
        if c.get("match"):
            grouped[key]["aliases"].add(c["match"])
        # provenance entry
        grouped[key]["provenance"].append({
            "source_type": "Case",
            "label": c.get("title") or c.get("case_id"),
            "source_id": c.get("case_id"),
            "page": None,
            "line": None,
            "excerpt": c.get("snippet") or "",
            "confidence": c.get("confidence", 0.5),
            "url": c.get("url"),
            "jurisdiction": c.get("jurisdiction"),
        })

    # shape to Breach-ui schema
    out = []
    for tag, obj in grouped.items():
        out.append({
            "category": obj["category"],
            "tag": tag,
            "aliases": sorted(a for a in obj["aliases"] if a and a.lower() != tag.lower()),
            "provenance": obj["provenance"],
        })

    with open(BREACHES_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
