#!/usr/bin/env python3
"""
Generate breach candidates by scanning case texts for breach-like phrases.
Output: out/breach_candidates.json
"""
from __future__ import annotations
import json, os, re
from util import OUT_DIR, CORPUS_JSONL, CANDIDATES_JSON, window_around

PHRASES = [
    r"breach of trust",
    r"breach of fiduciary duty",
    r"fiduciary breach",
    r"acted in self[- ]interest",
    r"conflicted trustee",
    r"misappropriation of (trust )?assets?",
    r"failure to (disclose|account)",
    r"unauthori[sz]ed investment",
    r"negligence\b",
    r"breach of (mandatory|regulatory) duty",
]

PATTERNS = [re.compile(p, re.I) for p in PHRASES]

def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    candidates = []

    with open(CORPUS_JSONL, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            doc = json.loads(line)
            text = doc.get("text", "") or ""
            for pat in PATTERNS:
                for m in pat.finditer(text):
                    snippet = window_around(text, m.start(), m.end(), chars=240)
                    candidates.append({
                        "case_id": doc.get("case_id"),
                        "title": doc.get("title"),
                        "url": doc.get("url"),
                        "jurisdiction": doc.get("jurisdiction"),
                        "match": m.group(0),
                        "tag_suggestion": pat.pattern,   # keep raw pattern for transparency
                        "snippet": snippet,
                        "confidence": 0.6,               # simple heuristic; tune later
                    })

    with open(CANDIDATES_JSON, "w", encoding="utf-8") as out:
        json.dump({"candidates": candidates}, out, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
