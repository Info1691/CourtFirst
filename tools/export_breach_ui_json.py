#!/usr/bin/env python3
import argparse, json
from pathlib import Path

def read_json(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))

def write_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    data = read_json(Path(args.input))
    cands = data.get("candidates") or []
    # group by tag (already consolidated by scraper, but safe)
    by_tag = {}
    for c in cands:
        tag = (c.get("normalized") or c.get("phrase") or "").strip()
        if not tag:
            continue
        bucket = by_tag.setdefault(tag, {"category": "Unclassified", "tag": tag, "aliases": [], "provenance": []})
        bucket["provenance"].append({
            "source_type": "Case",
            "label": c.get("title") or c.get("case_id"),
            "source_id": c.get("case_id"),
            "pid": c.get("pid"),
            "paragraph_index": c.get("para_index"),
            "excerpt": c.get("text", "")[:600],
            "jurisdiction": c.get("jurisdiction"),
            "url": c.get("url"),
            "confidence": 0.9 if c.get("in_outcome_zone") else 0.7
        })
    write_json(Path(args.out), list(by_tag.values()))
    print(f"Wrote breaches → {args.out}")

if __name__ == "__main__":
    main()
