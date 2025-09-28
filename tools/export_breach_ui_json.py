#!/usr/bin/env python3
import argparse, json, pathlib, sys

def load_json(path:str):
    p = pathlib.Path(path)
    if not p.exists():
        sys.exit(f"ERROR: Missing {path}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def to_breach_record(c):
    # Map a candidate into Breach-ui schema (category/tag/aliases + provenance)
    return {
        "category": "Litigation / Case Law",
        "tag": "Breach (candidate)",
        "aliases": [],
        "provenance": [{
            "source_type": "Case",
            "label": c.get("authority_label"),
            "source_id": c.get("authority_id"),
            "block_id": c.get("pid"),
            "page": c.get("page"),
            "line": c.get("line"),
            "excerpt": c.get("snippet", "")[:400],
            "confidence": 0.50
        }]
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="candidates.json from build_candidates")
    ap.add_argument("--out", required=True, help="breaches.json formatted for Breach-ui")
    args = ap.parse_args()

    data = load_json(args.input)
    cands = data.get("candidates") or []
    breaches = [to_breach_record(c) for c in cands]

    outp = pathlib.Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as f:
        json.dump(breaches, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(breaches)} breach records -> {outp}")

if __name__ == "__main__":
    main()
