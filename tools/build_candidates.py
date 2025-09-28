#!/usr/bin/env python3
import argparse, json, sys, pathlib

def load_json(path: str):
    p = pathlib.Path(path)
    if not p.exists():
        sys.exit(f"ERROR: Missing file {path}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ltj-lines", required=True)
    ap.add_argument("--ltj-citations", required=True)
    ap.add_argument("--ltj-index", required=False)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # Load inputs (structure: arrays/objects as produced by LTJ-ui build)
    lines = load_json(args.ltj_lines)         # huge; we won’t iterate fully here
    citations = load_json(args.ltj_citations) # list of citation dicts
    # index = load_json(args.ltj_index) if args.ltj_index else None

    # Very conservative candidate builder:
    # Treat any citation record whose "authority_kind" is "case" and
    # whose snippet includes keywords as a "breach" candidate.
    KEYWORDS = {"breach of trust", "fiduciary", "breach", "duty", "misappropriation"}
    candidates = []
    for c in citations if isinstance(citations, list) else []:
        if str(c.get("authority_kind", "")).lower() != "case":
            continue
        snippet = (c.get("snippet") or "").lower()
        if any(k in snippet for k in KEYWORDS):
            candidates.append({
                "phrase": "breach",                # normalized label
                "normalized": "breach",
                "polarity": "breach",
                "jurisdiction": c.get("jurisdiction"),
                "pid": c.get("from_pid") or c.get("pid"),
                "authority_id": c.get("to") or c.get("authority_id"),
                "authority_label": c.get("to_label") or c.get("authority_label"),
                "snippet": c.get("snippet", ""),
                "cues": [c.get("cue")] if c.get("cue") else [],
                "statutes": c.get("statutes", []),
            })

    outp = pathlib.Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as f:
        json.dump({"candidates": candidates}, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(candidates)} candidates -> {outp}")

if __name__ == "__main__":
    main()
