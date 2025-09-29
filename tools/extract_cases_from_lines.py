#!/usr/bin/env python3
import argparse, csv, hashlib, json, re
from pathlib import Path

CASE_SPLIT = re.compile(r"\s+v\s+", re.IGNORECASE)
YEAR_RE = re.compile(r"\[(\d{4})\]")
# Citations often sit at the end in square brackets or parenthesis; we capture a tidy tail.
CITATION_TAIL_RE = re.compile(r"(\[[^\]]+\]|\([^)]+\))\s*$")

def make_case_id(raw: str) -> str:
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"LTJ_{h}"

def parse_line(raw: str):
    """Return dict with minimal guarantees and only add parsed fields if confident."""
    out = {"raw": raw}
    # title (very light: split on ' v ')
    if CASE_SPLIT.search(raw):
        parts = CASE_SPLIT.split(raw, maxsplit=1)
        # keep full title around ' v '
        out["title"] = f"{parts[0].strip()} v {parts[1].strip()}"
    # citation tail
    m = CITATION_TAIL_RE.search(raw)
    if m:
        out["citation"] = m.group(1).strip()
    # year
    y = YEAR_RE.search(raw)
    if y:
        out["year"] = y.group(1)
    return out

def load_lines(p: Path):
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # tolerate either [{"line":123, "text":"..."}] or simple list of strings
    lines = []
    if isinstance(data, list):
        for i, item in enumerate(data, start=1):
            if isinstance(item, dict):
                txt = item.get("text") or item.get("line") or item.get("raw") or ""
                ln  = item.get("line_no") or item.get("line") or i
            else:
                txt = str(item)
                ln  = i
            lines.append((ln, txt))
    else:
        raise ValueError("Unexpected LTJ.lines.json format")
    return lines

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--start", type=int, required=True, help="Start line (inclusive)")
    ap.add_argument("--end", type=int, required=True, help="End line (inclusive)")
    ap.add_argument("--out", required=True, help="Output CSV (CourtFirst/data/cases.csv)")
    ap.add_argument("--merge", action="store_true", help="Merge with existing CSV if present")
    args = ap.parse_args()

    lines = load_lines(Path(args.ltj_lines))
    # filter by the requested span
    span = [(ln, txt) for (ln, txt) in lines if args.start <= ln <= args.end and txt.strip()]

    # build fresh rows
    new_rows = []
    for ln, raw in span:
        parsed = parse_line(raw)
        case_id = make_case_id(raw)
        row = {
            "case_id": case_id,
            "title": parsed.get("title",""),
            "citation": parsed.get("citation",""),
            "year": parsed.get("year",""),
            "jurisdiction": "",                 # unknown at this stage
            "source_url": "",                   # to be filled by enrichers
            "raw": raw,                         # exact, unmodified line text
            "ltj_line": ln
        }
        new_rows.append(row)

    # optional merge (by raw text hash)
    out_path = Path(args.out)
    merged = { r["raw"]: r for r in new_rows }

    if args.merge and out_path.exists():
        with out_path.open("r", newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                if r.get("raw"):
                    merged.setdefault(r["raw"], r)  # keep existing if already there

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["case_id","title","citation","year","jurisdiction","source_url","raw","ltj_line"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in merged.values():
            w.writerow({k: r.get(k,"") for k in fields})

    print(f"Wrote {len(merged)} rows to {out_path}")

if __name__ == "__main__":
    main()
