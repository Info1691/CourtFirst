#!/usr/bin/env python3
import argparse, json, re, csv, sys
from pathlib import Path

CASE_PATTERNS = [
    # Neutral citation patterns first (Jersey):
    re.compile(r"\[(20\d{2})\]\s*JRC\s*(\d{1,3}[A-Z]?)", re.I),
    re.compile(r"\[(20\d{2})\]\s*JCA\s*(\d{1,3}[A-Z]?)", re.I),
    # Loose "v" case name heuristic
    re.compile(r"\b([A-Z][A-Za-z0-9\.\-&' ]+ v [A-Z][A-Za-z0-9\.\-&' ]+)\b"),
    # Re/In re forms
    re.compile(r"\b(Re|In re|In the matter of)\s+[A-Z][A-Za-z0-9\.\-&' ]+\b", re.I),
]

def guess_case_id(text: str) -> tuple[str|None, str|None, str|None]:
    """
    Try to derive (case_id, jurisdiction, year) from a line of text.
    """
    for rx in CASE_PATTERNS:
        m = rx.search(text)
        if not m: 
            continue
        if rx.pattern.startswith(r"\[(20"):  # neutral citation
            year, num = m.group(1), m.group(2)
            # JRC or JCA?
            jrc = "JRC" if "JRC" in m.group(0).upper() else ("JCA" if "JCA" in m.group(0).upper() else "JRC")
            case_id = f"{jrc}_{year}_{num}"
            return case_id, "Jersey", year
        else:
            # name match only; fallback id from a normalized slug
            name = m.group(0)
            year = None
            slug = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
            case_id = f"NAME_{slug[:50]}"
            return case_id, "Jersey", year
    return None, None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--start", type=int, default=1276)
    ap.add_argument("--end",   type=int, default=3083)
    ap.add_argument("--out",   required=True, help="Output CSV (cases.csv)")
    args = ap.parse_args()

    path = Path(args.lines)
    if not path.exists():
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(2)

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # LTJ.lines.json is an array of { "line": int, "text": "...", ... }
    rows = [row for row in data if args.start <= int(row.get("line", -1)) <= args.end]

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    seen = set()
    with outp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["case_id","jurisdiction","title","year","source_url"])  # url left empty for now
        for r in rows:
            text = str(r.get("text",""))
            case_id, jur, year = guess_case_id(text)
            if case_id and case_id not in seen:
                seen.add(case_id)
                # keep the whole line as title candidate for now
                title = text.strip()
                # trim wildly long titles
                if len(title) > 300: title = title[:297] + "..."
                w.writerow([case_id, jur or "", title, year or "", ""])

    print(f"Wrote {outp} with {len(seen)} unique rows.")

if __name__ == "__main__":
    main()
