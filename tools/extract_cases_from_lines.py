#!/usr/bin/env python3
import argparse, csv, json, re
from pathlib import Path

# Heuristics:
# - Many entries look like: "Case Title , [2014] JCA 095 , ..." or "... [1990] 1 AC 109"
# - We try to pick the last [YEAR] block as 'year' and then the token(s) following it as 'citation'
# - Jurisdiction inferred if tokens like JLR/JRC/JCA/JRB/RC appear → "Jersey"; else blank

YEAR_RX = re.compile(r"\[(\d{4})\]")
# grab last [yyyy] and any immediately following report tokens (e.g., "JCA 095", "1 AC 109", etc.)
CITATION_TAIL_RX = re.compile(r"\[(\d{4})\]\s*([^\],;]+(?:\s[^\],;]+)*)")

def parse_row(text: str):
    t = text.strip().strip(",;")
    year = ""
    citation = ""
    title = t

    # find last [year]
    years = list(YEAR_RX.finditer(t))
    if years:
        y_m = years[-1]
        year = y_m.group(1)

        # Try to capture tail as citation
        tail_m = CITATION_TAIL_RX.search(t[y_m.start():])
        if tail_m and tail_m.group(1) == year:
            citation = tail_m.group(0).strip()
            # remove the citation chunk from title
            title = (t[:y_m.start()] + t[y_m.start():].replace(citation, "", 1)).strip(" ,;")
        else:
            # keep year as citation if we can't find tail
            citation = f"[{year}]"
            title = (t[:y_m.start()] + t[y_m.start():].replace(f"[{year}]", "", 1)).strip(" ,;")

    # Clean repeated commas/spaces
    title = re.sub(r"\s*,\s*$", "", title)

    juris = ""
    if re.search(r"\b(JLR|JRC|JCA|RC Jersey|Royal Ct|Samedi|HB|JRB)\b", t, re.I):
        juris = "Jersey"

    return title, year, citation, juris

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json (or LTK.lines.json)")
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--out", required=True, help="Output CSV (merged)")
    args = ap.parse_args()

    lines_path = Path(args.lines)
    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # load existing to merge
    existing = []
    if out_csv.exists():
        with out_csv.open("r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                existing.append(row)

    # de-dupe key set
    seen = {(r.get("title",""), r.get("citation",""), r.get("source_line","")) for r in existing}

    with lines_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    new_rows = []
    for item in data:
        ln = item.get("line_no")
        if not isinstance(ln, int) or ln < args.start or ln > args.end:
            continue
        text = (item.get("text") or "").strip()
        if not text:
            continue

        title, year, citation, juris = parse_row(text)
        if not title and not citation:
            continue

        row = {
            "Title": title,
            "Year": year,
            "Citation": citation,
            "Jurisdiction": juris,
            "Line": str(ln),
            "URL": "",
        }
        key = (row["Title"], row["Citation"], row["Line"])
        if key not in seen:
            new_rows.append(row)
            seen.add(key)

    # Write merged, ordered like your screenshot
    headers = ["Title","Year","Citation","Jurisdiction","Line","URL"]
    all_rows = existing if existing and set(existing[0].keys())==set(headers) else []

    # If the existing file had a different header, we re-normalize it
    if not all_rows:
        all_rows = []

    all_rows.extend(new_rows)
    # sort: primary Title, then Year, then Line (as int)
    def _k(r):
        y = int(r["Year"]) if r["Year"].isdigit() else 0
        try:
            ln = int(r["Line"])
        except:
            ln = 0
        return (r["Title"].lower(), y, ln)

    all_rows.sort(key=_k)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    print(f"Wrote {len(all_rows)} rows → {out_csv}")

if __name__ == "__main__":
    main()
