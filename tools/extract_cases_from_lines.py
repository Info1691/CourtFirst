#!/usr/bin/env python3
import argparse, json, csv, re, sys
from pathlib import Path

CASE_SPLIT_RE = re.compile(r"\s+v\s+", re.IGNORECASE)
YEAR_IN_BRACKETS_RE = re.compile(r"\[\s*\d{4}\s*]")   # e.g. [2010]
# A conservative Jersey/UK citation hint (not exhaustive, just to improve signal)
CITATION_HINT_RE = re.compile(
    r"\b(JRC|JLR|EWHC|EWCA|UKSC|Privy\s*Council|PC|JCA|JCA\s*\d{4}|WLR|All\s*ER)\b",
    re.IGNORECASE
)

def looks_like_case(text: str) -> bool:
    """Very light filter so we only capture real case rows."""
    if " v " not in text and " V " not in text and " v. " not in text:
        return False
    if YEAR_IN_BRACKETS_RE.search(text):
        return True
    if CITATION_HINT_RE.search(text):
        return True
    return False

def extract_title_and_citation(text: str):
    """
    Return (title, citation) where:
      - title is column B style: case name trimmed, without trailing citations/pages
      - citation is first [...] if present; otherwise empty
    """
    # Grab the first [....] block as a 'citation' if present
    citation = ""
    m = re.search(r"\[.*?]", text)
    if m:
        citation = m.group(0).strip()

    # Strip any trailing ' , digits-digits' etc (page/range noise)
    cleaned = re.sub(r"[,\s]*\d+(?:-\d+)?(?:,\s*\d+(?:-\d+)?)*\s*$", "", text).strip()

    # If there is a colon prefix (some lines are "N: …"), remove it
    cleaned = re.sub(r"^[A-Za-z]\s*:\s*", "", cleaned).strip()

    # If the text begins with the citation, keep the part before it as title; else take until citation
    if citation:
        parts = cleaned.split(citation, 1)
        title = parts[0].strip().rstrip(",;")
    else:
        # If no citation, still try to trim at the end of a case-like bit (up to any bracket)
        title = re.split(r"\s*\[", cleaned, 1)[0].strip().rstrip(",;")

    # Final tiny tidy
    title = re.sub(r"\s+", " ", title)

    return title, citation

def slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

def main():
    ap = argparse.ArgumentParser(description="Extract case titles from LTJ(LTK).lines.json")
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json (or LTK.lines.json)")
    ap.add_argument("--start", type=int, required=True, help="Start line number (inclusive)")
    ap.add_argument("--end", type=int, required=True, help="End line number (inclusive)")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g., data/cases.csv)")
    args = ap.parse_args()

    lines_path = Path(args.ltj_lines)
    if not lines_path.exists():
        print(f"ERROR: lines JSON not found: {lines_path}", file=sys.stderr)
        sys.exit(1)

    with lines_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # data is expected to be a list of { "line_no": int, "text": str }
    if not isinstance(data, list):
        print("ERROR: lines JSON is not a list", file=sys.stderr)
        sys.exit(1)

    # Filter the requested range
    subset = [row for row in data if isinstance(row, dict) and
              "line_no" in row and "text" in row and
              args.start <= int(row["line_no"]) <= args.end]

    matched = []
    for row in subset:
        text = str(row["text"]).strip()
        if not text:
            continue
        if not looks_like_case(text):
            continue
        title, citation = extract_title_and_citation(text)
        if not title or " v " not in title.lower():
            # If too aggressive, relax this guard.
            continue
        source_line = int(row["line_no"])
        # case_id can be refined later; for now: slug(title) + optional year from citation
        year = ""
        m = re.search(r"\[(\d{4})]", citation)
        if m:
            year = m.group(1)
        case_id = slugify(title + ("_" + year if year else ""))

        matched.append({
            "case_id": case_id,
            "title": title,
            "citation": citation,
            "jurisdiction": "",     # filled later by enrichment
            "url": "",               # filled later by enrichment
            "source_line": source_line
        })

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["case_id", "title", "citation", "jurisdiction", "url", "source_line"])
        w.writeheader()
        for row in matched:
            w.writerow(row)

    print(f"Wrote {len(matched)} rows to {out_path}")
    # Small debug preview
    for row in matched[:5]:
        print("  •", row["title"], row["citation"], f"(line {row['source_line']})")

if __name__ == "__main__":
    main()
