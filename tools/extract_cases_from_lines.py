#!/usr/bin/env python3
"""
Extract structured case rows from LTJ-ui/out/LTJ.lines.json (book index).

Outputs CSV with columns:
  line_no,title,year,citation,pinpoints

Design goals:
- Preserve line_no for back-reference.
- Handle "v" cases and "Re/In re/Matter of" forms.
- Extract year from [1991] / (1991) / plain 1991 when part of the citation.
- Clean trailing pinpoint ranges like '12-23, 14-16' out of citation (keep in 'pinpoints').
- Skip folio roman numerals, blank lines, and "Table of Cases" headers.
- Emit a JSON skip report for anything ambiguous (so we never silently drop data).
"""

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

ROMAN_RE = re.compile(r"^(?i)[ivxlcdm]+\.?$")  # e.g. xxxvii
PAGE_RANGE_LIST_RE = re.compile(r"""^
    (?:
        \d+\s*(?:-\s*\d+)?      # 12 or 12-23
    )
    (?:\s*,\s*
        \d+\s*(?:-\s*\d+)?      # , 14-16 ...
    )*
\s*$""", re.X)

# Trailing comma-separated pinpoint ranges after a citation:
TRAILING_PINPOINTS_RE = re.compile(r"""
    (?:                           # optional leading comma/space
        [\s,]+
    )
    (?P<pinpoints>
        \d+(?:\s*-\s*\d+)?        # 12 or 12-23
        (?:\s*,\s*\d+(?:\s*-\s*\d+)?)*   # , 14-16, ...
    )
    \s*$
""", re.X)

# Extract a bracketed/parenthesized 4-digit year anywhere:
YEAR_RE = re.compile(r"[[(](?P<year>\d{4})[\])]|(?<=\s)(?P<bare>\d{4})(?=\s)")

# Recognize "Re / In re / In the matter of / Matter of" forms:
RE_FORMS = re.compile(
    r"^(?P<title>.*?\b(?:Re|In\s+re|In\s+the\s+Matter\s+of|Matter\s+of)\b.*?)"
    r"(?:\s*,\s*)?"
    r"(?P<rest>.*)$",
    re.IGNORECASE,
)

# A pragmatic citation catcher: assume the earliest reporter token marks start of the citation.
# We avoid gobbling the trailing pinpoints (we strip them later).
REPORTER_START = re.compile(
    r"""
    (?:\bJLR\b|\bWLR\b|\bAC\b|\bQB\b|\bCh\b|\bFam\b|\bBCLC\b|\bAll\s*ER\b|
     \bEWHC\b|\bEWCA\b|\bUKSC\b|\bUKPC\b|\bJCA\b|\bJRC\b|\bJRCM\b|\bPC\b|\bHL\b)
    """,
    re.IGNORECASE | re.VERBOSE,
)

TABLE_HEADER_RE = re.compile(r"^\s*Table of Cases\s*$", re.IGNORECASE)

def looks_like_header_or_noise(text: str) -> bool:
    t = text.strip()
    if not t:
        return True
    if TABLE_HEADER_RE.match(t):
        return True
    if ROMAN_RE.match(t):  # folio roman numerals like xxxvii
        return True
    if PAGE_RANGE_LIST_RE.match(t):  # bare page-range lines like "12-23, 14-16"
        return True
    # Single-letter A/B/C headings (rare; keep conservative)
    if len(t) == 1 and t.isalpha():
        return True
    return False

def split_title_and_rest(text: str) -> Tuple[str, str]:
    """
    Try to split the line into a 'title' and 'rest' (which should contain year/citation).
    Handles:
      - "Cocks v Chapman [1990] 1 AC 123 ..."
      - "Clapham, Re [1990] JLR 100 ..."
      - "In re Estate of ..."  "(1991) ..."
    If no reporter is found, we still return a best-effort title and empty rest.
    """
    t = text.strip().rstrip(",")  # strip trailing comma noise
    # Handle Re/In re forms first
    m = RE_FORMS.match(t)
    if m:
        title = m.group("title").strip().rstrip(",")
        rest = m.group("rest").strip()
        return title, rest

    # Else, try to find a reporter token as the start of the citation
    m2 = REPORTER_START.search(t)
    if m2:
        # title is before the reporter token
        title = t[:m2.start()].strip().rstrip(",")
        rest = t[m2.start():].strip()
        return title, rest

    # Fall back: look for a bracketed/parenthesized year; split there
    m3 = YEAR_RE.search(t)
    if m3:
        split_at = m3.start()
        title = t[:split_at].strip().rstrip(",")
        rest = t[split_at:].strip()
        return title, rest

    # As a last resort, treat the whole thing as a title; rest empty
    return t, ""

def extract_year(rest: str) -> Optional[str]:
    m = YEAR_RE.search(rest)
    if not m:
        return None
    return m.group("year") or m.group("bare")

def clean_citation_and_pinpoints(rest: str) -> Tuple[str, str]:
    """
    Separate the citation body from trailing pinpoints: "JLR 103, 12-23, 14-16"
    returns ("JLR 103", "12-23, 14-16")
    If no reporter seen, return ("", "")
    """
    if not rest:
        return "", ""

    # Find a plausible reporter start; if none, give up
    mrep = REPORTER_START.search(rest)
    if not mrep:
        # Sometimes the citation can be just "[1991] 1 AC 123"
        # Try from the year to the end as citation:
        my = YEAR_RE.search(rest)
        if my:
            candidate = rest[my.start():].strip()
        else:
            candidate = rest.strip()
    else:
        candidate = rest[mrep.start():].strip()

    # Peel trailing pinpoints
    pinpoints = ""
    mtrail = TRAILING_PINPOINTS_RE.search(candidate)
    if mtrail:
        pinpoints = mtrail.group("pinpoints").strip()
        candidate = candidate[: mtrail.start()].rstrip(",; ").strip()

    # Remove leading year wrappers like "[1991]" or "(1991)" if the citation starts with it
    # but keep them if they’re part of a standard report cite.
    # We’ll allow them; the main cleanse is to avoid dangling commas.
    return candidate, pinpoints

def normalize_title(title: str) -> str:
    """
    Light normalization only:
    - collapse whitespace
    - preserve punctuation like apostrophes and commas (don’t over-normalize)
    - ensure "v" stays as " v " (not mangled)
    """
    t = " ".join(title.split())
    # Normalize various ' v ' variants (avoid touching 'Re')
    t = re.sub(r"\s+v\s+", " v ", t)
    return t

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--out", required=True, help="Path to write CSV (cases)")
    ap.add_argument("--skip-report", default=None, help="Optional JSON for skipped/ambiguous lines")
    ap.add_argument("--start-line", type=int, default=None, help="Inclusive start line_no")
    ap.add_argument("--end-line", type=int, default=None, help="Inclusive end line_no")
    args = ap.parse_args()

    src = Path(args.input)
    data = json.loads(src.read_text(encoding="utf-8"))

    rows = []
    skipped: Dict[int, Dict[str, str]] = {}

    for obj in data:
        line_no = obj.get("line_no")
        text = obj.get("text", "")

        if args.start_line is not None and line_no < args.start_line:
            continue
        if args.end_line is not None and line_no > args.end_line:
            continue

        if looks_like_header_or_noise(text):
            continue

        title_raw, rest = split_title_and_rest(text)
        title = normalize_title(title_raw)

        # Heuristic sanity: if the “title” is clearly not a case (e.g., starts with "v." or is 1–2 words),
        # we’ll still keep it (index entries can be terse), but make sure we don’t create garbage.
        year = extract_year(rest) or ""

        citation, pinpoints = clean_citation_and_pinpoints(rest)

        # If we have absolutely no citation and the title looks like a folio/garbage, flag it
        if not title or title.lower() in {"table of cases"}:
            skipped[line_no] = {"text": text, "reason": "title-empty-or-header"}
            continue

        # A minimal check: ensure title contains something other than numbers/punctuation
        if not re.search(r"[A-Za-z]", title):
            skipped[line_no] = {"text": text, "reason": "no-alpha-in-title"}
            continue

        # Record row
        rows.append({
            "line_no": line_no,
            "title": title,
            "year": year,
            "citation": citation,
            "pinpoints": pinpoints,
        })

    # Write CSV
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["line_no", "title", "year", "citation", "pinpoints"])
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: r["line_no"]))

    # Optional skip report
    if args.skip_report:
        Path(args.skip_report).write_text(json.dumps(skipped, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Wrote {len(rows)} rows to {outp}")
    if args.skip_report:
        print(f"Skipped {len(skipped)} lines → {args.skip_report}")

if __name__ == "__main__":
    main()
