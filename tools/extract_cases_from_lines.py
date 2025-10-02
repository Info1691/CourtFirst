#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract a clean cases.csv from LTJ-ui/out/LTJ.lines.json.

Inputs:
  --ltj-lines PATH     Path to LTJ.lines.json (from LTJ-ui).
  --out PATH           Output CSV path (will be created/overwritten).
  --start-line INT     Inclusive line_no start bound (optional).
  --end-line INT       Inclusive line_no end bound (optional).

Output CSV columns (exactly):
  Title,Year,Citation,Jurisdiction,Line

Notes:
- Keeps exact case titles (no rephrasing), only trims obvious noise.
- Skips roman numeral folios (e.g., 'xxxvii'), “Table of Cases”, blanks,
  and pure page-range rows like '12-23' or '7-34'.
- Supports titles like:
    'A v B [1990] 1 AC 109'
    'A v. B (1990) 1 WLR 123'
    'Re A Settlement, [2009] JRC 1253'
    'In re Estate of X (1975) ...'
    'X, Re [1900] ...'
- Year is taken from first [...]/(...) group containing a 4-digit year.
- Citation is the remainder of the report text following that year; if
  no bracketed/parenthesized year, we try to capture obvious report tokens.
- Jurisdiction is left blank for now (to be inferred next stage).
"""

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Optional, Tuple

# --- Regexes (careful with flags placement) ---

# Roman numerals line (folio pages like xxxvii). Case-insensitive.
ROMAN_RE = re.compile(r"(?i)^[ivxlcdm]+\.?\s*$")

# Pure page-range rows like '12-23', '7-34', '9-9,10-9, 10-12' etc.
PAGE_RANGE_RE = re.compile(r"^\s*(\d+\s*(?:-\s*\d+)?)(?:\s*,\s*\d+\s*(?:-\s*\d+)?)*\s*$")

# “Table of Cases” header or obvious section labels we should skip
SKIP_LABELS = {
    "table of cases",
    "table of authorities",
    "cases after pitt v holt",
}

# Detect a 4-digit year inside [....] or (....)
BRACKETED_YEAR_RE = re.compile(r"(?P<all>[\[\(]\s*(?P<year>1[89]\d{2}|20\d{2}|2100)\s*[\]\)])")

# Some report tokens when no bracketed year exists (we keep these as citation if present)
REPORT_TOKEN_RE = re.compile(
    r"\b("
    r"JRC|JLR|JCA|WLR|AC|QB|Ch|Fam|EWCA|EWHC|UKSC|UKPC|PC|All\s*ER|Lloyd'?s\s*Rep|BCLC|"
    r"JCA|CA|HL|PC|QBD|KB|CP|SCC|SCR"
    r")\b.*", re.IGNORECASE
)

# Titles that begin with “Re …”, “In re …”, “In the matter of …”
RE_BEGIN_RE = re.compile(r"(?i)^\s*(in\s+the\s+matter\s+of|in\s+re|re)\b")

# Titles of the form 'X, Re' → normalize title part as 'Re X'
COMMA_RE_RE = re.compile(r"(?i)^\s*(?P<name>.+?)\s*,\s*re\b")


def is_skip_line(text: str) -> bool:
    t = text.strip()
    if not t:
        return True
    if ROMAN_RE.match(t):
        return True
    if PAGE_RANGE_RE.match(t):
        return True
    t_low = t.lower()
    if t_low in SKIP_LABELS:
        return True
    # tolerate lines that are just punctuation or a single letter
    if len(t) <= 2 and not any(ch.isalnum() for ch in t):
        return True
    return False


def split_title_year_citation(text: str) -> Tuple[str, str, str]:
    """
    Extract (Title, Year, Citation) from a case line.
    We **do not** synthesize new content; we only slice the given text.

    Strategy:
      1) Find the first ([YYYY]) or ([YYYY]) occurrence; that gives Year.
      2) Title = text up to that bracket start (trim trailing commas/whitespace).
      3) Citation = remainder after the bracketed year token.
      4) If no bracketed year, Title = up to first report token (if any),
         and Citation = that token onward. Year = ''.
      5) Normalize 'X, Re' into 'Re X' (only the leading pattern).
    """
    s = text.strip()

    # Normalize a leading 'X, Re' to 'Re X' (only if it starts the line)
    m_comma_re = COMMA_RE_RE.match(s)
    if m_comma_re:
        s = f"Re {m_comma_re.group('name').strip()}"

    m_year = BRACKETED_YEAR_RE.search(s)
    if m_year:
        year = m_year.group("year")
        # Title is everything before the bracket start
        title_raw = s[:m_year.start()].rstrip(" ,;:-\u2013\u2014").strip()
        # Citation is everything after the bracketed year group
        citation = s[m_year.end():].strip(" ,;:-\u2013\u2014")
        # If citation is empty but there are visible report tokens before year,
        # move them to citation (rare, but guard anyway).
        if not citation:
            before = s[:m_year.start()]
            m_rep_before = REPORT_TOKEN_RE.search(before)
            if m_rep_before:
                # Title should end before the token
                title_raw = before[:m_rep_before.start()].rstrip(" ,;:-\u2013\u2014").strip()
                citation = before[m_rep_before.start():].strip(" ,;:-\u2013\u2014")
        title = title_raw
        return (title, year, citation)

    # No bracketed/parenthesized year: try to split on first obvious report token
    m_rep = REPORT_TOKEN_RE.search(s)
    if m_rep:
        title = s[:m_rep.start()].rstrip(" ,;:-\u2013\u2014").strip()
        citation = s[m_rep.start():].strip(" ,;:-\u2013\u2014")
        return (title, "", citation)

    # Otherwise we keep the whole thing as title (no fabricated fields)
    return (s.rstrip(" ,;:-\u2013\u2014"), "", "")


def looks_like_case_title(title: str) -> bool:
    """
    Heuristic to avoid headers that slipped through:
    - must contain letters
    - must not be just 'v.' or a lone word like 'v.'
    - allow forms starting with 'Re', 'In re', 'In the matter of'
    - typical ' X v Y ' detection but NOT mandatory
    """
    t = title.strip()
    if len(t) < 2:
        return False
    if not any(ch.isalpha() for ch in t):
        return False
    if t.lower() in {"v", "v.", "re"}:
        return False
    return True


def extract_from_lines(lines_json_path: Path,
                       out_csv_path: Path,
                       start_line: Optional[int],
                       end_line: Optional[int]) -> int:
    data = json.loads(Path(lines_json_path).read_text(encoding="utf-8"))

    # Expect a list of {"line_no": int, "text": str}
    rows_out = []
    for item in data:
        try:
            line_no = int(item.get("line_no"))
        except Exception:
            continue
        text = (item.get("text") or "").strip()

        if start_line is not None and line_no < start_line:
            continue
        if end_line is not None and line_no > end_line:
            continue
        if is_skip_line(text):
            continue

        title, year, citation = split_title_year_citation(text)

        # Additional gentle filters
        if not looks_like_case_title(title):
            continue

        # Skip obvious chapter/section lines that slipped through
        low = title.lower()
        if any(kw in low for kw in ["litigation costs", "non-party cost orders",
                                    "responses and evidence", "documents disclosable"]):
            # these look like section headings, not cases
            continue

        rows_out.append({
            "Title": title,
            "Year": year,
            "Citation": citation,
            "Jurisdiction": "",   # populate later
            "Line": line_no,
        })

    # Write CSV
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with out_csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Title", "Year", "Citation", "Jurisdiction", "Line"])
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    return len(rows_out)


def main():
    ap = argparse.ArgumentParser(description="Extract cases.csv from LTJ.lines.json")
    ap.add_argument("--ltj-lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--out", required=True, help="Output CSV path (will be overwritten)")
    ap.add_argument("--start-line", type=int, default=None, help="Inclusive start line_no")
    ap.add_argument("--end-line", type=int, default=None, help="Inclusive end line_no")
    args = ap.parse_args()

    n = extract_from_lines(Path(args.ltj_lines),
                           Path(args.out),
                           args.start_line,
                           args.end_line)
    print(f"✅ Wrote {n} rows to {args.out}")


if __name__ == "__main__":
    main()
