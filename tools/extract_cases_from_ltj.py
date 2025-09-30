#!/usr/bin/env python3
import argparse, json, re, csv, sys
from pathlib import Path

CASE_ROW = ["case_id","title","citation","year","jurisdiction","url","source_line"]

# Patterns:
#  - Lines look like: "A v B [2012] JRC 101, 12-23"
#  - Or "Smith v Jones [1995] 1 WLR 234"
#  - We ignore bare page lists like "12-23" etc.
re_pages_tail = re.compile(r'\s*,\s*\d{1,3}(?:-\d{1,3})?(?:\s*,\s*\d{1,3}(?:-\d{1,3})?)*\s*$')
re_year = re.compile(r'\[(\d{4})\]')
# For “looks like” a case: has a bracketed year and at least one capital word + " v " or "Re " etc.
re_looks_case = re.compile(r'(?:\bRe\b|\bv\b|\bIn re\b|\bR\b)\s', re.I)

def looks_like_case(text:str)->bool:
    if not re_year.search(text): 
        return False
    if re_looks_case.search(text):
        return True
    # Some lists include single-party titles: accept if there's a bracket + law report token
    tokens = ['JRC','JLR','EWHC','EWCA','UKSC','UKPC','WLR','All ER','AC','QB','Ch','Fam','BCLC','Lloyd\'s Rep']
    return any(t in text for t in tokens)

def strip_trailing_pages(text:str)->str:
    return re_pages_tail.sub('', text).strip()

def split_title_citation(text:str):
    """
    Split at the first bracketed year to separate title vs citation.
    """
    m = re_year.search(text)
    if not m:
        return text.strip(), "", None
    year = m.group(1)
    idx = m.start()
    title = text[:idx].rstrip(" ,")
    citation = text[idx:].strip()
    return title.strip(), citation.strip(), year

def parse_cases(lines, start, end, max_n=0):
    out = []
    n = 0
    for obj in lines:
        ln = obj.get("line_no") or obj.get("line") or obj.get("no") or obj.get("lineNo")
        txt = obj.get("text","").strip()
        if ln is None or not (start <= int(ln) <= end):
            continue
        # skip pure page ranges like "12-23"
        if re.fullmatch(r'\d{1,3}(?:-\d{1,3})?(?:,\s*\d{1,3}(?:-\d{1,3})?)*', txt):
            continue
        if not looks_like_case(txt):
            continue
        cleaned = strip_trailing_pages(txt)
        title, citation, year = split_title_citation(cleaned)
        if not title or not citation:
            # keep, but mark citation empty if weird line; we want zero drops
            pass
        # guess jurisdiction (very light; empty if unsure)
        juris = ""
        if "JRC" in citation or "JLR" in citation:
            juris = "Jersey"
        case_id = f"LTJ_{ln}"
        out.append({
            "case_id": case_id,
            "title": title,
            "citation": citation,
            "year": year or "",
            "jurisdiction": juris,
            "url": "",
            "source_line": ln
        })
        n += 1
        if max_n and n >= max_n:
            break
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ltj-lines", required=True)
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max", type=int, default=0)
    args = ap.parse_args()

    with open(args.ltj_lines, "r", encoding="utf-8") as f:
        data = json.load(f)

    # LTJ.lines.json is usually an array of objects
    if isinstance(data, dict) and "lines" in data:
        lines = data["lines"]
    elif isinstance(data, list):
        lines = data
    else:
        print("Unrecognised LTJ lines structure", file=sys.stderr)
        sys.exit(1)

    rows = parse_cases(lines, args.start, args.end, max_n=args.max)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CASE_ROW)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Wrote {len(rows)} rows -> {args.out}")

if __name__ == "__main__":
    main()
