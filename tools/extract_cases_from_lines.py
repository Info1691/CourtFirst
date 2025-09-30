# tools/extract_cases_from_lines.py
import json, os, re, csv, sys
from util import write_csv

IN_LINES = os.environ.get("LTJ_LINES", "LTJ-ui/out/LTJ.lines.json")
OUT_CSV  = os.environ.get("OUT_CSV", "data/cases.csv")
START = int(os.environ.get("START_LINE", "1276"))
END   = int(os.environ.get("END_LINE", "3083"))

PINPOINT = re.compile(r"(,\s*\d+(?:-\d+)?(?:,\s*\d+(?:-\d+)?)*)\s*$")  # trailing “, 12-23, 45-47”
YEAR = re.compile(r"\[(\d{4})\]|\((\d{4})\)")

def clean_title(s:str)->str:
    s = PINPOINT.sub("", s).strip()
    return s

def guess_year(s:str)->str:
    m = YEAR.search(s or "")
    if not m: return ""
    return m.group(1) or m.group(2) or ""

def main():
    with open(IN_LINES, encoding="utf-8") as f:
        lines = json.load(f)

    rows=[]
    for item in lines:
        ln = item.get("line_no") or item.get("line") or item.get("line_no".upper())
        txt = item.get("text","").strip()
        if not isinstance(ln, int): continue
        if ln < START or ln > END: continue
        if not txt: continue

        title = clean_title(txt)
        yr = guess_year(txt)
        rows.append({"Title": title, "Year": yr, "Citation":"", "Jurisdiction":"", "Source_Line": ln})

    write_csv(OUT_CSV, rows, ["Title","Year","Citation","Jurisdiction","Source_Line"])
    print(f"Wrote {len(rows)} rows to {OUT_CSV}")

if __name__=="__main__":
    main()
