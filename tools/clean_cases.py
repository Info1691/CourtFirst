#!/usr/bin/env python3
import argparse, csv, json, re
from pathlib import Path

re_multi_space = re.compile(r'\s+')
# Remove any residual ", 12-34" etc at end
re_pages_tail = re.compile(r'\s*,\s*\d{1,3}(?:-\d{1,3})?(?:\s*,\s*\d{1,3}(?:-\d{1,3})?)*\s*$')

def clean_title(title:str)->str:
    t = re_pages_tail.sub('', title or '').strip(' ,;\u00a0')
    t = re_multi_space.sub(' ', t).strip()
    return t

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    rpt = {"input": args.inp, "output": args.out, "changed": 0, "rows": 0}

    rows_out = []
    with open(args.inp, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rpt["rows"] += 1
            original = row["title"]
            new = clean_title(original)
            if new != original:
                rpt["changed"] += 1
            row["title"] = new
            rows_out.append(row)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        w.writeheader()
        w.writerows(rows_out)

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(rpt, f, indent=2)
    print(json.dumps(rpt, indent=2))

if __name__ == "__main__":
    main()
