# tools/enrich_firstN.py
"""
Update the first N cases in data/cases.csv with a direct judgment (or PDF) URL.
Heartbeat prints each case so you can abort if things look off.

Usage:
  python tools/enrich_firstN.py --input data/cases.csv --out data/cases.csv --limit 10 --start 0
"""
import csv, argparse, sys
from util import sleep_jitter, pick_best_url

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out",   required=True)
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--sleep-min", type=float, default=1.0)
    ap.add_argument("--sleep-max", type=float, default=2.0)
    args = ap.parse_args()

    with open(args.input, newline='', encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # ensure url column exists
    fieldnames = list(rows[0].keys()) if rows else ["Title","Year","Citation","url"]
    if "url" not in fieldnames:
        fieldnames.append("url")

    end = min(len(rows), args.start + args.limit)
    processed = 0
    for i in range(args.start, end):
        r = rows[i]
        title = r.get("Title","").strip()
        citation = r.get("Citation","").strip()
        if not title:
            print(f"[{i+1}/{end}] skip (empty title)")
            continue
        print(f"[{i+1}/{end}] seeking | {title} | {citation}")
        url = pick_best_url(title, citation)
        if url:
            r["url"] = url
            print(f"  -> OK {url}")
        else:
            print("  -> no verified match")
        processed += 1
        sleep_jitter(args.sleep_min, args.sleep_max)

    # write back (overwrite is fine for preview too)
    with open(args.out, "w", newline='', encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # ensure every row has url column
            if "url" not in r: r["url"] = ""
            w.writerow(r)

    print(f"Done. Updated {processed} rows into {args.out}")

if __name__ == "__main__":
    sys.exit(main())
