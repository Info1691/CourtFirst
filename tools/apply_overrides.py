# tools/apply_overrides.py
import csv, argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases", required=True, help="data/cases.csv")
    ap.add_argument("--overrides", required=True, help="data/manual_overrides.csv")
    args = ap.parse_args()

    with open(args.cases, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    key = lambda r: (r.get("Title","").strip().lower(), r.get("Citation","").strip().lower(), r.get("Year","").strip())

    o_map = {}
    with open(args.overrides, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            o_map[key(r)] = r

    # ensure columns exist
    fieldnames = list(rows[0].keys()) if rows else ["Title","Citation","Year"]
    for col in ("page_url","pdf_url","verified_source","source_hash","last_checked"):
        if col not in fieldnames:
            fieldnames.append(col)

    changed = 0
    for r in rows:
        k = key(r)
        if k in o_map:
            o = o_map[k]
            if o.get("page_url"):
                r["page_url"] = o["page_url"]
            if o.get("pdf_url"):
                r["pdf_url"] = o["pdf_url"]
            r["verified_source"] = r.get("verified_source") or "manual"
            changed += 1

    with open(args.cases, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Applied {changed} manual override(s).")

if __name__ == "__main__":
    main()
