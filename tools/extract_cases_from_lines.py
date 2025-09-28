# CourtFirst/tools/extract_cases_from_lines.py
import argparse, csv, json, re
from pathlib import Path

JRC = re.compile(r"\[(?P<year>\d{4})\]\s*(?P<reporter>JRC)\s*(?P<num>\d{1,4})\b")
JCA = re.compile(r"\[(?P<year>\d{4})\]\s*(?P<reporter>JCA)\s*(?P<num>\d{1,4})\b")
JLR = re.compile(r"\[(?P<year>\d{4})\]\s*(?P<reporter>JLR)\s*(?P<num>\d{1,4})\b")
TITLE = re.compile(r"(?P<title>\b[A-Z][A-Za-z0-9&.'\- ]+?\s+v\s+[A-Z][A-Za-z0-9&.'\- ]+)\b")

def slug(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", s.strip())
    return re.sub(r"_+", "_", s).strip("_")

def read_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ltj_lines", required=True, help="Path to LTJ-ui/out/LTJ.lines.json")
    ap.add_argument("--out", required=True, help="Path to write cases.csv")
    ap.add_argument("--range_start", type=int, default=1276)
    ap.add_argument("--range_end", type=int, default=3083)
    args = ap.parse_args()

    lines = read_lines(args.ltj_lines)

    # Heuristics: LTJ.lines.json is usually a list of objects:
    # { "pid": "...", "line": <int>, "text": "..." }
    # If it’s not, we fail loudly so you can adjust.
    if not isinstance(lines, list) or not lines or "text" not in lines[0]:
        raise RuntimeError("Unexpected LTJ.lines.json structure; expected list of {pid,line,text}")

    start, end = args.range_start, args.range_end
    picked = [row for row in lines if isinstance(row.get("line"), int) and start <= row["line"] <= end]

    seen = set()
    rows = []

    for row in picked:
        txt = row.get("text", "") or ""
        pid = row.get("pid", "")
        line_no = row.get("line", None)

        found = False

        # Prefer exact JRC/JCA/JLR citations
        for rx in (JRC, JCA, JLR):
            for m in rx.finditer(txt):
                d = m.groupdict()
                year = d.get("year")
                reporter = d.get("reporter")
                num = d.get("num")
                cid = f"{reporter}_{year}_{num}"
                key = (cid, line_no)
                if key in seen:
                    continue
                seen.add(key)

                title = None
                t = TITLE.search(txt)
                if t:
                    title = t.group("title").strip()

                rows.append({
                    "case_id": cid,
                    "jurisdiction": "Jersey",
                    "title": title or "",
                    "year": year or "",
                    "reporter": reporter or "",
                    "report_no": num or "",
                    "pid": pid,
                    "line_no": line_no,
                    "raw": txt.strip()
                })
                found = True

        if found:
            continue

        # If no formal reporter found, try title-only lines
        t = TITLE.search(txt)
        if t:
            title = t.group("title").strip()
            cid = slug(title)[:80]
            key = (cid, line_no)
            if key not in seen:
                seen.add(key)
                rows.append({
                    "case_id": cid,
                    "jurisdiction": "Jersey",
                    "title": title,
                    "year": "",
                    "reporter": "",
                    "report_no": "",
                    "pid": pid,
                    "line_no": line_no,
                    "raw": txt.strip()
                })

    # Deduplicate by case_id preferring JRC/JCA over slug titles
    by_id = {}
    for r in rows:
        cid = r["case_id"]
        cur = by_id.get(cid)
        if cur is None:
            by_id[cid] = r
        else:
            # Keep the one with reporter if current has none
            if not cur.get("reporter") and r.get("reporter"):
                by_id[cid] = r

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with open(outp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "case_id","jurisdiction","title","year","reporter","report_no","pid","line_no","raw"
        ])
        w.writeheader()
        for r in sorted(by_id.values(), key=lambda x: (x["year"] or "0000", x["case_id"])):
            w.writerow(r)

    print(f"Wrote {len(by_id)} cases → {outp}")

if __name__ == "__main__":
    main()
