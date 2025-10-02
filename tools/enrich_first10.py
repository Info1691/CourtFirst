# tools/enrich_first10.py
"""
Process the first N rows of data/cases.csv and attach a DIRECT judgment URL.
Artifacts written to out/preview-enrichment/:
  - cases_preview.csv (Title,Citation,url)
  - urls_preview.json  (per-row diagnostics)
  - skipped_preview.json (rows without verified direct link)
Includes a heartbeat line per case and aborts after too many consecutive failures.
"""
import csv
import json
import os
import sys
from typing import List, Dict, Any, Tuple

from util import sleep_jitter, pick_best_url

INPUT = os.environ.get("INPUT", "data/cases.csv")
OUTDIR = os.environ.get("OUTDIR", "out/preview-enrichment")
LIMIT = int(os.environ.get("LIMIT", "10"))
ABORT_AFTER = int(os.environ.get("ABORT_AFTER", "8"))
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "2.0"))
SLEEP_MAX = float(os.environ.get("SLEEP_MAX", "3.5"))

def read_cases_csv(path: str) -> List[Dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return list(rdr)

def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_cases_preview(rows: List[Tuple[str, str, str]], outpath: str) -> None:
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Title", "Citation", "url"])
        w.writerows(rows)

def write_json(obj: Any, outpath: str) -> None:
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def main() -> int:
    rows = read_cases_csv(INPUT)
    ensure_outdir(OUTDIR)

    ok_rows: List[Tuple[str, str, str]] = []
    diagnostics: Dict[int, Any] = {}
    skipped: Dict[int, Any] = {}

    total = min(LIMIT, len(rows))
    consec_fail = 0

    print(f"[enrich_first10] input={INPUT} limit={LIMIT} -> total={total}", flush=True)

    for idx in range(total):
        r = rows[idx]
        # Accept either Title/Citation or title/citation (robust to header case)
        title = (r.get("Title") or r.get("title") or "").strip()
        citation = (r.get("Citation") or r.get("citation") or "").strip()

        best, diags = pick_best_url(title, citation)
        if best:
            ok_rows.append((title, citation, best))
            diagnostics[idx] = {"title": title, "citation": citation, "primary": best, **diags}
            consec_fail = 0
        else:
            skipped[idx] = {"title": title, "citation": citation, "reason": "no-verified-direct-link", **diags}
            consec_fail += 1

        done = idx + 1
        print(
            f"[heartbeat] case {done}/{total} | ok:{len(ok_rows)} skip:{len(skipped)} | title='{title[:60]}'",
            flush=True,
        )

        if consec_fail >= ABORT_AFTER:
            print(f"!! aborting: {consec_fail} consecutive failures (max {ABORT_AFTER})", flush=True)
            break

        sleep_jitter(SLEEP_MIN, SLEEP_MAX)

    write_cases_preview(ok_rows, os.path.join(OUTDIR, "cases_preview.csv"))
    write_json(diagnostics, os.path.join(OUTDIR, "urls_preview.json"))
    write_json(skipped, os.path.join(OUTDIR, "skipped_preview.json"))

    print(
        f"Done. Success={len(ok_rows)} Skipped={len(skipped)}  Artifacts -> {OUTDIR}",
        flush=True,
    )
    return 0 if ok_rows else 2

if __name__ == "__main__":
    sys.exit(main())
