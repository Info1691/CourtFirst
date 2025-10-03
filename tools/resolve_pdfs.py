# tools/resolve_pdfs.py
import csv, argparse, time
from datetime import datetime
from typing import List, Dict, Tuple, Optional

from util_sources import (
    sleep, looks_pdf_url, http_get_bytes, sha256_bytes,
    jerseylaw_find, bailii_find, web_pdf_fallback, verify_pdf_contains
)

def ensure_cols(fieldnames: List[str]) -> List[str]:
    for col in ("page_url","pdf_url","verified_source","source_hash","last_checked"):
        if col not in fieldnames:
            fieldnames.append(col)
    return fieldnames

def resolve_one(title: str, citation: str) -> Tuple[Dict[str,str], Optional[bytes]]:
    """Returns (meta, pdf_bytes|None). meta has: page_url, pdf_url, verified_source."""
    meta = {"page_url":"", "pdf_url":"", "verified_source":""}
    # 1) JerseyLaw
    page, pdf = jerseylaw_find(title, citation)
    if page or pdf:
        meta["verified_source"] = "jerseylaw"
        meta["page_url"] = page or meta["page_url"]
        if pdf and looks_pdf_url(pdf):
            try:
                b = http_get_bytes(pdf)
                if verify_pdf_contains(b, title, citation):
                    meta["pdf_url"] = pdf
                    return meta, b
            except Exception:
                pass
    sleep()

    # 2) BAILII
    page, pdf = bailii_find(title, citation)
    if page or pdf:
        meta["verified_source"] = meta["verified_source"] or "bailii"
        meta["page_url"] = meta["page_url"] or page or ""
        if pdf and looks_pdf_url(pdf):
            try:
                b = http_get_bytes(pdf)
                if verify_pdf_contains(b, title, citation):
                    meta["pdf_url"] = pdf
                    return meta, b
            except Exception:
                pass
    sleep()

    # 3) Web PDF fallback (verified)
    candidate = web_pdf_fallback(title, citation)
    if candidate and looks_pdf_url(candidate):
        try:
            b = http_get_bytes(candidate)
            if verify_pdf_contains(b, title, citation):
                meta["verified_source"] = meta["verified_source"] or "web-verified"
                meta["pdf_url"] = candidate
                return meta, b
        except Exception:
            pass

    # no PDF, but we might still have a page_url from primaries
    return meta, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="data/cases.csv")
    ap.add_argument("--out", required=True, help="data/cases.csv (overwrite)")
    ap.add_argument("--unresolved", default="reports/unresolved.csv")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=25)
    args = ap.parse_args()

    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    fieldnames = ensure_cols(list(rows[0].keys()) if rows else ["Title","Citation","Year"])
    end = min(len(rows), args.start + args.limit)

    # unresolved report
    unr: List[Dict[str,str]] = []
    ts = datetime.utcnow().isoformat(timespec="seconds")+"Z"

    for i in range(args.start, end):
        r = rows[i]
        title = (r.get("Title") or "").strip()
        citation = (r.get("Citation") or "").strip()
        if not title:
            continue

        print(f"[{i+1}/{end}] {title} | {citation}")
        meta, pdf_bytes = resolve_one(title, citation)

        # update row
        if meta.get("page_url"): r["page_url"] = meta["page_url"]
        if meta.get("pdf_url"):  r["pdf_url"]  = meta["pdf_url"]
        if meta.get("verified_source"): r["verified_source"] = meta["verified_source"]
        r["last_checked"] = ts

        # hash PDF if any
        if pdf_bytes:
            r["source_hash"] = sha256_bytes(pdf_bytes)
        else:
            # record unresolved if neither PDF nor page_url
            if not r.get("page_url") and not r.get("pdf_url"):
                unr.append({
                    "Title": title, "Citation": citation, "Year": r.get("Year",""),
                    "reason": "no-match", "checked": ts
                })

        time.sleep(0.5)  # polite

    # write back cases.csv
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # write unresolved report
    if unr:
        import os
        os.makedirs("reports", exist_ok=True)
        with open(args.unresolved, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["Title","Citation","Year","reason","checked"])
            w.writeheader()
            w.writerows(unr)

    print("Done.")

if __name__ == "__main__":
    main()
