# tools/fetch_case_text.py
import os, csv, argparse, hashlib
from pdfminer.high_level import extract_text as pdf_extract_text
import requests

HDRS = {"User-Agent":"CourtFirst/1.0 (+GitHub Actions; requests)"}

def sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()

def fname_safe(s: str) -> str:
    return "".join(c for c in s if c.isalnum() or c in ("-","_","+"," ")).strip().replace(" ","_")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cases", required=True, help="data/cases.csv")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    os.makedirs("cache/pdfs", exist_ok=True)
    os.makedirs("cache/text", exist_ok=True)

    with open(args.cases, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    count = 0
    for r in rows:
        if count >= args.limit: break
        pdf_url = (r.get("pdf_url") or "").strip()
        title = (r.get("Title") or "").strip()
        if not pdf_url or not title:
            continue
        base = fname_safe(title)[:80]
        pdf_path = f"cache/pdfs/{base}.pdf"
        txt_path = f"cache/text/{base}.txt"

        try:
            b = requests.get(pdf_url, headers=HDRS, timeout=35).content
            if len(b) < 500:  # sanity
                continue
            with open(pdf_path, "wb") as pf:
                pf.write(b)
            txt = pdf_extract_text(pdf_path) or ""
            with open(txt_path, "w", encoding="utf-8") as tf:
                tf.write(txt)
            count += 1
            print(f"OK: {title}")
        except Exception as e:
            print(f"FAIL: {title} -> {e}")

    print(f"Fetched & extracted text for {count} PDF(s).")

if __name__ == "__main__":
    main()
