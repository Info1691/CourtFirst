#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Attach verified public URLs to cases in data/cases.csv.

Policy:
- Prefer JerseyLaw for Jersey matters; prefer BAILII for UK/EW* / UKSC / EWCA / EWHC etc.
- Never fabricate. Only write a URL if verification passes.
- Verification = title similarity >= THRESH and (if a citation exists) the citation (or a strong token from it)
  appears somewhere in the page text.
- Everything else is left blank and logged as "skipped" with a reason.

Inputs:
  --input data/cases.csv
  --out   data/cases.csv     (safe, in-place update)
  --start 0 --end 500        (0-based, end exclusive; handy for small trials)
  --sleep-min 1.5 --sleep-max 3.5
  --batch-name "1..500"      (just for console/report labels)
  --emit-json                (write small previews under out/preview-enrichment/)
"""

import argparse
import csv
import html
import io
import json
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# --- Tunables ---------------------------------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) CourtFirstBot/0.1 (+contact: maintainer)",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DDG_HTML = "https://duckduckgo.com/html/"
TIMEOUT = 30
TITLE_SIM_THRESHOLD = 0.72  # conservative; we can tighten/loosen later
MAX_PER_SITE_RESULTS = 5    # scan first N results per domain attempt

ALLOWED_SITES = [
    "www.jerseylaw.je",
    "www.bailii.org",
]
# We’ll never *prefer* casemine; if a user later wants it, add here and a site-specific validator.

# simple “Jersey” detector by citation/jurisdiction strings
JERSEY_MARKERS = [
    "JLR", "JRC", "JCA", "Jersey"
]

UK_MARKERS = [
    "EWCA", "EWHC", "UKHL", "UKSC", "WLR", "All ER", "AC", "QB", "Ch", "Fam"
]

# ---------------------------------------------------------------------

def norm(s: str) -> str:
    """Lowercase, collapse spaces, strip punctuation (lightly), normalise v/vs, remove commas/dots/brackets."""
    s = html.unescape(s or "")
    s = s.replace(" vs ", " v ")
    s = s.replace(" v. ", " v ")
    s = re.sub(r"[\u2018\u2019\u201C\u201D]", "", s)  # quotes
    s = re.sub(r"[.,;:“”\"'()\[\]{}]", " ", s)
    s = re.sub(r"\s+", " ", s.lower()).strip()
    return s

def title_similarity(a: str, b: str) -> float:
    """Very small token-based similarity, no external deps."""
    ta = set(norm(a).split())
    tb = set(norm(b).split())
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    denom = len(ta | tb)
    return inter / denom if denom else 0.0

def looks_jersey(row: Dict[str, str]) -> bool:
    fields = " ".join([row.get("Citation",""), row.get("Jurisdiction",""), row.get("citation",""), row.get("jurisdiction","")])
    return any(m in fields for m in JERSEY_MARKERS)

def looks_uk(row: Dict[str, str]) -> bool:
    fields = " ".join([row.get("Citation",""), row.get("Jurisdiction",""), row.get("citation",""), row.get("jurisdiction","")])
    return any(m in fields for m in UK_MARKERS)

def ddg_query(q: str) -> List[str]:
    """Query DDG HTML endpoint; return list of result URLs (as absolute)."""
    params = {"q": q}
    r = requests.get(DDG_HTML, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.select("a.result__a"):
        href = a.get("href", "")
        if not href:
            continue
        # DDG returns direct links in HTML version
        out.append(href)
    return out

def fetch(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        # Very basic content discard for PDFs etc (we’ll skip them for now)
        ctype = r.headers.get("Content-Type","").lower()
        if "text/html" not in ctype:
            return None
        # Avoid binary surprise
        text = r.text
        if not text:
            return None
        return text
    except Exception:
        return None

def citation_token(cite: str) -> Optional[str]:
    """Extract a strong token to require on page (e.g., [2014], or neutral citation chunk)."""
    if not cite:
        return None
    # Prefer bracketed year
    m = re.search(r"\[(\d{4})\]", cite)
    if m:
        return m.group(0)  # keep the brackets
    # Else pull a neutral chunk like EWHC 1234 or JRC 045
    m = re.search(r"\b(EWHC|EWCA|UKSC|UKHL|JRC|JCA|JLR)\b[^\s,;]{0,10}", cite)
    if m:
        return m.group(0)
    return None

def choose_domains(row: Dict[str, str]) -> List[str]:
    # Policy: Jersey first if looks jersey; else UK -> BAILII first; else try both.
    if looks_jersey(row):
        return ["www.jerseylaw.je", "www.bailii.org"]
    if looks_uk(row):
        return ["www.bailii.org", "www.jerseylaw.je"]
    return ["www.jerseylaw.je", "www.bailii.org"]

@dataclass
class Verdict:
    url: Optional[str]
    reason: str

def verify_candidate(title: str, cite: str, html_text: str) -> bool:
    # Extract page title-ish text
    soup = BeautifulSoup(html_text, "html.parser")
    page_head = soup.find("title")
    h1 = soup.find("h1")
    candidates = []
    if page_head and page_head.text:
        candidates.append(page_head.text)
    if h1 and h1.text:
        candidates.append(h1.text)
    # Fallback: first strong or heading tag
    for sel in ["h2","strong","b"]:
        el = soup.find(sel)
        if el and el.text:
            candidates.append(el.text)
            break
    # Compare
    best = 0.0
    for c in candidates:
        best = max(best, title_similarity(title, c))
    if best < TITLE_SIM_THRESHOLD:
        return False
    tok = citation_token(cite)
    if tok:
        if tok not in soup.get_text(" ", strip=True):
            return False
    return True

def resolve_url_for_row(row: Dict[str,str]) -> Verdict:
    title = (row.get("Title") or row.get("title") or "").strip()
    cite  = (row.get("Citation") or row.get("citation") or "").strip()
    if not title:
        return Verdict(None, "no-title")

    domains = choose_domains(row)

    # Try domain-scoped exact title searches first, then fall back to generic.
    queries = []
    for d in domains:
        queries.append(f'"{title}" site:{d}')
        if cite:
            queries.append(f'"{title}" {cite} site:{d}')

    # last resort (domain-free)
    queries.append(f'"{title}" {cite}'.strip())
    if cite:
        queries.append(f'"{title}" {cite.split()[0]}')

    seen = set()
    for q in queries:
        try:
            hits = ddg_query(q)
        except Exception as e:
            return Verdict(None, f"ddg-error:{type(e).__name__}")

        # keep only allowed sites
        hits = [u for u in hits if any(u.startswith(f"https://{dom}") or u.startswith(f"http://{dom}") for dom in ALLOWED_SITES)]
        for u in hits[:MAX_PER_SITE_RESULTS]:
            if u in seen:
                continue
            seen.add(u)
            html_text = fetch(u)
            if not html_text:
                continue
            if verify_candidate(title, cite, html_text):
                return Verdict(u, "ok")
    return Verdict(None, "no-verified-match")

def read_csv(path: str) -> List[Dict[str,str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path: str, rows: List[Dict[str,str]]):
    # Preserve field order if present, else add url at end
    fields = list(rows[0].keys()) if rows else []
    if "url" not in fields:
        fields.append("url")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            if "url" not in r:
                r["url"] = ""
            w.writerow(r)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=-1, help="end (exclusive); -1 = to end")
    ap.add_argument("--sleep-min", type=float, default=1.5)
    ap.add_argument("--sleep-max", type=float, default=3.5)
    ap.add_argument("--batch-name", default="")
    ap.add_argument("--emit-json", action="store_true")
    args = ap.parse_args()

    rows = read_csv(args.input)
    n = len(rows)
    s = max(0, args.start)
    e = n if args.end < 0 else min(n, args.end)

    if s >= e:
        print(f"Nothing to do (slice {s}:{e} of {n})")
        return

    ok, skipped = {}, {}
    processed = 0

    # Ensure url column exists in memory
    for r in rows:
        if "url" not in r:
            r["url"] = ""

    for i in range(s, e):
        r = rows[i]
        title = (r.get("Title") or r.get("title") or "").strip()
        if not title:
            skipped[i] = {"title": title, "reason": "no-title"}
            continue

        # If already has a URL, keep it
        if r.get("url"):
            processed += 1
            if processed % 10 == 0:
                print(f"[{time.strftime('%H:%M:%S')}] progress: {processed}/{e-s} (kept existing)")
            continue

        v = resolve_url_for_row(r)
        if v.url:
            rows[i]["url"] = v.url
            ok[i] = {"title": title, "url": v.url}
        else:
            skipped[i] = {"title": title, "reason": v.reason}

        processed += 1
        if processed % 10 == 0:
            print(f"[{time.strftime('%H:%M:%S')}] progress: {processed}/{e-s}")

        # politeness sleep
        pause = random.uniform(args.sleep_min, args.sleep_max)
        time.sleep(pause)

    write_csv(args.out, rows)

    if args.emit_json:
        os.makedirs("out/preview-enrichment", exist_ok=True)
        with open("out/preview-enrichment/urls_preview.json", "w", encoding="utf-8") as f:
            json.dump(ok, f, indent=2, ensure_ascii=False)
        with open("out/preview-enrichment/skipped_preview.json", "w", encoding="utf-8") as f:
            json.dump(skipped, f, indent=2, ensure_ascii=False)
        # small CSV sample
        sample = [["Title","Citation","url"]]
        for idx in sorted(ok.keys())[:20]:
            rr = rows[idx]
            sample.append([rr.get("Title",""), rr.get("Citation",""), rr.get("url","")])
        with open("out/preview-enrichment/cases_preview.csv","w",newline="",encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerows(sample)

    print(f"Done. Verified URLs added: {len(ok)} ; skipped: {len(skipped)} of slice {s}:{e} (total rows {n}).")

if __name__ == "__main__":
    main()
