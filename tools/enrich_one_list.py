#!/usr/bin/env python3
"""
Enrich a single listing of cases into one canonical CSV: out/cases_enriched.csv

Inputs
------
- data/cases.csv  (flexible headers; case-insensitive)
  Accepts any of these column names (case-insensitive):
    - id columns:        case_id | id
    - title columns:     title | case | name | raw
    - citation columns:  citation | cite
    - jurisdiction:      jurisdiction | juris
    - ltj refs:          ltj_refs | ltj | refs
    - url column:        url | source_url

What it does
------------
1) Normalizes rows.
2) Builds search links (DuckDuckGo, BAILII, JerseyLaw).
3) Attempts to auto-resolve authoritative URLs:
   - Respect an existing URL in the row (no fabrication).
   - Otherwise try to find a result from JerseyLaw or BAILII via DuckDuckGo (html).
4) Optionally fetches the chosen URL and extracts a few safe metadata fields & an
   outcome snippet (verbatim if found; else blank). No generative text.
5) Emits ONE file: out/cases_enriched.csv

Environment
-----------
- Designed to run in GitHub Actions or locally (Python 3.11+).
- Network calls are throttled. No scraping beyond basic HTML GET of the single chosen page.
"""

from __future__ import annotations
import csv
import html
import os
import re
import sys
import time
import json
import urllib.parse
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------- Config ----------------------

OUT_DIR = os.environ.get("OUT_DIR", "out")
IN_CSV  = os.environ.get("IN_CSV", "data/cases.csv")

# polite defaults
HTTP_TIMEOUT = 20
PAUSE_BETWEEN_REQUESTS = 1.0  # seconds

DDG_HTML = "https://html.duckduckgo.com/html/"

HEADERS = {
    "User-Agent": "CourtFirst/0.1 (+https://github.com/; research non-commercial)"
}

# ----------------------------------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def read_cases_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return []
    # normalize keys to lowercase
    norm = []
    for r in rows:
        norm.append({(k or "").strip().lower(): (v or "").strip() for k, v in r.items()})
    return norm

def pick(r: Dict[str, str], names: List[str]) -> str:
    for n in names:
        v = r.get(n)
        if v:
            return v
    return ""

def normalize_row(r: Dict[str, str]) -> Dict[str, str]:
    case_id = pick(r, ["case_id","id"])
    title   = pick(r, ["title","case","name","raw"])
    citation= pick(r, ["citation","cite"])
    juris   = pick(r, ["jurisdiction","juris"])
    ltjrefs = pick(r, ["ltj_refs","ltj","refs"])
    url     = pick(r, ["url","source_url"])
    return {
        "case_id": case_id,
        "title": title,
        "citation": citation,
        "jurisdiction": juris,
        "ltj_refs": ltjrefs,
        "url": url
    }

def build_search_queries(title: str, citation: str) -> Dict[str, str]:
    # conservative query strings
    base = title or citation
    q_core = base if base else ""
    # Quote title/citation if available to reduce noise
    terms = []
    if citation:
        terms.append(f'"{citation}"')
    if title:
        terms.append(f'"{title}"')
    q = " ".join(terms) if terms else q_core

    ddg_q = q
    ddg = f"{DDG_HTML}?q={urllib.parse.quote(ddg_q)}"

    ddg_bailii = f"{DDG_HTML}?q={urllib.parse.quote('site:bailii.org ' + q)}"
    ddg_jl     = f"{DDG_HTML}?q={urllib.parse.quote('site:jerseylaw.je ' + q)}"

    return {
        "ddg": ddg,
        "ddg_bailii": ddg_bailii,
        "ddg_jerseylaw": ddg_jl,
    }

def ddg_first_result(url: str, prefer_domains: Tuple[str,...]=()) -> Optional[str]:
    """
    Returns first result URL from DuckDuckGo HTML (no JS).
    If prefer_domains provided, returns the first result that matches any domain.
    """
    try:
        resp = requests.post(url, headers=HEADERS, timeout=HTTP_TIMEOUT, data={"kl":"wt-wt"})
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        for a in soup.select("a.result__url, a.result__a"):
            href = a.get("href") or ""
            if href.startswith("http"):
                links.append(href)
        # new DDG HTML sometimes uses .result__a; fallback scanning:
        if not links:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http"):
                    links.append(href)
        if prefer_domains:
            for href in links:
                host = urllib.parse.urlparse(href).netloc.lower()
                if any(dom in host for dom in prefer_domains):
                    return href
        return links[0] if links else None
    except Exception:
        return None

def resolve_urls(title: str, citation: str, existing_url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], str]:
    """
    Return bailii_url, jerseylaw_url, ddg_url, final_url, notes
    """
    notes = []
    if existing_url:
        # Trust user-provided URL most
        final_url = existing_url
        # attempt to classify
        host = urllib.parse.urlparse(existing_url).netloc.lower()
        bailii = existing_url if "bailii.org" in host else None
        jl     = existing_url if "jerseylaw.je" in host else None
        ddg    = None
        return bailii, jl, ddg, final_url, "; ".join(notes) if notes else ""

    qs = build_search_queries(title, citation)
    # Prefer domain-specific first:
    bailii = ddg_first_result(qs["ddg_bailii"], prefer_domains=("bailii.org",))
    time.sleep(PAUSE_BETWEEN_REQUESTS)
    jl = ddg_first_result(qs["ddg_jerseylaw"], prefer_domains=("jerseylaw.je",))
    time.sleep(PAUSE_BETWEEN_REQUESTS)
    ddg_any = ddg_first_result(qs["ddg"])

    final_url = jl or bailii or ddg_any
    ddg = ddg_any

    if not final_url:
        notes.append("no_url_resolved")

    return bailii, jl, ddg, final_url, "; ".join(notes) if notes else ""

def fetch_once(url: str) -> Tuple[str, int, Optional[str]]:
    """
    Fetch a single page. Returns (status, http_code, html_text or None)
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
        code = resp.status_code
        if 200 <= code < 300 and "text/html" in (resp.headers.get("content-type","").lower()):
            return "ok", code, resp.text
        return "error", code, None
    except requests.RequestException as e:
        return "error", 0, None

def parse_metadata(html_text: str) -> Dict[str,str]:
    """
    Super-conservative metadata parser:
    - tries to pick up neutral citation, court, date
    - leaves blank if not found
    (No hallucination: only regex/DOM extraction)
    """
    out = {"decision_date":"", "court":"", "neutral_citation":""}
    if not html_text:
        return out
    text = " ".join(BeautifulSoup(html_text, "html.parser").stripped_strings)
    # very basic patterns
    m = re.search(r"\b([12][0-9]{3})\b", text)
    if m:
        out["decision_date"] = m.group(1)  # year only if that's all we can find

    # neutral citation like [2014] JRC 123 or [2010] UKSC 4
    m2 = re.search(r"\[[12][0-9]{3}\]\s+[A-Z]{2,}[A-Z0-9]*\s+\d+\b", text)
    if m2:
        out["neutral_citation"] = m2.group(0)

    # extremely light "court" capture (look near neutral citation)
    if out["neutral_citation"]:
        court_guess = out["neutral_citation"].split()[-2]  # JRC/UKSC/… token
        out["court"] = court_guess

    return out

def extract_outcome_snippet(html_text: str) -> str:
    """
    Try to capture a small verbatim outcome/held snippet if the page exposes
    headings like "Held", "Decision", "Conclusion", "Judgment".
    Returns a short string (<= 300 chars) or "".
    """
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    # Look for headings and gather following paragraph
    heads = soup.find_all(re.compile(r"^h[1-6]$"))
    keys = ("held", "decision", "conclusion", "judgment", "disposition", "order")
    for h in heads:
        t = (h.get_text() or "").strip().lower()
        if any(k in t for k in keys):
            # pick next paragraph-like text
            node = h.find_next(["p","div","span","li"])
            if node:
                snippet = " ".join(node.stripped_strings)
                snippet = snippet[:300]
                return snippet
    # fallback: nothing
    return ""

def main() -> None:
    ensure_dir(OUT_DIR)
    rows = read_cases_csv(IN_CSV)

    out_rows = []
    for r in rows:
        base = normalize_row(r)
        case_id = base["case_id"] or ""
        title   = base["title"]
        citation= base["citation"]
        juris   = base["jurisdiction"]
        ltjrefs = base["ltj_refs"]
        existing_url = base["url"]

        bailii, jl, ddg, final_url, notes = resolve_urls(title, citation, existing_url)

        fetch_status = "skipped"
        http_code = ""
        decision_date = court = neutral = outcome = ""

        if final_url:
            status, code, html_text = fetch_once(final_url)
            fetch_status = status
            http_code = str(code) if code else ""
            if status == "ok" and html_text:
                meta = parse_metadata(html_text)
                decision_date = meta.get("decision_date","")
                court = meta.get("court","")
                neutral = meta.get("neutral_citation","")
                outcome = extract_outcome_snippet(html_text)
            elif status != "ok":
                notes = (notes + "; " if notes else "") + "fetch_failed"

            time.sleep(PAUSE_BETWEEN_REQUESTS)
        else:
            # Keep row; mark not found
            fetch_status = "skipped"
            http_code = ""
            notes = (notes + "; " if notes else "") + "no_final_url"

        out_rows.append({
            "case_id": case_id,
            "title": title,
            "citation": citation,
            "jurisdiction": juris,
            "ltj_refs": ltjrefs,
            "ddg_url": ddg or "",
            "bailii_url": bailii or "",
            "jerseylaw_url": jl or "",
            "final_url": final_url or "",
            "fetch_status": fetch_status,
            "http_status": http_code,
            "decision_date": decision_date,
            "court": court,
            "neutral_citation": neutral,
            "outcome_snippet": outcome,
            "notes": notes,
        })

    # write ONE file
    out_path = os.path.join(OUT_DIR, "cases_enriched.csv")
    ensure_dir(os.path.dirname(out_path))
    fieldnames = [
        "case_id","title","citation","jurisdiction","ltj_refs",
        "ddg_url","bailii_url","jerseylaw_url","final_url",
        "fetch_status","http_status","decision_date","court","neutral_citation",
        "outcome_snippet","notes"
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in out_rows:
            w.writerow(row)

    # tiny run report for troubleshooting
    report = {
        "input_rows": len(rows),
        "output_rows": len(out_rows),
        "timestamp": int(time.time())
    }
    with open(os.path.join(OUT_DIR, "run_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
