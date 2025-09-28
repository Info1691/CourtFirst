#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build search links for JerseyLaw & BAILII and (optionally) auto-resolve the first
result that looks like the same case. We DO NOT invent URLs; auto-resolve only
runs if --auto_resolve=true is set, and still records exactly what was chosen.

Input CSV (UTF-8) must have columns (case-insensitive):
  - case_id
  - title             (case title as extracted from LTJ)
  - citation          (neutral or report citation if available; may be blank)
  - url               (optional; if present we keep it)

Outputs in --outdir:
  - search_links.csv  (case_id, jl_search, bailii_search)
  - resolved.csv      (case_id, source_url)  [includes original url rows + any auto-resolved]
  - resolve_log.json  (full log of attempts; exact HTML titles for chosen links)
"""

import argparse
from pathlib import Path
from typing import Dict, List
import urllib.parse as ul

import requests
from bs4 import BeautifulSoup

from tools.util import read_csv, write_csv, save_json, ensure_dir, http_get, sleep_jitter

JL_SEARCH = "https://www.jerseylaw.je/judgments/pages/search.aspx?Query={q}"
BAILII_SEARCH = "https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={q}"

def norm(s: str) -> str:
    return " ".join((s or "").split())

def make_query(title: str, citation: str) -> str:
    # Prefer citation if present; fall back to title
    q = citation.strip() if citation and citation.strip() else title.strip()
    return ul.quote_plus(q)

def first_result_url(html: str, base: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # JerseyLaw search: results are usually in <a> with href containing "/judgments/"
    a = soup.select_one('a[href*="/judgments/"]')
    if a and a.get("href"):
        href = a["href"]
        if href.startswith("http"):
            return href
        # make absolute if relative
        from urllib.parse import urljoin
        return urljoin(base, href)
    # BAILII search: table results, links often under <a href="/ew/cases/...">
    a = soup.select_one('a[href^="/"], a[href^="http"]')
    if a and a.get("href"):
        href = a["href"]
        if href.startswith("http"):
            return href
        from urllib.parse import urljoin
        return urljoin("https://www.bailii.org/", href)
    return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", required=True, help="cases.csv with case_id,title,citation[,url]")
    ap.add_argument("--outdir", dest="outdir", required=True, help="output folder")
    ap.add_argument("--auto_resolve", default="false", help="true|false (default false)")
    args = ap.parse_args()

    IN = Path(args.in_csv)
    OUT = Path(args.outdir)
    ensure_dir(OUT)

    hmap, rows = read_csv(IN)
    need = {"case_id", "title", "citation"}
    if not need.issubset(hmap.keys()):
        raise ValueError(f"Input must have columns (case-insensitive): {sorted(need)}")
    has_url = "url" in hmap

    # 1) Build search links
    search_rows: List[List[str]] = []
    for r in rows:
        case_id = norm(r[hmap["case_id"]])
        title   = norm(r[hmap["title"]])
        citation= norm(r[hmap["citation"]]) if hmap.get("citation") is not None else ""
        if not case_id: 
            continue
        q = make_query(title, citation)
        jl = JL_SEARCH.format(q=q)
        bl = BAILII_SEARCH.format(q=q)
        search_rows.append([case_id, jl, bl])
    write_csv(["case_id", "jl_search", "bailii_search"], search_rows, OUT / "search_links.csv")

    # 2) Collate known URLs (from CSV) + optionally auto-resolve first results
    resolved_rows: List[List[str]] = []
    log: Dict = {"taken": [], "skipped": []}

    # Keep any explicit URLs provided by you
    if has_url:
        for r in rows:
            case_id = norm(r[hmap["case_id"]])
            url = norm(r[hmap["url"]]) if r[hmap["url"]] else ""
            if case_id and url:
                resolved_rows.append([case_id, url])
                log["taken"].append({"case_id": case_id, "source_url": url, "source": "provided"})

    auto = (args.auto_resolve.strip().lower() == "true")
    if auto:
        sess = requests.Session()
        for r in rows:
            case_id = norm(r[hmap["case_id"]])
            title   = norm(r[hmap["title"]])
            citation= norm(r[hmap["citation"]]) if hmap.get("citation") is not None else ""
            if not case_id: 
                continue

            # Skip if already have explicit URL
            if any(case_id == x[0] for x in resolved_rows):
                continue

            # Try JerseyLaw first
            q = make_query(title, citation)
            jl_url = JL_SEARCH.format(q=q)
            try:
                status, html = http_get(jl_url, sess, timeout=25)
                if status == 200:
                    candidate = first_result_url(html, base="https://www.jerseylaw.je/")
                    if candidate:
                        resolved_rows.append([case_id, candidate])
                        log["taken"].append({"case_id": case_id, "source_url": candidate, "source": "auto:JL"})
                        sleep_jitter(0.8)
                        continue
                log["skipped"].append({"case_id": case_id, "reason": f"JL status {status}"})
            except Exception as e:
                log["skipped"].append({"case_id": case_id, "reason": f"JL error: {e}"})

            # Then BAILII
            bl_url = BAILII_SEARCH.format(q=q)
            try:
                status, html = http_get(bl_url, sess, timeout=25)
                if status == 200:
                    candidate = first_result_url(html, base="https://www.bailii.org/")
                    if candidate:
                        resolved_rows.append([case_id, candidate])
                        log["taken"].append({"case_id": case_id, "source_url": candidate, "source": "auto:BAILII"})
                        sleep_jitter(0.8)
                        continue
                log["skipped"].append({"case_id": case_id, "reason": f"BAILII status {status}"})
            except Exception as e:
                log["skipped"].append({"case_id": case_id, "reason": f"BAILII error: {e}"})

            sleep_jitter(0.8)

    write_csv(["case_id", "source_url"], resolved_rows, OUT / "resolved.csv")
    save_json(log, OUT / "resolve_log.json")

if __name__ == "__main__":
    main()
