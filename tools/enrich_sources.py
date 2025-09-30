# tools/enrich_sources.py
"""
Enrich cases with best-guess public URLs.
Priority:
  1) JerseyLaw (if looks like a Jersey citation/title)
  2) BAILII (generic UK-style citations)
  3) DuckDuckGo query URL as fallback (no fabrication)

- Heartbeat every 25 rows.
- Checkpoint every 200 rows -> out/checkpoints/enrich_progress_*.json
- Chunked processing via START_INDEX, END_INDEX env vars (inclusive 0-based).

Inputs:
  data/cases.csv (must have columns: Title, Year, Citation; optional Jurisdiction)
Outputs:
  out/cases_enriched.csv
  out/enrich_report.json
  out/heartbeat.log
  out/checkpoints/...
"""

import os, re, sys, json
from typing import Dict, Any, List
from util import (read_csv, write_csv, save_json, load_json,
                  Heartbeat, build_queries, try_get, ddg_search_url, sleep_jitter)

IN_CSV = os.environ.get("IN_CSV", "data/cases.csv")
OUT_DIR = os.environ.get("OUT_DIR", "out")
OUT_CSV = os.path.join(OUT_DIR, "cases_enriched.csv")
REPORT = os.path.join(OUT_DIR, "enrich_report.json")
CHECK_DIR = os.path.join(OUT_DIR, "checkpoints")
os.makedirs(CHECK_DIR, exist_ok=True)

START_INDEX = int(os.environ.get("START_INDEX", "0"))
END_INDEX   = os.environ.get("END_INDEX")  # inclusive; if None -> all
END_INDEX = None if END_INDEX in (None,"") else int(END_INDEX)

JERSEY_HINT = re.compile(r"\bJERSEY\b|\bJRC\b|\bJLR\b", re.I)
UK_HINT     = re.compile(r"\bUKSC\b|\bEWCA\b|\bEWHC\b|\bWLR\b|\bAll ER\b", re.I)

def jersey_candidate(q:str)->str:
    # Search page on jersey law, not scraping results:
    return f"https://www.jerseylaw.je/?s={q.replace(' ', '+')}"

def bailii_candidate(q:str)->str:
    return f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?search={q.replace(' ','+')}"

def best_url_for(title:str, citation:str, jurisdiction:str) -> Dict[str,str]:
    # We do NOT fabricate final URLs. We record a “likely search URL” on official sites.
    queries = build_queries(title, citation, jurisdiction)
    # Prefer Jerseylaw if looks Jersey-ish:
    if any(JERSEY_HINT.search(x or "") for x in [citation, jurisdiction, title]):
        return {"source_hint":"JerseyLaw", "url": jersey_candidate(queries[0]), "via":"search"}
    # Prefer BAILII for UK-style:
    if any(UK_HINT.search(x or "") for x in [citation, title]):
        return {"source_hint":"BAILII", "url": bailii_candidate(queries[0]), "via":"search"}
    # Fallback to DDG
    return {"source_hint":"DDG", "url": ddg_search_url(queries[0]), "via":"search"}

def main():
    rows = read_csv(IN_CSV)
    n = len(rows)
    if END_INDEX is None: end = n-1
    else: end = min(END_INDEX, n-1)
    start = max(0, min(START_INDEX, end))

    work = rows[start:end+1]
    hb = Heartbeat(total=len(work), every=25, out_dir=OUT_DIR, name="enrich")

    # load previous checkpoint if any (by index window)
    ck_name = os.path.join(CHECK_DIR, f"enrich_progress_{start}_{end}.json")
    progress = load_json(ck_name, {"done":0, "items":[]})
    enriched: List[Dict[str,Any]] = progress.get("items", [])
    done = progress.get("done", 0)

    report = {
        "window": {"start": start, "end": end, "total": len(work)},
        "counts": {"jersey":0, "bailii":0, "ddg":0},
        "errors": []
    }

    # resume position
    idx0 = start + done

    for i in range(idx0, end+1):
        row = rows[i]
        title = row.get("Title","").strip()
        year  = row.get("Year","").strip()
        cit   = row.get("Citation","").strip()
        jur   = row.get("Jurisdiction","").strip()

        if not title:
            report["errors"].append({"i":i, "reason":"missing_title"})
            enriched.append({**row, "source_hint":"", "url":""})
            done += 1; hb.tick(); continue

        try:
            best = best_url_for(title, cit, jur)
            hint = best.get("source_hint","")
            if hint=="JerseyLaw": report["counts"]["jersey"] += 1
            elif hint=="BAILII": report["counts"]["bailii"] += 1
            else: report["counts"]["ddg"] += 1

            enriched.append({**row,
                             "url": best["url"],
                             "source_hint": best["source_hint"],
                             "via": best["via"]})
        except Exception as e:
            report["errors"].append({"i":i, "title":title, "err":str(e)})
            enriched.append({**row, "source_hint":"", "url":""})

        done += 1
        hb.tick()

        # checkpoint every 200
        if done % 200 == 0 or i==end:
            save_json(ck_name, {"done":done, "items":enriched})

        # be polite
        sleep_jitter(0.3, 0.4)

    # merge back into full set so downstream files stay simple
    out_rows = rows[:]
    out_rows[start:end+1] = enriched

    write_csv(OUT_CSV, out_rows,
              fieldnames=list(out_rows[0].keys()) + ["url","source_hint","via"] if out_rows else
              ["Title","Year","Citation","Jurisdiction","url","source_hint","via"])

    save_json(REPORT, report)
    print(f"Enriched window {start}-{end}. Jersey:{report['counts']['jersey']}  "
          f"BAILII:{report['counts']['bailii']}  DDG:{report['counts']['ddg']}")
    print(f"Wrote {OUT_CSV} and {REPORT}")

if __name__=="__main__":
    main()
