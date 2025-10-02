#!/usr/bin/env python3
import csv, json, os, re, sys, time
from pathlib import Path
from typing import List, Dict, Optional

# ------------- Config -------------
ALLOWED_DOMAINS = [
    "www.jerseylaw.je", "jerseylaw.je",
    "www.bailii.org", "bailii.org",
    "www.casemine.com", "casemine.com",
    "www.jerseylaw.je", "jerseylaw.je",
]
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "6"))
PAUSE_SECS = float(os.getenv("RATE_LIMIT_SLEEP", "2.2"))   # polite default
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))
HEARTBEAT_EVERY = int(os.getenv("HEARTBEAT_EVERY", "50"))
IN_CSV = Path(os.getenv("IN_CSV", "data/cases.csv"))
OUT_CSV = Path(os.getenv("OUT_CSV", "data/cases.csv"))
ART_DIR = Path(os.getenv("ART_DIR", "out/enriched"))
START_IDX = int(os.getenv("START_IDX", "1"))  # 1-based (row after header)
END_IDX = int(os.getenv("END_IDX", "0"))     # 0 => process to end
# ----------------------------------

# Lazy import so the script is still importable without deps
def _ddg_search():
    from duckduckgo_search import DDGS  # type: ignore
    return DDGS()

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _has_allowed_domain(url: str) -> bool:
    return any(d in url for d in ALLOWED_DOMAINS)

def _guess_queries(title: str, year: str, citation: str, jurisdiction: str) -> List[str]:
    title = _norm(title)
    year = _norm(year)
    citation = _norm(citation)
    j = (_norm(jurisdiction) or "").lower()

    q = []
    # 1) Exact title + citation first
    if citation:
        q.append(f'{title} "{citation}"')
    # 2) Title + year + site scopes in priority
    if year:
        q.append(f'site:jerseylaw.je {title} {year}')
        q.append(f'site:bailii.org {title} {year}')
    # 3) Bare title scoped
    q.append(f"site:jerseylaw.je {title}")
    q.append(f"site:bailii.org {title}")
    # 4) General web fallback
    if citation and year:
        q.append(f"{title} {citation} {year}")
    else:
        q.append(f"{title}")
    # unique order-preserving
    seen = set()
    uniq = []
    for x in q:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

def _search_one(query: str, max_results: int) -> List[str]:
    # auto-throttled DDG text search
    # Use html backend implicitly; DDGS handles pacing but we also sleep outside.
    tries = 0
    while True:
        tries += 1
        try:
            with _ddg_search() as ddgs:
                results = ddgs.text(query, region="uk-en", safesearch="off", max_results=max_results)
            urls = [r.get("href") or r.get("url") or "" for r in results]
            return [u for u in urls if u]
        except Exception as e:
            if tries >= MAX_RETRIES:
                return []
            # Exponential backoff on any rate/transport error
            sleep_for = min(60.0, PAUSE_SECS * (2 ** (tries - 1)))
            print(f"[ddg/backoff] {type(e).__name__}: {e} | sleeping {sleep_for:.1f}s …", flush=True)
            time.sleep(sleep_for)

def _pick_url(title: str, year: str, citation: str, jurisdiction: str) -> Optional[str]:
    for q in _guess_queries(title, year, citation, jurisdiction):
        urls = _search_one(q, MAX_RESULTS)
        # Keep first allowed domain if any; otherwise keep first result
        allowed = [u for u in urls if _has_allowed_domain(u)]
        candidate_list = allowed if allowed else urls
        if candidate_list:
            # Heuristic: prefer JerseyLaw over BAILII when both show up
            best = None
            for u in candidate_list:
                if "jerseylaw.je" in u:
                    best = u; break
            if not best:
                for u in candidate_list:
                    if "bailii.org" in u:
                        best = u; break
            if not best:
                best = candidate_list[0]
            return best
        # polite pause between queries
        time.sleep(PAUSE_SECS)
    return None

def main():
    IN_CSV.parent.mkdir(parents=True, exist_ok=True)
    ART_DIR.mkdir(parents=True, exist_ok=True)

    if not IN_CSV.exists():
        print(f"ERROR: {IN_CSV} not found", file=sys.stderr)
        sys.exit(1)

    # Load rows
    with IN_CSV.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        fieldnames = rdr.fieldnames or []
        rows = list(rdr)

    # Make sure we have required columns
    needed = ["Title", "Year", "Citation", "Jurisdiction", "Line"]
    for col in needed:
        if col not in fieldnames:
            print(f"ERROR: Missing required column '{col}' in {IN_CSV}", file=sys.stderr)
            sys.exit(2)
    if "URL" not in fieldnames:
        fieldnames.append("URL")

    # Determine end index (1-based to human friendly)
    if END_IDX <= 0 or END_IDX > len(rows):
        end = len(rows)
    else:
        end = END_IDX
    start = max(1, START_IDX)  # first data row is index 1 (after header)

    enriched = 0
    looked = 0
    report: Dict[str, int] = {"total": end - start + 1, "updated": 0, "skipped_existing_url": 0, "failed": 0}

    for i in range(start, end + 1):
        r = rows[i - 1]
        title = r.get("Title", "")
        year = r.get("Year", "")
        citation = r.get("Citation", "")
        jurisdiction = r.get("Jurisdiction", "")
        url_existing = _norm(r.get("URL", ""))

        looked += 1
        if url_existing:
            report["skipped_existing_url"] += 1
            if looked % HEARTBEAT_EVERY == 0:
                print(f"[hb] looked={looked} updated={report['updated']} skipped={report['skipped_existing_url']} failed={report['failed']}")
            continue

        url = _pick_url(title, year, citation, jurisdiction)
        if url:
            rows[i - 1]["URL"] = url
            enriched += 1
            report["updated"] += 1
        else:
            report["failed"] += 1

        if looked % HEARTBEAT_EVERY == 0:
            print(f"[hb] looked={looked} updated={report['updated']} skipped={report['skipped_existing_url']} failed={report['failed']}", flush=True)

        # polite pacing
        time.sleep(PAUSE_SECS)

    # Write CSV back (in-place update)
    tmp = OUT_CSV.with_suffix(".tmp.csv")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    tmp.replace(OUT_CSV)

    # Save small JSON report + list of found URLs
    (ART_DIR / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    urls = [r.get("URL", "") for r in rows if _norm(r.get("URL", ""))]
    (ART_DIR / "urls.json").write_text(json.dumps(urls, indent=2), encoding="utf-8")

    print(f"[done] looked={looked} updated={report['updated']} skipped={report['skipped_existing_url']} failed={report['failed']}")
    # Exit non-zero only if *nothing* worked
    if report["updated"] == 0 and report["skipped_existing_url"] == 0:
        sys.exit(3)

if __name__ == "__main__":
    main()
