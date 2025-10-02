#!/usr/bin/env python3
# tools/enrich_sources.py
# Populate authoritative URLs for cases by polite, batched DuckDuckGo HTML lookups.
# No fabrication: if a safe, parseable link on the intended domain isn't found, we leave URL empty.

import argparse
import csv
import json
import os
import random
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ----------------------------
# CLI
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich case CSV with real source URLs (polite, batched).")
    p.add_argument("--input", required=True, help="Path to input CSV (must contain Title column).")
    p.add_argument("--out", required=True, help="Path to output CSV.")
    p.add_argument("--start", type=int, default=0, help="Start row index (inclusive) in the input dataframe.")
    p.add_argument("--end", type=int, default=None, help="End row index (exclusive). Omit for end-of-file.")
    p.add_argument("--sleep-min", type=float, default=2.0, help="Min seconds to sleep between queries.")
    p.add_argument("--sleep-max", type=float, default=4.0, help="Max seconds to sleep between queries.")
    p.add_argument("--max-retries", type=int, default=3, help="Max retries per network request.")
    p.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout per request (seconds).")
    p.add_argument("--user-agent", default="Mozilla/5.0 (X11; Linux x86_64) CourtFirstBot/1.0", help="HTTP User-Agent.")
    p.add_argument("--batch-name", default="", help="Optional batch label for logs.")
    p.add_argument("--ddg-base", default="https://duckduckgo.com/html", help="DuckDuckGo HTML endpoint.")
    p.add_argument("--emit-json", action="store_true", help="Also write urls.json and fetch_report.json next to --out.")
    return p.parse_args()

# ----------------------------
# Models / helpers
# ----------------------------
@dataclass
class URLPick:
    row_index: int
    title: str
    year: Optional[str]
    citation: Optional[str]
    jurisdiction: Optional[str]
    chosen_url: Optional[str]
    chosen_domain: Optional[str]
    method: str  # 'ddg:jerseylaw', 'ddg:bailii', 'ddg:open'
    reason: str  # short text

@dataclass
class Counters:
    total: int = 0
    attempted: int = 0
    found: int = 0
    not_found: int = 0
    ddg_rate_limited: int = 0
    errors: int = 0

HEADERS_TEMPLATE = lambda ua: {
    "User-Agent": ua,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Connection": "keep-alive",
}

SAFE_DOMAINS = ["jerseylaw.je", "bailii.org"]

def is_on_domain(url: str, domain: str) -> bool:
    try:
        from urllib.parse import urlparse
        netloc = urlparse(url).netloc.lower()
        return netloc.endswith(domain)
    except Exception:
        return False

def row_value(row, key: str) -> Optional[str]:
    if key in row and pd.notna(row[key]):
        v = str(row[key]).strip()
        return v if v else None
    return None

def ddg_query(session: requests.Session, base: str, query: str, timeout: float) -> Tuple[Optional[str], Optional[str]]:
    """Return first result URL + title from DuckDuckGo HTML results page, or (None, None) if none."""
    params = {"q": query, "kl": "uk-en"}
    resp = session.get(base, params=params, timeout=timeout)
    if resp.status_code == 429:
        raise requests.HTTPError("429 Too Many Requests")
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    # DDG HTML page has results links in a.tags with class 'result__a' or in 'links_main__link'
    # We handle both patterns.
    link = soup.select_one("a.result__a") or soup.select_one("a.links_main__link")
    if not link or not link.get("href"):
        return None, None
    return link.get("href"), link.get_text(" ", strip=True)

def polite_sleep(smin: float, smax: float):
    time.sleep(random.uniform(smin, smax))

def build_queries(title: str, year: Optional[str], citation: Optional[str]) -> List[Tuple[str, str]]:
    """Return an ordered list of (method_label, query) pairs to try."""
    tokens = [title]
    if year:
        tokens.append(str(year))
    if citation:
        tokens.append(str(citation))
    base = " ".join(t for t in tokens if t)

    return [
        ("ddg:jerseylaw", f'site:jerseylaw.je "{title}" {year or ""}'),
        ("ddg:bailii",    f'site:bailii.org "{title}" {year or ""}'),
        ("ddg:open",      base),
    ]

def pick_url_for_row(session: requests.Session,
                     ddg_base: str,
                     title: str,
                     year: Optional[str],
                     citation: Optional[str],
                     timeout: float,
                     smin: float, smax: float,
                     max_retries: int) -> Tuple[Optional[str], Optional[str], str, str]:
    """
    Try a sequence of search strategies. Return (url, domain, method, reason).
    If nothing found, url/domain are None.
    """
    for method, q in build_queries(title, year, citation):
        tries = 0
        while True:
            tries += 1
            try:
                url, _ = ddg_query(session, ddg_base, q, timeout)
                if url:
                    # Prefer exact domain if method suggests it
                    if "jerseylaw" in method and is_on_domain(url, "jerseylaw.je"):
                        return url, "jerseylaw.je", method, f"match via DDG ({q})"
                    if "bailii" in method and is_on_domain(url, "bailii.org"):
                        return url, "bailii.org", method, f"match via DDG ({q})"
                    # For open search, accept urls on safe domains first
                    if is_on_domain(url, "jerseylaw.je") or is_on_domain(url, "bailii.org"):
                        dom = "jerseylaw.je" if is_on_domain(url, "jerseylaw.je") else "bailii.org"
                        return url, dom, method, f"match via DDG ({q})"
                    # Otherwise skip (we only accept a controlled domain)
                    reason = f"first result not on allowed domain: {url}"
                else:
                    reason = "no DDG result"
            except requests.HTTPError as e:
                if "429" in str(e):
                    reason = "rate limited"
                else:
                    reason = f"http error: {e}"
            except Exception as e:
                reason = f"error: {e}"

            if tries >= max_retries:
                break

            # Backoff before retry
            polite_sleep(smin, smax)

        # move to next method if this one failed
        polite_sleep(smin, smax)

    return None, None, "none", "no acceptable domain match"

def write_step_summary(lines: List[str]):
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception:
        pass

# ----------------------------
# Main
# ----------------------------
def main():
    args = parse_args()

    # Read input CSV
    df = pd.read_csv(args.input)
    if "Title" not in df.columns:
        sys.stderr.write("ERROR: input CSV must have a 'Title' column\n")
        sys.exit(2)

    start = max(0, int(args.start or 0))
    end = int(args.end) if args.end is not None else len(df)
    end = min(end, len(df))
    if start >= end:
        sys.stderr.write(f"Nothing to do: start={start} >= end={end}\n")
        # still write out unchanged CSV for determinism
        df.to_csv(args.out, index=False)
        return

    # Ensure output columns exist
    for col in ["url", "url_source", "url_status"]:
        if col not in df.columns:
            df[col] = ""

    session = requests.Session()
    session.headers.update(HEADERS_TEMPLATE(args.user_agent))

    picks: List[URLPick] = []
    ctr = Counters(total=(end - start))
    t0 = time.time()

    # Heartbeat header
    write_step_summary([
        f"### Enrich batch {args.batch_name or ''}".strip(),
        f"- Rows: **{start}..{end-1}** (total {ctr.total})",
        f"- Sleep: {args.sleep_min:.2f}–{args.sleep_max:.2f}s · Retries: {args.max_retries} · Timeout: {args.timeout:.1f}s",
        ""
    ])

    for i in range(start, end):
        row = df.iloc[i]
        title = row_value(row, "Title")
        year = row_value(row, "Year")
        citation = row_value(row, "Citation")
        juris = row_value(row, "Jurisdiction")

        ctr.attempted += 1

        if not title:
            picks.append(URLPick(i, "", year, citation, juris, None, None, "skip", "missing title"))
            df.at[i, "url"] = ""
            df.at[i, "url_source"] = ""
            df.at[i, "url_status"] = "skip: no title"
            continue

        try:
            url, domain, method, reason = pick_url_for_row(
                session=session,
                ddg_base=args.ddg_base,
                title=title,
                year=year,
                citation=citation,
                timeout=args.timeout,
                smin=args.sleep_min,
                smax=args.sleep_max,
                max_retries=args.max_retries,
            )
            if url:
                ctr.found += 1
                df.at[i, "url"] = url
                df.at[i, "url_source"] = domain or ""
                df.at[i, "url_status"] = method
                picks.append(URLPick(i, title, year, citation, juris, url, domain, method, reason))
            else:
                ctr.not_found += 1
                df.at[i, "url"] = ""
                df.at[i, "url_source"] = ""
                df.at[i, "url_status"] = reason
                picks.append(URLPick(i, title, year, citation, juris, None, None, method, reason))
        except requests.HTTPError as e:
            msg = str(e)
            if "429" in msg:
                ctr.ddg_rate_limited += 1
            ctr.errors += 1
            df.at[i, "url"] = ""
            df.at[i, "url_source"] = ""
            df.at[i, "url_status"] = f"http error: {msg}"
            picks.append(URLPick(i, title, year, citation, juris, None, None, "error", msg))
        except Exception as e:
            ctr.errors += 1
            df.at[i, "url"] = ""
            df.at[i, "url_source"] = ""
            df.at[i, "url_status"] = f"error: {e}"
            picks.append(URLPick(i, title, year, citation, juris, None, None, "error", str(e)))

        # polite pacing between rows
        polite_sleep(args.sleep_min, args.sleep_max)

        # heartbeat line to logs each ~50 rows
        if (i - start + 1) % 50 == 0 or (i + 1) == end:
            done = (i - start + 1)
            rate = done / max(1.0, (time.time() - t0))
            print(f"enrich: {done}/{ctr.total} (~{rate:.2f}/s)", flush=True)

    # Write outputs
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    df.to_csv(args.out, index=False)

    # Optional JSON sidecar reports
    if args.emit_json:
        out_dir = os.path.dirname(os.path.abspath(args.out)) or "."
        with open(os.path.join(out_dir, "urls.json"), "w", encoding="utf-8") as fh:
            json.dump([asdict(p) for p in picks], fh, ensure_ascii=False, indent=2)
        with open(os.path.join(out_dir, "fetch_report.json"), "w", encoding="utf-8") as fh:
            json.dump(asdict(ctr), fh, ensure_ascii=False, indent=2)

    # Final summary
    elapsed = time.time() - t0
    summary = [
        "### Enrich results",
        f"- Processed: **{ctr.total}** rows",
        f"- Found: **{ctr.found}**, Not found: **{ctr.not_found}**, Errors: **{ctr.errors}**, 429s: **{ctr.ddg_rate_limited}**",
        f"- Elapsed: **{elapsed:.1f}s**",
    ]
    print("\n".join(summary), flush=True)
    write_step_summary(["", *summary])

if __name__ == "__main__":
    main()
