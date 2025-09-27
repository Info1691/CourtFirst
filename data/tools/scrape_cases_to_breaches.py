#!/usr/bin/env python3
# CourtFirst/tools/scrape_cases_to_breaches.py
import csv, hashlib, json, os, re, sys, time, urllib.parse, urllib.request
from html import unescape
from pathlib import Path
from typing import Dict, List, Tuple, Optional

try:
    from bs4 import BeautifulSoup  # type: ignore
except ImportError:
    print("This script requires beautifulsoup4. In CI we pip install it.")
    sys.exit(1)

DATA_CSV = Path("data/cases.csv")
OUT_DIR = Path("out")
CASE_JSON_DIR = OUT_DIR / "case_json"
CANDIDATES_JSON = OUT_DIR / "breach_candidates.json"
BREACHES_JSON = OUT_DIR / "breaches.json"
CACHE_DIR = Path(".cache") / "cases"

USER_AGENT = "CourtFirst/1.0 (+contact: maintainer@example)"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN = 1.0
MAX_RETRIES = 3

# Limit domains to official/public sites you intend to use
BASE_DOMAINS = {
    "www.jerseylaw.je","jerseylaw.je",
    "www.bailii.org","bailii.org"
}

OUTCOME_HEADINGS = [
    "held","conclusion","conclusions","disposition","order","orders",
    "result","reasons","findings","judgment","decision"
]

# Court-first mining (no hard-wired breach list)
NEGATIONS = re.compile(r"\b(no|not|never|without|none)\b.*\b(breach|failed?|failure|liable|misappl(?:y|ication)|misappropriation)\b", re.I)
OUTCOME_VERBS = re.compile(r"\b(held|finds?|found|decides?|decided|orders?|ordered|concludes?|concluded|liable)\b", re.I)
NP_AFTER_BREACH = re.compile(r"\bbreach of ([a-z][a-z\s\-]{2,80}?)\b", re.I)
FAILURE_TO      = re.compile(r"\b(failure|failed)\s+to\s+([a-z][a-z\s\-]{2,80}?)\b", re.I)
MIS_APP         = re.compile(r"\bmisappl(?:y|ication) of ([a-z][a-z\s\-]{2,80}?)\b", re.I)
SELF_DEAL       = re.compile(r"\b(self\-dealing|acted in (?:own|self) interest|acted in self\-interest)\b", re.I)
CONFLICT        = re.compile(r"\bconflict of interest\b", re.I)

MINERS = [
    (lambda s: NP_AFTER_BREACH.findall(s),  lambda x: f"breach of {x}"),
    (lambda s: FAILURE_TO.findall(s),       lambda x: f"failure to {x}"),
    (lambda s: MIS_APP.findall(s),          lambda x: f"misapplication of {x}"),
    (lambda s: SELF_DEAL.findall(s),        lambda x: "self-dealing"),
    (lambda s: CONFLICT.findall(s),         lambda x: "conflict of interest"),
]

def ensure_dirs():
    for p in [OUT_DIR, CASE_JSON_DIR, CACHE_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def hash_url(url: str) -> str:
    import hashlib as _h; return _h.sha256(url.encode("utf-8")).hexdigest()[:16]

def same_domain_ok(url: str) -> bool:
    netloc = urllib.parse.urlparse(url).netloc.lower().split(":")[0]
    return netloc in BASE_DOMAINS

def polite_fetch(url: str) -> Optional[str]:
    if not same_domain_ok(url):
        print(f"[SKIP] Domain not allowed by BASE_DOMAINS: {url}")
        return None
    cache_path = CACHE_DIR / f"{hash_url(url)}.html"
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="ignore")

    for i in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                html = resp.read().decode("utf-8", "ignore")
                cache_path.write_text(html, encoding="utf-8")
                time.sleep(SLEEP_BETWEEN)
                return html
        except Exception as e:
            print(f"[RETRY {i+1}/{MAX_RETRIES}] {url}: {e}")
            time.sleep(1.5 + i)
    return None

def extract_paragraphs(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for tag in soup.select("p, div, li"):
        text = tag.get_text(" ", strip=True)
        if not text or len(text) < 20:
            continue
        pid = tag.get("id") or tag.get("name") or None
        candidates.append({"pid": pid, "text": unescape(text)})
    if not candidates:
        txt = soup.get_text("\n", strip=True)
        for i, b in enumerate([t for t in txt.split("\n") if len(t.strip()) > 20], 1):
            candidates.append({"pid": f"p{i}", "text": b.strip()})
    return candidates

def find_outcome_spans(paragraphs: List[Dict]) -> List[int]:
    hits = set()
    for i, p in enumerate(paragraphs):
        t = p["text"].lower()
        for h in OUTCOME_HEADINGS:
            if re.search(rf"\b{re.escape(h)}\b[:\s]*$", t) or re.search(rf"^{re.escape(h)}[:\s]", t):
                for j in range(i, min(i+12, len(paragraphs))):
                    hits.add(j)
    return sorted(hits)

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip(" \t\n\r-—.,;:")

def detect_breaches(paragraphs: List[Dict]) -> List[Dict]:
    results = []
    outcome_zone = set(find_outcome_spans(paragraphs))
    for idx, p in enumerate(paragraphs):
        text = p["text"].strip()
        if idx not in outcome_zone and OUTCOME_VERBS.search(text) is None:
            continue
        if NEGATIONS.search(text):
            continue
        found_here = []
        for finder, normaliser in MINERS:
            hits = finder(text)
            if not hits:
                continue
            for h in hits:
                phrase = h if isinstance(h, str) else " ".join([g for g in h if isinstance(g, str)])
                phrase = norm_space(phrase)
                if not phrase:
                    continue
                tag_text = norm_space(normaliser(phrase).lower())
                if tag_text:
                    found_here.append(tag_text)
        seen = set()
        for tag_text in found_here:
            if tag_text in seen:  # dedupe in-paragraph
                continue
            seen.add(tag_text)
            results.append({
                "phrase": tag_text,
                "normalized": tag_text,
                "para_index": idx,
                "pid": p.get("pid"),
                "text": text,
                "in_outcome_zone": idx in outcome_zone
            })
    return results

def consolidate(case_id: str, title: str, juris: str, url: str, cands: List[Dict]) -> List[Dict]:
    by_tag: Dict[str, List[Dict]] = {}
    for c in cands:
        by_tag.setdefault(c["normalized"], []).append(c)
    items = []
    for tag, cs in by_tag.items():
        prov = []
        for c in cs:
            prov.append({
                "source_type": "Case",
                "label": title or case_id,
                "source_id": case_id,
                "paragraph_index": c["para_index"],
                "pid": c.get("pid"),
                "excerpt": c["text"][:600],
                "jurisdiction": juris or "Jersey",
                "confidence": 0.9 if c.get("in_outcome_zone") else 0.7,
                "url": url
            })
        items.append({
            "category": "Unclassified",
            "tag": tag,
            "aliases": [],
            "provenance": prov
        })
    return items

def main():
    if not DATA_CSV.exists():
        print(f"Missing {DATA_CSV}. Add rows: case_id,jurisdiction,url")
        sys.exit(2)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CASE_JSON_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    all_candidates = []
    all_breaches = []

    with DATA_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            case_id = row["case_id"].strip()
            url = row["url"].strip()
            juris = (row.get("jurisdiction") or "").strip() or "Jersey"
            print(f"\n[CASE] {case_id} ({juris})\n{url}")

            html = polite_fetch(url)
            if not html:
                print(f"[WARN] Could not fetch: {url}")
                continue

            # crude title extraction
            m = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
            title = unescape(m.group(1).strip()) if m else case_id

            paragraphs = extract_paragraphs(html)
            # persist per-case
            (CASE_JSON_DIR / f"{case_id}.json").write_text(
                json.dumps({"case_id": case_id, "jurisdiction": juris, "url": url, "title": title, "paragraphs": paragraphs},
                           ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

            cands = detect_breaches(paragraphs)
            for c in cands:
                c.update({"case_id": case_id, "title": title, "url": url, "jurisdiction": juris})
            all_candidates.extend(cands)

            all_breaches.extend(consolidate(case_id, title, juris, url, cands))

    # write artifacts
    CANDIDATES_JSON.write_text(json.dumps({"count": len(all_candidates), "candidates": all_candidates},
                                          ensure_ascii=False, indent=2), encoding="utf-8")

    # merge by (tag)
    merged: Dict[str, Dict] = {}
    for item in all_breaches:
        key = item["tag"]
        if key not in merged:
            merged[key] = item
        else:
            merged[key]["provenance"].extend(item["provenance"])

    # de-dup provenance
    for k, v in merged.items():
        seen = set(); outp = []
        for p in v["provenance"]:
            sig = (p.get("source_id"), p.get("pid"), p.get("paragraph_index"))
            if sig in seen:
                continue
            seen.add(sig)
            outp.append(p)
        v["provenance"] = outp

    BREACHES_JSON.write_text(json.dumps(list(merged.values()), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[OK] Wrote:\n - {CANDIDATES_JSON}\n - {BREACHES_JSON}\n - {len(list(CASE_JSON_DIR.glob('*.json')))} case files.")
if __name__ == "__main__":
    main()
