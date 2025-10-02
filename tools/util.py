# tools/util.py
import csv, re, time, random, sys
from typing import Dict, Optional, Tuple, Iterable, List
import requests
from bs4 import BeautifulSoup

# --- HTTP helpers -------------------------------------------------------------

def http_get(url: str, timeout: float = 30.0) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0 CourtFirst/1.0"}, timeout=timeout)
        if r.status_code == 200:
            return r.text
        return None
    except Exception:
        return None

def sleep_jitter(min_s: float = 1.2, max_s: float = 2.6) -> None:
    time.sleep(random.uniform(min_s, max_s))

# --- CSV helpers --------------------------------------------------------------

def read_cases_csv(path: str) -> List[Dict[str,str]]:
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        rows = [dict(r) for r in rd]
    return rows

def write_cases_csv(path: str, rows: Iterable[Dict[str,str]]) -> None:
    rows = list(rows)
    # Preserve fixed header order if possible
    base = ["Title","Year","Citation","Jurisdiction","Line","url"]
    # collect all keys
    keys = list({k for r in rows for k in r.keys()})
    # order: base first then the rest
    header = [k for k in base if k in keys] + [k for k in keys if k not in base]
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=header)
        wr.writeheader()
        for r in rows:
            wr.writerow(r)

def ensure_url_column(rows: List[Dict[str,str]]) -> None:
    for r in rows:
        if "url" not in r:
            r["url"] = ""

# --- URL picking --------------------------------------------------------------

BAILII_CASE_RE = re.compile(r"https?://(?:www\.)?bailii\.org/.+?/cases?/.+\.html", re.I)
BAILII_DB_RE   = re.compile(r"https?://(?:www\.)?bailii\.org/(?:databases|cgi-bin/sino_search)", re.I)

JLIB_CASE_RE   = re.compile(r"https?://(?:www\.)?jerseylaw\.je/.+/judgments/.+", re.I)
JLIB_SEARCH_RE = re.compile(r"https?://(?:www\.)?jerseylaw\.je/search/Pages/Results\.aspx", re.I)

def is_real_case(url: str) -> bool:
    u = url or ""
    return bool(BAILII_CASE_RE.search(u) or JLIB_CASE_RE.search(u))

def is_search(url: str) -> bool:
    u = url or ""
    return bool(BAILII_DB_RE.search(u) or JLIB_SEARCH_RE.search(u))

def extract_first_case_from_html(html: str, domain: str) -> Optional[str]:
    if not html: return None
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.select("a[href]")
    for a in anchors:
        href = a.get("href","")
        if domain == "bailii" and BAILII_CASE_RE.search(href):
            return absolutize(href, "https://www.bailii.org")
        if domain == "jlib" and JLIB_CASE_RE.search(href):
            return absolutize(href, "https://www.jerseylaw.je")
    return None

def absolutize(href: str, base: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return base.rstrip("/") + href
    return base.rstrip("/") + "/" + href

def pick_best_url(urls: Dict[str, Optional[str]]) -> Tuple[str, Dict[str,str]]:
    """
    urls may contain:
        jlib_case, jlib_search
        bailii_case, bailii_search
        primary_suggested (fallback)
    Strategy:
      1) prefer direct case links (JerseyLaw, then Bailii).
      2) If only search links exist, fetch the search page and extract first case link.
      3) Fallback to 'primary_suggested' if still nothing.
    """
    plan = {}
    # 1) direct
    for key in ("jlib_case","bailii_case"):
        u = urls.get(key)
        if u and is_real_case(u):
            plan["decision"] = f"direct:{key}"
            return u, plan
    # 2) dereference search pages
    for key, dom in (("jlib_search","jlib"), ("bailii_search","bailii")):
        u = urls.get(key)
        if u and is_search(u):
            plan["decision"] = f"search:{key}"
            html = http_get(u)
            sleep_jitter(0.8, 1.6)
            resolved = extract_first_case_from_html(html, dom)
            if resolved:
                plan["resolved_from"] = key
                return resolved, plan
    # 3) fallback
    if urls.get("primary_suggested"):
        plan["decision"] = "fallback:primary_suggested"
        return urls["primary_suggested"], plan
    plan["decision"] = "none"
    return "", plan
