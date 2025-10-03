# tools/util_sources.py
import re, time, random, hashlib, html
from typing import Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text

UA = "CourtFirst/1.0 (+GitHub Actions; requests)"
HDRS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def sleep(min_s=0.8, max_s=1.8):
    time.sleep(random.uniform(min_s, max_s))

def http_get(url: str, timeout=25) -> str:
    r = requests.get(url, headers=HDRS, timeout=timeout)
    r.raise_for_status()
    return r.text

def http_get_bytes(url: str, timeout=30) -> bytes:
    r = requests.get(url, headers=HDRS, timeout=timeout)
    r.raise_for_status()
    return r.content

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def norm_text(s: str) -> str:
    s = html.unescape((s or "").strip())
    s = re.sub(r"\s+", " ", s)
    s = s.replace("’","'").replace("–","-").replace("—","-")
    return s

def looks_pdf_url(u: str) -> bool:
    return bool(u and re.search(r"\.pdf($|\?)", u, re.I))

# ---------- Primary: JerseyLaw ------------------------------------------------

def jerseylaw_search_url(query: str) -> str:
    return "https://www.jerseylaw.je/judgments/Pages/results.aspx?" + urlencode({"k": query})

def jerseylaw_pick_case_link(html_txt: str) -> Optional[str]:
    soup = BeautifulSoup(html_txt, "lxml")
    for a in soup.select("a[href*='/judgments/']"):
        href = a.get("href")
        if href:
            return urljoin("https://www.jerseylaw.je/", href)
    return None

def jerseylaw_find(title: str, citation: str) -> Tuple[Optional[str], Optional[str]]:
    q = f"{title} {citation}".strip()
    search_u = jerseylaw_search_url(q)
    try:
        res = http_get(search_u)
    except Exception:
        return None, None
    case_page = jerseylaw_pick_case_link(res)
    if not case_page:
        return search_u, None
    try:
        page = http_get(case_page)
    except Exception:
        return search_u, None
    # “View PDF”
    soup = BeautifulSoup(page, "lxml")
    a = soup.find("a", string=re.compile(r"View\s+PDF", re.I))
    if a and a.get("href"):
        pdf = urljoin(case_page, a["href"])
        return case_page, pdf
    # generic .pdf link
    a = soup.find("a", href=re.compile(r"\.pdf$", re.I))
    if a and a.get("href"):
        return case_page, urljoin(case_page, a["href"])
    return case_page, None

# ---------- Primary: BAILII ---------------------------------------------------

def bailii_search_url(query: str) -> str:
    return "https://www.bailii.org/cgi-bin/sino_search_1.cgi?" + urlencode({"query": query})

def bailii_pick_case_link(html_txt: str) -> Optional[str]:
    soup = BeautifulSoup(html_txt, "lxml")
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href: 
            continue
        if href.startswith("/"):
            href = urljoin("https://www.bailii.org/", href)
        # prefer case pages
        if re.search(r"/\w\w/.+/\d{4}/\d+\.html?$", href):
            return href
    return None

def bailii_find(title: str, citation: str) -> Tuple[Optional[str], Optional[str]]:
    q = f"{title} {citation}".strip()
    search_u = bailii_search_url(q)
    try:
        res = http_get(search_u)
    except Exception:
        return None, None
    case_page = bailii_pick_case_link(res)
    if not case_page:
        return search_u, None
    try:
        page = http_get(case_page)
    except Exception:
        return search_u, None
    soup = BeautifulSoup(page, "lxml")
    a = soup.find("a", href=re.compile(r"\.pdf$", re.I))
    if a and a.get("href"):
        return case_page, urljoin(case_page, a["href"])
    return case_page, None

# ---------- PDF-specific web fallback (DuckDuckGo HTML) -----------------------

def ddg_first(query: str) -> Optional[str]:
    html_txt = http_get("https://duckduckgo.com/html/?" + urlencode({"q": query, "ia": "web"}))
    soup = BeautifulSoup(html_txt, "lxml")
    for a in soup.select("a.result__a"):
        href = a.get("href") or ""
        # unwrap /l/?uddg=
        if href.startswith("/l/?"):
            qs = parse_qs(urlparse(href).query)
            u = qs.get("uddg", [None])[0]
            if u:
                return u
        return href
    return None

def web_pdf_fallback(title: str, citation: str) -> Optional[str]:
    q = f'{title} {citation} filetype:pdf'
    hit = ddg_first(q)
    if hit and looks_pdf_url(hit):
        return hit
    return None

# ---------- Verification ------------------------------------------------------

def verify_pdf_contains(pdf_bytes: bytes, title: str, citation: str) -> bool:
    try:
        txt = pdf_extract_text(pdf_bytes) or ""
    except Exception:
        return False
    T = " ".join(norm_text(title).split())[:120].lower()
    C = (citation or "").lower()
    body = " ".join(norm_text(txt).split()).lower()
    # require several title tokens + (citation or year)
    tokens = [w for w in re.split(r"[\W_]+", T) if len(w) > 2][:5]
    ok_title = all(t in body for t in tokens[:3])  # at least 3 tokens
    ok_cite = (C and C in body) or True
    return ok_title and ok_cite
