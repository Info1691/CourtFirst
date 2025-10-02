# tools/util.py
import re, time, random, html
from urllib.parse import urlencode, quote_plus, urlparse
import requests
from bs4 import BeautifulSoup

HDRS = {
    "User-Agent": "CourtFirstBot/0.1 (+github actions; requests)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def sleep_jitter(min_s=1.0, max_s=2.0):
    time.sleep(random.uniform(min_s, max_s))

class FetchError(RuntimeError): pass

def http_get(url, params=None, timeout=20, allow_redirects=True):
    try:
        r = requests.get(url, params=params, timeout=timeout, headers=HDRS, allow_redirects=allow_redirects)
        # DuckDuckGo rate-limit is 202; treat 429/5xx as retryable upstream.
        if r.status_code >= 400:
            raise FetchError(f"GET {url} -> {r.status_code}")
        return r.text
    except requests.RequestException as e:
        raise FetchError(f"GET {url} failed: {e}")

_ROMAN = r"(?:[ivxlcdm]+\.?\s*)"
# crude normalizers to match titles across sites
def norm_title(t: str) -> str:
    t = html.unescape(t or "").strip()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("’", "'").replace("–","-").replace("—","-")
    t = re.sub(r"\([^)]*\)", "", t)           # drop parenthetical
    t = re.sub(r"\b(in|the|of|and|&)\b", lambda m:m.group(0).lower(), t)
    return t

def plausible_match(page_text: str, title: str, citation: str) -> bool:
    T = norm_title(title).lower()
    H = norm_title(BeautifulSoup(page_text, "html.parser").get_text(" ")).lower()
    ok_title = all(w in H for w in [w for w in T.split() if len(w) > 2][:4]) if T else True
    ok_cite  = (citation and citation.lower() in H) or True
    return ok_title and ok_cite

def ddg_first(query: str) -> str|None:
    # HTML endpoint (lite) to avoid JS
    q = {"q": query, "t":"h_", "ia":"web"}
    html_txt = http_get("https://duckduckgo.com/html/", params=q)
    soup = BeautifulSoup(html_txt, "html.parser")
    for a in soup.select("a.result__a"):
        href = a.get("href","")
        # strip DDG redirect if present
        if href.startswith("/l/?kh=") and "uddg=" in href:
            from urllib.parse import parse_qs
            try:
                uddg = parse_qs(urlparse(href).query).get("uddg", [None])[0]
                href = uddg or href
            except Exception:
                pass
        return href
    return None

# ---------- JerseyLaw helpers ----------
def jlib_search_url(title:str, citation:str=""):
    return "https://www.jerseylaw.je/judgments/?"+urlencode({"k": f"{title} {citation}".strip()})

def jlib_pick_direct_from_results(html_txt:str) -> str|None:
    soup = BeautifulSoup(html_txt, "html.parser")
    # result cards with anchor to judgment page
    for a in soup.select("a[href*='/judgments/']"):
        href = a.get("href")
        if href and "/judgments/" in href:
            return "https://www.jerseylaw.je"+href if href.startswith("/") else href
    return None

def jlib_extract_pdf(html_txt:str) -> str|None:
    soup = BeautifulSoup(html_txt, "html.parser")
    # “View PDF” button
    a = soup.find("a", string=re.compile(r"View\s+PDF", re.I))
    if a and a.get("href"):
        href = a["href"]
        return "https://www.jerseylaw.je"+href if href.startswith("/") else href
    # sometimes a direct link with .pdf
    a = soup.find("a", href=re.compile(r"\.pdf$", re.I))
    if a and a.get("href"):
        href = a["href"]
        return "https://www.jerseylaw.je"+href if href.startswith("/") else href
    return None

def jlib_find(title:str, citation:str="") -> str|None:
    # search → first judgment page → pdf if present
    html_res = http_get(jlib_search_url(title, citation))
    direct = jlib_pick_direct_from_results(html_res)
    if not direct:
        return None
    page = http_get(direct)
    if not plausible_match(page, title, citation):
        return None
    pdf = jlib_extract_pdf(page)
    return pdf or direct

# ---------- BAILII helpers ----------
def bailii_search_url(title:str, citation:str=""):
    q = f'{title}'.strip()
    return "https://www.bailii.org/cgi-bin/sino_search_1.cgi?"+urlencode({"query": q})

def bailii_pick_direct_from_results(html_txt:str) -> str|None:
    soup = BeautifulSoup(html_txt, "html.parser")
    # typical results: ordered list with <a href="/ew/cases/...html">
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href: 
            continue
        if href.startswith("/"): href = "https://www.bailii.org"+href
        # prefer case page
        if re.search(r"/\w\w/.*/\d{4}/\d+\.html?$", href):
            return href
    return None

def bailii_extract_pdf(html_txt:str) -> str|None:
    soup = BeautifulSoup(html_txt, "html.parser")
    a = soup.find("a", href=re.compile(r"\.pdf$", re.I))
    if a and a.get("href"):
        href = a["href"]
        return "https://www.bailii.org"+href if href.startswith("/") else href
    return None

def bailii_find(title:str, citation:str="") -> str|None:
    html_res = http_get(bailii_search_url(title, citation))
    direct = bailii_pick_direct_from_results(html_res)
    if not direct:
        return None
    page = http_get(direct)
    if not plausible_match(page, title, citation):
        return None
    pdf = bailii_extract_pdf(page)
    return pdf or direct

def pick_best_url(title, citation):
    # try Jersey first for Jersey citations; otherwise try both
    try:
        u = jlib_find(title, citation)
        if u: return u
    except FetchError:
        pass
    sleep_jitter(0.6, 1.2)
    try:
        u = bailii_find(title, citation)
        if u: return u
    except FetchError:
        pass
    sleep_jitter(0.6, 1.2)
    # fallback: ddg with site filter
    for site in ("site:jerseylaw.je/judgments", "site:bailii.org"):
        hit = ddg_first(f'{title} {citation} {site}')
        if hit:
            try:
                page = http_get(hit)
                if plausible_match(page, title, citation):
                    # try to upgrade to PDF if link is a case page
                    if "jerseylaw.je" in hit:
                        pdf = jlib_extract_pdf(page)
                        return pdf or hit
                    if "bailii.org" in hit:
                        pdf = bailii_extract_pdf(page)
                        return pdf or hit
                    return hit
            except FetchError:
                continue
    return None
