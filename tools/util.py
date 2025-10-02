# tools/util.py
import time
import random
import re
from typing import Optional, Tuple, Dict, Any, List
from urllib.parse import quote_plus, urljoin, urlparse
import requests
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept-Language": "en"})

def sleep_jitter(min_s: float = 1.5, max_s: float = 3.5) -> None:
    time.sleep(random.uniform(min_s, max_s))

def http_get(url: str, timeout: float = 20.0) -> requests.Response:
    resp = SESSION.get(url, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    return resp

def norm_text(s: str) -> str:
    return " ".join((s or "").split())

_ROMAN_RE = re.compile(r"^(?ixv   # flags INSENSITIVE, VERBOSE via inline
    [ivxlcdm]+   # roman numerals
)$")

def is_roman_page_marker(s: str) -> bool:
    s = (s or "").strip()
    return bool(_ROMAN_RE.match(s))

def build_ddg_html_url(query: str, site: Optional[str] = None) -> str:
    # Use the no-JS HTML interface (no API keys, stable for scraping)
    q = query
    if site:
        q = f'{query} site:{site}'
    return f"https://duckduckgo.com/html/?q={quote_plus(q)}"

def first_link_from_ddg_html(query: str, prefer_domains: List[str]) -> Optional[str]:
    """
    Scrape DuckDuckGo HTML results and return the first link whose domain is in prefer_domains.
    """
    url = build_ddg_html_url(query)
    try:
        resp = http_get(url)
    except Exception:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.select("a.result__a"):
        href = a.get("href") or ""
        # DDG HTML already gives direct destination links
        netloc = urlparse(href).netloc.lower()
        if any(netloc.endswith(d) for d in prefer_domains):
            return href
    return None

def resolve_bailii_from_search(query: str) -> Optional[str]:
    """
    Query Bailii's 'sino' search, then pull the first judgment link (not the sino_search page).
    """
    # Bailii 'sino' HTML search page – returns results we can parse.
    search_url = f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={quote_plus(query)}"
    try:
        resp = http_get(search_url)
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    # Target judgment links, which typically contain /cases/ or /jrc/ and are not another sino_search link.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "sino_search" in href:
            continue
        if "/cases/" in href or "/jrc/" in href or href.endswith(".html"):
            return urljoin("https://www.bailii.org/", href)
    return None

def resolve_jerseylaw_from_search(query: str) -> Optional[str]:
    """
    JerseyLaw recently moved off the old Results.aspx; the site search works at /search?q=.
    We open the HTML and pick the first link pointing into /judgments/ or /judgments/unreported/ or /jrc/.
    """
    # site search results (HTML)
    search_url = f"https://www.jerseylaw.je/search?q={quote_plus(query)}"
    try:
        resp = http_get(search_url)
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    # Newer site uses result cards with links to judgments
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(seg in href for seg in ["/judgments/", "/judgments/unreported/", "/jrc/"]):
            # Make absolute
            return urljoin("https://www.jerseylaw.je", href)
    return None

def pick_best_url(title: str, citation: str) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Try JerseyLaw first (if likely Jersey), then Bailii, then DDG fallback.
    Returns (best_url, diagnostics)
    """
    q_base = title
    if citation:
        q_base = f"{title} {citation}"

    diags: Dict[str, Any] = {"query": q_base, "attempts": {}}

    # 1) JerseyLaw
    jl = resolve_jerseylaw_from_search(q_base)
    diags["attempts"]["jerseylaw"] = jl
    if jl:
        return jl, diags

    # 2) BAILII
    bl = resolve_bailii_from_search(q_base)
    diags["attempts"]["bailii"] = bl
    if bl:
        return bl, diags

    # 3) DDG (prefer Jersey/Bailii)
    ddg = first_link_from_ddg_html(q_base, prefer_domains=["jerseylaw.je", "bailii.org"])
    diags["attempts"]["ddg"] = ddg
    if ddg:
        return ddg, diags

    return None, diags
