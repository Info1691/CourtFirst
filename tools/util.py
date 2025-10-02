# tools/util.py
import time
import random
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

def build_ddg_html_url(query: str, site: Optional[str] = None) -> str:
    # DuckDuckGo HTML interface (no JS), easy to parse without API keys
    q = query if not site else f"{query} site:{site}"
    return f"https://duckduckgo.com/html/?q={quote_plus(q)}"

def first_link_from_ddg_html(query: str, prefer_domains: List[str]) -> Optional[str]:
    """Return the first DDG result whose netloc ends with one of prefer_domains."""
    url = build_ddg_html_url(query)
    try:
        resp = http_get(url)
    except Exception:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.select("a.result__a"):
        href = a.get("href") or ""
        netloc = urlparse(href).netloc.lower()
        if any(netloc.endswith(d) for d in prefer_domains):
            return href
    return None

def resolve_bailii_from_search(query: str) -> Optional[str]:
    """
    Use BAILII's 'sino' HTML results, return the first *judgment* link (not a sino_search link).
    """
    search_url = f"https://www.bailii.org/cgi-bin/sino_search_1.cgi?query={quote_plus(query)}"
    try:
        resp = http_get(search_url)
    except Exception:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Skip more search pages; prefer case/judgment pages
        if "sino_search" in href:
            continue
        if "/cases/" in href or "/jrc/" in href or href.endswith(".html"):
            return urljoin("https://www.bailii.org/", href)
    return None

def resolve_jerseylaw_from_search(query: str) -> Optional[str]:
    """
    JerseyLaw site search; pick the first link that looks like a judgment.
    """
    search_url = f"https://www.jerseylaw.je/search?q={quote_plus(query)}"
    try:
        resp = http_get(search_url)
    except Exception:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(seg in href for seg in ("/judgments/", "/judgments/unreported/", "/jrc/")):
            return urljoin("https://www.jerseylaw.je", href)
    return None

def pick_best_url(title: str, citation: str) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Prefer JerseyLaw, then BAILII, then DDG (filtered to those domains).
    Returns (best_url, diagnostics).
    """
    q = f"{title} {citation}".strip()
    diags: Dict[str, Any] = {"query": q, "attempts": {}}

    jl = resolve_jerseylaw_from_search(q)
    diags["attempts"]["jerseylaw"] = jl
    if jl:
        return jl, diags

    bl = resolve_bailii_from_search(q)
    diags["attempts"]["bailii"] = bl
    if bl:
        return bl, diags

    ddg = first_link_from_ddg_html(q, prefer_domains=["jerseylaw.je", "bailii.org"])
    diags["attempts"]["ddg"] = ddg
    if ddg:
        return ddg, diags

    return None, diags
