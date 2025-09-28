import os, json, re
from bs4 import BeautifulSoup  # needs beautifulsoup4 in requirements.txt

NEUTRAL_RE = re.compile(r"\[(\d{4})\]\s+[A-Z][A-Z0-9]+(?:\s+\d+)?", re.I)

def parse_meta(html: str):
    soup = BeautifulSoup(html, "html.parser")

    # title
    title = None
    for sel in ["h1", "title", "h2"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = el.get_text(" ", strip=True)
            break

    # date (very heuristic)
    date = None
    date_el = soup.find(text=re.compile(r"\b\d{1,2}\s+\w+\s+\d{4}\b"))  # e.g., 12 March 2019
    if date_el:
        m = re.search(r"\b\d{1,2}\s+\w+\s+\d{4}\b", date_el)
        if m: date = m.group(0)

    # neutral citation (heuristic)
    neutral = None
    if title:
        m = NEUTRAL_RE.search(title)
        if m:
            neutral = m.group(0)
    if not neutral:
        body_text = soup.get_text(" ", strip=True)[:2000]
        m = NEUTRAL_RE.search(body_text)
        if m: neutral = m.group(0)

    # court (best-effort: BAILII often shows in breadcrumbs or h2/h3)
    court = None
    crumbs = soup.select("div.breadcrumbs, nav.breadcrumb, ul.breadcrumb li, .crumbs a")
    if crumbs:
        court = " / ".join([c.get_text(" ", strip=True) for c in crumbs if c.get_text(strip=True)])[:200]

    return {
        "title": title,
        "neutral_citation": neutral,
        "date": date,
        "court": court
    }

def main():
    fetched = json.load(open("out/fetched.json", encoding="utf-8"))
    enriched = []
    for item in fetched:
        meta = {}
        if item.get("fetched") and item.get("html_path") and os.path.exists(item["html_path"]):
            html = open(item["html_path"], encoding="utf-8").read()
            meta = parse_meta(html)
        enriched.append({**item, **meta})

    with open("out/cases_enriched.json", "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
