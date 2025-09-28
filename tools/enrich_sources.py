import csv, os, re, json
from urllib.parse import quote_plus
from tools.util import http_get, sleep_jitter, save_json

JERSEY_SEARCH = "https://www.jerseylaw.je/judgments/pages/search.aspx?query={q}"
BAILII_SEARCH = "https://www.bailii.org/cgi-bin/lucy_search_1?query={q}"

def first_href(html: str, domain_hint: str):
    # very light-weight link picker; we prefer first match on the target domain/structure
    import re
    # JerseyLaw judgment URLs often contain "/judgments/" and ".aspx"
    if "jerseylaw.je" in domain_hint:
        m = re.search(r'href="([^"]+/judgments/[^"]+\.aspx)"', html, re.I)
        if m: return "https://www.jerseylaw.je" + m.group(1) if m.group(1).startswith("/") else m.group(1)
    # BAILII judgments often under /ewhc/, /ewca/, /ukpc/, etc.
    m = re.search(r'href="(https?://www\.bailii\.org/[^"]+/(ewhc|ewca|uksc|ukpc|ewhc[^/]*|sc|pc)[^"]+\.html)"', html, re.I)
    if m: return m.group(1)
    # Fallback: first bailii link
    m = re.search(r'href="(https?://www\.bailii\.org/[^"]+\.html)"', html, re.I)
    return m.group(1) if m else None

def search_url(case_name: str, jurisdiction: str):
    q = quote_plus(case_name)
    if (jurisdiction or "").strip().lower() == "jersey":
        return JERSEY_SEARCH.format(q=q), "jerseylaw.je"
    return BAILII_SEARCH.format(q=q), "bailii.org"

def main():
    os.makedirs("out/html", exist_ok=True)
    rows = []
    with open("data/cases.csv", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            rows.append(row)

    enriched = []
    for i, r in enumerate(rows, 1):
        case_name = (r.get("case_name") or r.get("name") or "").strip()
        jur = (r.get("jurisdiction") or "").strip()
        url = (r.get("source_url") or "").strip()
        found = url

        if not case_name:
            enriched.append({**r, "source_url": url, "source_resolved": False, "note": "missing case_name"})
            continue

        if not url:
            s_url, hint = search_url(case_name, jur)
            try:
                html = http_get(s_url)
                candidate = first_href(html, hint)
                if candidate:
                    found = candidate
                sleep_jitter(1.0, 0.7)
            except Exception as e:
                enriched.append({**r, "source_url": url, "source_resolved": False, "note": f"search error: {e}"})
                continue

        enriched.append({**r, "source_url": found or url, "source_resolved": bool(found)})

    save_json("out/sources.json", enriched)

if __name__ == "__main__":
    main()
