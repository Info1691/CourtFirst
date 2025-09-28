import os, json
from tools.util import http_get, sleep_jitter, safe_filename

def main():
    os.makedirs("out/html", exist_ok=True)
    data = json.load(open("out/sources.json", encoding="utf-8"))

    fetched = []
    for item in data:
        url = (item.get("source_url") or "").strip()
        cid = (item.get("case_id") or item.get("case_name") or "case").strip()
        if not url:
            item["fetched"] = False
            fetched.append(item)
            continue
        try:
            html = http_get(url)
            fname = f"out/html/{safe_filename(cid)}.html"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(html)
            item["html_path"] = fname
            item["fetched"] = True
            sleep_jitter(1.0, 0.7)
        except Exception as e:
            item["fetched"] = False
            item["fetch_error"] = str(e)
        fetched.append(item)

    with open("out/fetched.json", "w", encoding="utf-8") as f:
        json.dump(fetched, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
