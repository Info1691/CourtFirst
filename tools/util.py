# tools/util.py
import csv, json, os, sys, time, math, random, pathlib
from dataclasses import dataclass
from typing import Iterable, Dict, Any, Optional, List, Tuple
import urllib.parse as _u
import urllib.request as _r
import ssl

UA = "CourtFirstBot/1.0 (+https://github.com) python-urllib/3.x"
CTX = ssl.create_default_context()

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

class Heartbeat:
    """Periodically prints progress and writes a small on-disk heartbeat file."""
    def __init__(self, total:int, every:int=25, out_dir:str="out", name:str="enrich"):
        self.total = total
        self.every = max(1, every)
        self.out_dir = pathlib.Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.name = name
        self.done = 0
        self.t0 = time.time()
        self.file = self.out_dir / f"{name}_heartbeat.log"

    def tick(self, delta:int=1):
        self.done += delta
        if self.done % self.every == 0:
            elapsed = time.time() - self.t0
            rate = self.done / max(1.0, elapsed)
            msg = f"[{now_ts()}] {self.name}: {self.done}/{self.total} (~{rate:.2f}/s)"
            print(msg, flush=True)
            try:
                self.file.write_text(msg + "\n", encoding="utf-8")
            except Exception as e:
                print(f"(heartbeat write failed: {e})", flush=True)

def read_csv(path:str) -> List[Dict[str,str]]:
    rows=[]
    with open(path, newline='', encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append({k:(v or "").strip() for k,v in row.items()})
    return rows

def write_csv(path:str, rows:List[Dict[str,Any]], fieldnames:List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in fieldnames})

def save_json(path:str, obj:Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path:str, default:Any):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default

def sleep_jitter(base:float=0.7, spread:float=0.6):
    time.sleep(base + random.random()*spread)

def http_get(url:str, timeout:float=30.0, headers:Optional[Dict[str,str]]=None) -> Tuple[int, bytes, Dict[str,str]]:
    req=_r.Request(url, headers={"User-Agent":UA, **(headers or {})})
    with _r.urlopen(req, timeout=timeout, context=CTX) as resp:
        status=resp.status
        data=resp.read()
        hdrs={k.lower():v for k,v in resp.headers.items()}
        return status, data, hdrs

def try_get(url:str, retries:int=3, backoff:float=1.5) -> Optional[str]:
    for i in range(retries):
        try:
            status, data, _ = http_get(url)
            if 200 <= status < 300:
                return data.decode("utf-8", "replace")
        except Exception as e:
            pass
        sleep_jitter(backoff*(i+1), 0.3)
    return None

def ddg_search_url(query:str) -> str:
    # Use HTML results page; we won’t parse DDG HTML here — we only record the search URL as a fallback
    q=_u.quote_plus(query)
    return f"https://duckduckgo.com/?q={q}"

def clean_title(title:str) -> str:
    # drop trailing pinpoint page spans like “..., 12-45, 77-81”
    if not title: return title
    # keep commas in names; only strip trailing page-ranges block
    return title.rstrip().rstrip(",").strip()

def build_queries(title:str, citation:str, jurisdiction:str) -> List[str]:
    t=clean_title(title)
    c=(citation or "").strip()
    j=(jurisdiction or "").strip()
    base=[t]
    if c: base.append(f"{t} {c}")
    if j: base.append(f"{t} {j}")
    return list(dict.fromkeys([q for q in base if q]))  # dedupe, keep order
