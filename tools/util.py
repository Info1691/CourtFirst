import time, random, json, re
from dataclasses import dataclass
from typing import Optional, Dict, Any
import urllib.parse
import urllib.request

UA = "CourtFirstBot/0.1 (+contact: maintainer; purpose: legal research)"

def http_get(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def sleep_jitter(base: float = 1.0, spread: float = 0.5):
    time.sleep(base + random.random() * spread)

def safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:200]

def save_json(path: str, data: Any):
    import os, json
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
