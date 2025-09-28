#!/usr/bin/env python3
"""
Shared helpers for CourtFirst tools.
"""

import json
import os
import random
import time
from typing import Any, Dict, Optional

import requests


def ensure_dir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def save_json(path: str, data: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def sleep_jitter(lo: float = 0.5, hi: float = 1.25) -> None:
    time.sleep(random.uniform(lo, hi))


def http_get(url: str, user_agent: str = "CourtFirstBot/1.0", timeout: int = 30) -> Optional[str]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    # Only keep HTML-like responses
    ctype = r.headers.get("Content-Type", "")
    if "html" not in ctype.lower():
        return r.text  # Still return; caller can decide
    return r.text
