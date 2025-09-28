#!/usr/bin/env python3
"""
Shared utilities. Strictly non-inventive:
- Only returns what servers give us.
- Saves exactly what we receive.
"""

import os
import json
import time
import random
import pathlib
from typing import Any, Optional, Dict

import requests


def ensure_dir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def save_json(path: str, data: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def sleep_jitter(lo: float = 0.75, hi: float = 1.75) -> None:
    """Jitter to be polite to servers."""
    time.sleep(random.uniform(lo, hi))


DEFAULT_UA = "CourtFirstBot/1.0 (+https://github.com/)"


def http_get(url: str, timeout: int = 30, headers: Optional[Dict[str, str]] = None) -> requests.Response:
    """
    Do a GET and return the Response object so caller can use:
      resp.status_code, resp.text, resp.url, resp.headers
    Raises for HTTP errors.
    """
    _headers = {"User-Agent": DEFAULT_UA, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"}
    if headers:
        _headers.update(headers)
    resp = requests.get(url, headers=_headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    return resp


def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)


def write_text(path: str, text: str) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def repo_root() -> str:
    return str(pathlib.Path(__file__).resolve().parents[1])
