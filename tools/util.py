#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import re
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional

USER_AGENT = (
    "CourtFirstBot/1.0 (+https://github.com/; contact: admin@example.com) "
    "Requests"
)

def http_get(url: str, session, *, timeout: int = 25) -> Tuple[int, str]:
    """GET a URL with a conservative UA and return (status_code, text)."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    return resp.status_code, resp.text

def sleep_jitter(base: float = 0.8) -> None:
    """Tiny polite delay between requests."""
    time.sleep(base)

def safe_filename(s: str, maxlen: int = 180) -> str:
    """Filesystem-safe filename from an identifier/URL."""
    s = re.sub(r"[^\w\-.]+", "_", s.strip())
    if len(s) > maxlen:
        s = s[: maxlen - 8] + "_" + hex(abs(hash(s)))[2:8]
    return s

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def save_json(obj, path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def read_csv(path: Path) -> Tuple[Dict[str, int], Iterable[list]]:
    """
    Minimal CSV reader (no external deps): returns (header_index, rows).
    Assumes UTF-8 and first line is header. Commas inside quotes are handled.
    """
    import csv
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return {}, []
    header = rows[0]
    hmap = {h.strip().lower(): i for i, h in enumerate(header)}
    return hmap, rows[1:]

def write_csv(header: Iterable[str], rows: Iterable[Iterable], path: Path) -> None:
    import csv
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(list(header))
        for r in rows:
            w.writerow(list(r))
