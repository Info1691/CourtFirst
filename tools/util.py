#!/usr/bin/env python3
"""
Shared utilities for CourtFirst.
"""
from __future__ import annotations
import csv, json, os, re, time, typing as t
from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup

# ----------------------------
# Files & paths
# ----------------------------
ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
OUT_DIR = os.path.join(ROOT, "out")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

CASES_CSV = os.path.join(DATA_DIR, "cases.csv")
CORPUS_JSONL = os.path.join(OUT_DIR, "corpus.jsonl")
CANDIDATES_JSON = os.path.join(OUT_DIR, "breach_candidates.json")
BREACHES_JSON = os.path.join(OUT_DIR, "breaches.json")

# ----------------------------
# Data models
# ----------------------------
@dataclass
class CaseRow:
    case_id: str
    title: str
    source_url: str
    local_text: str | None = None
    jurisdiction: str | None = None

def read_cases_csv(path: str = CASES_CSV) -> list[CaseRow]:
    """
    Accepts either a `source_url` column or a `url` column.
    Only `case_id` and one of {source_url|url} are required.
    Optional columns: title, local_text, jurisdiction.
    """
    rows: list[CaseRow] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])
        if "case_id" not in headers:
            raise ValueError("cases.csv missing required column: 'case_id'")
        if not (("source_url" in headers) or ("url" in headers)):
            raise ValueError("cases.csv must include either 'source_url' or 'url' column")

        for r in reader:
            case_id = (r.get("case_id") or "").strip()
            if not case_id:
                # skip blank rows silently
                continue
            source_url = (r.get("source_url") or r.get("url") or "").strip()
            if not source_url and not (r.get("local_text") or "").strip():
                # need *either* a fetchable URL or inline text
                raise ValueError(f"Row for case_id='{case_id}' has neither source_url/url nor local_text")

            rows.append(
                CaseRow(
                    case_id=case_id,
                    title=(r.get("title") or case_id).strip(),
                    source_url=source_url,
                    local_text=(r.get("local_text") or "").strip() or None,
                    jurisdiction=(r.get("jurisdiction") or "").strip() or None,
                )
            )
    return rows

# ----------------------------
# HTTP & HTML
# ----------------------------
_UA = "CourtFirstBot/0.1 (+https://example.invalid; contact: admin@example.invalid)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _UA})

def fetch_html(url: str, timeout: int = 30) -> str:
    resp = SESSION.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text

def extract_readable_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()
    main = soup.find(["article", "main"]) or soup.body
    text = main.get_text("\n") if main else soup.get_text("\n")
    return normalize_ws(text)

# ----------------------------
# Text helpers
# ----------------------------
def normalize_ws(s: str) -> str:
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def window_around(text: str, match_start: int, match_end: int, chars: int = 240) -> str:
    a = max(0, match_start - chars)
    b = min(len(text), match_end + chars)
    snippet = text[a:b].strip()
    snippet = re.sub(r"\s+", " ", snippet)
    return snippet

def save_json(obj: t.Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def append_jsonl(obj: t.Any, path: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def sleep_ms(ms: int) -> None:
    time.sleep(ms / 1000.0)
