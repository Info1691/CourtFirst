#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ultra-conservative metadata parser.
- Extracts <title> and first <h1>, if present.
- Writes a JSON list; no invented content.
"""

import argparse
import re
from pathlib import Path
from tools.util import save_json

TITLE_RE = re.compile(rb"<title[^>]*>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(rb"<h1[^>]*>(.*?)</h1>", re.I | re.S)

def textify(b: bytes) -> str:
    t = re.sub(rb"<[^>]+>", b"", b or b"", flags=re.S)
    return (t.decode("utf-8", "ignore")).strip()

def extract_bits(html: bytes):
    title = textify(TITLE_RE.search(html).group(1)) if TITLE_RE.search(html) else ""
    h1 = textify(H1_RE.search(html).group(1)) if H1_RE.search(html) else ""
    return {"title": title, "h1": h1}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--html", dest="html_dir", required=True)
    p.add_argument("--out", dest="out_json", required=True)
    return p.parse_args()

def main():
    args = parse_args()
    html_dir = Path(args.html_dir)
    items = []
    for f in sorted(html_dir.glob("*.html")):
        try:
            bits = extract_bits(f.read_bytes())
            items.append({"file": f.name, **bits})
        except Exception as e:
            items.append({"file": f.name, "error": str(e)})
    save_json(args.out_json, items)
    print(f"Wrote metadata for {len(items)} files -> {args.out_json}")

if __name__ == "__main__":
    main()
