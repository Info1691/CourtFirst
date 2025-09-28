#!/usr/bin/env python3
import argparse
from pathlib import Path

def extract_lines(source_file: Path, start: int, end: int, output_file: Path):
    """Extract lines [start:end] (inclusive) from a text file and save them."""
    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    with source_file.open("r", encoding="utf-8") as f:
        lines = f.readlines()

    if start < 1 or end > len(lines):
        raise ValueError(f"Line range {start}-{end} out of bounds (file has {len(lines)} lines)")

    selected = [line.strip() for line in lines[start - 1:end]]

    # Write to output file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as out:
        for line in selected:
            if line:  # skip empty lines
                out.write(line + "\n")

    print(f"✅ Extracted {len(selected)} lines from {source_file} → {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Extract a specific line range from a .txt file")
    parser.add_argument("--source", required=True, help="Path to the source .txt file")
    parser.add_argument("--start", type=int, required=True, help="Start line number (1-based)")
    parser.add_argument("--end", type=int, required=True, help="End line number (inclusive)")
    parser.add_argument("--out", default="out/cases_raw.txt", help="Output file path")
    args = parser.parse_args()

    extract_lines(Path(args.source), args.start, args.end, Path(args.out))

if __name__ == "__main__":
    main()
