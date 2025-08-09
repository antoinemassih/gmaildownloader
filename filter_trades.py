#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

def to_bool(val):
    if val is None:
        return False
    s = str(val).strip().lower()
    if s in {"true", "1", "yes", "y", "t"}:
        return True
    if s in {"false", "0", "no", "n", "f", ""}:
        return False
    # Fallback: non-empty strings default to False unless explicitly truthy
    return False

def main():
    in_path = Path("trades_fixed.csv")
    out_path = Path("trades_fixed_final.csv")

    if not in_path.exists():
        print(f"Input not found: {in_path.resolve()}", file=sys.stderr)
        sys.exit(1)

    with in_path.open(newline="", encoding="utf-8") as fin, out_path.open("w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        if "parse_ok" not in reader.fieldnames:
            print("Column 'parse_ok' not found.", file=sys.stderr)
            sys.exit(2)

        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        kept = 0
        total = 0
        for row in reader:
            total += 1
            if to_bool(row.get("parse_ok")):
                writer.writerow(row)
                kept += 1

    print(f"Done. Kept {kept}/{total} rows -> {out_path}")

if __name__ == "__main__":
    main()