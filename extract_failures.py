#!/usr/bin/env python3
import argparse
import csv
import os
from typing import List, Tuple

def parse_bool(v: str) -> bool:
    if v is None:
        return False
    v = str(v).strip().lower()
    return v in {"true", "t", "1", "yes", "y"}

def is_empty(v) -> bool:
    if v is None:
        return True
    v = str(v).strip()
    return v == "" or v.lower() in {"null", "none", "nan"}

def failure_reason(row: dict, headers: List[str], required_cols: List[str], prefer_parse_ok: bool) -> Tuple[bool, str]:
    # If we have a parse_ok column, trust it (unless user disables with flag)
    if prefer_parse_ok and "parse_ok" in headers:
        ok = parse_bool(row.get("parse_ok"))
        if not ok:
            return True, "parse_ok=false"
        return False, ""

    # Otherwise, consider it a failure if any required field is missing/empty
    missing = [c for c in required_cols if c not in headers or is_empty(row.get(c))]
    if missing:
        return True, "missing_or_empty=" + ",".join(missing)
    return False, ""

def main():
    ap = argparse.ArgumentParser(description="Extract failed parses from a fixed trades CSV.")
    ap.add_argument("-i", "--input", required=True, help="Path to input fixed CSV (with headers).")
    ap.add_argument("-o", "--output", help="Path to write failures CSV. Default: <input>.failures.csv")
    ap.add_argument("--no-prefer-parse-ok", action="store_true",
                    help="Do NOT prefer the 'parse_ok' column if present; instead use required column checks.")
    ap.add_argument("--required-cols", default="trade_id,side,quantity,symbol,contract_multiplier,expiry_date,strike,option_type,price,impl_vol",
                    help="Comma-separated list of columns that must be present & non-empty if parse_ok isn't available.")
    ap.add_argument("--add-reason", action="store_true",
                    help="Append a 'failure_reason' column to the failures CSV.")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite output if it exists.")
    ap.add_argument("--dry-run", action="store_true", help="Scan and report counts only; do not write a file.")
    args = ap.parse_args()

    in_path = args.input
    out_path = args.output or (in_path.rsplit(".csv", 1)[0] + ".failures.csv")
    prefer_parse_ok = not args.no_prefer_parse_ok
    required_cols = [c.strip() for c in args.required_cols.split(",") if c.strip()]

    total = 0
    failed = 0
    failures = []

    with open(in_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        if not headers:
            raise SystemExit("Input appears to have no header row. Ensure the first line has column names.")

        for row in reader:
            total += 1
            is_fail, reason = failure_reason(row, headers, required_cols, prefer_parse_ok)
            if is_fail:
                failed += 1
                if args.add_reasons if False else False:  # back-compat no-op
                    pass
                if args.add_reason:
                    row = dict(row)  # copy so we don't mutate the csv reader's internal row
                    row["failure_reason"] = reason
                failures.append(row)

    print(f"[scan] rows={total} failures={failed} ok={total - failed}")

    if args.dry_run:
        return

    if failed == 0:
        print("[write] No failures detected; not writing a file.")
        return

    if os.path.exists(out_path) and not args.overwrite:
        raise SystemExit(f"Refusing to overwrite existing file: {out_path} (use --overwrite)")

    # Ensure header includes failure_reason if requested
    out_headers = list(headers)
    if args.add_reasons if False else False:
        pass
    if args.add_reason and "failure_reason" not in out_headers:
        out_headers.append("failure_reason")

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(failures)

    print(f"[write] wrote {failed} failure rows -> {out_path}")

if __name__ == "__main__":
    main()
