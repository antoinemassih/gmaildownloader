# reparse_csv.py
import csv
import argparse
from subject_parser import parse_trade_subject

CORE_COLS = [
    "parse_ok","trade_id","side","quantity_signed","quantity","symbol",
    "multiplier","option_expiry","strike","option_type","price",
    "underlying_mark","implied_vol","account_code","parse_notes"
]

BASE_EMAIL_COLS = [
    "message_id","thread_id","date_raw","date_iso","date_epoch",
    "from","to","subject"
]

def row_needs_fix(row: dict) -> bool:
    # If parse_ok says False (string form), or any core fields are empty/null → fix it
    pok = (row.get("parse_ok") or "").strip().lower()
    if pok in ("false", "", "0", "none", "null"):
        return True
    for k in ("trade_id","side","quantity","symbol","option_expiry","strike","option_type","price"):
        v = (row.get(k) or "").strip()
        if v == "" or v.lower() in ("none", "null"):
            return True
    return False

def ensure_columns(fieldnames):
    # Guarantee our output has exactly BASE + CORE (in that order).
    # If input had extras, they are preserved after these.
    seen = set()
    out = []
    for c in BASE_EMAIL_COLS + CORE_COLS:
        if c not in seen:
            out.append(c)
            seen.add(c)
    for c in fieldnames:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out

def main():
    ap = argparse.ArgumentParser(description="Reparse bad rows in a trades CSV using improved parser.")
    ap.add_argument("--in", dest="in_csv", required=True, help="Input CSV (existing export).")
    ap.add_argument("--out", dest="out_csv", required=True, help="Output cleaned CSV.")
    ap.add_argument("--report", dest="report_csv", default="reparse_failures.csv",
                    help="CSV of rows that still fail after reparse.")
    ap.add_argument("--only-bad", action="store_true",
                    help="Only re-parse rows detected as bad; otherwise re-parse ALL rows.")
    args = ap.parse_args()

    total = 0
    fixed = 0
    still_bad = 0

    with open(args.in_csv, "r", newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        fieldnames = ensure_columns(reader.fieldnames or [])
        rows_out = []
        failures = []

        for row in reader:
            total += 1
            need = row_needs_fix(row) or (not args.only_bad)

            if need:
                parsed = parse_trade_subject(row.get("subject", ""))

                # Update parsed columns
                for k in CORE_COLS:
                    row[k] = parsed.get(k)

                if parsed["parse_ok"]:
                    fixed += 1
                else:
                    still_bad += 1
                    failures.append(row.copy())
            rows_out.append(row)

    with open(args.out_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    if failures:
        with open(args.report_csv, "w", newline="", encoding="utf-8") as frep:
            writer = csv.DictWriter(frep, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failures)

    print(f"[reparse] scanned={total}, reparsed={'ALL' if not args.only_bad else 'BAD_ONLY'}")
    print(f"[reparse] fixed_ok={fixed}, still_bad={still_bad}")
    print(f"[reparse] output -> {args.out_csv}")
    if failures:
        print(f"[reparse] failures -> {args.report_csv}")

if __name__ == "__main__":
    main()
