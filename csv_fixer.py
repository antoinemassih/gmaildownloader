# csv_fixer.py
import argparse
import csv
import os
from tos_trade_parser import parse_trade_subject

def fix_csv(in_path: str, out_path: str, fail_path: str | None = None) -> tuple[int, int]:
    """
    Reads an existing CSV that contains a 'subject' column,
    reparses every row with parse_trade_subject, and writes a new CSV
    with updated/added columns.

    Returns (num_rows, num_fixed_failures)
    """
    needed_cols = [
        "parse_ok", "fail_reason",
        "trade_id", "side",
        "qty_signed", "qty_abs",
        "symbol", "contract_multiplier",
        "expiry_date", "strike",
        "option_type", "price",
        "underlying_mark", "impl_vol",
        "account",
    ]

    total = 0
    fails = 0

    with open(in_path, "r", encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin)
        if "subject" not in (reader.fieldnames or []):
            raise SystemExit("Input CSV must have a 'subject' header/column.")

        # Build output headers = all original + any missing parse columns
        out_headers = list(reader.fieldnames)
        for c in needed_cols:
            if c not in out_headers:
                out_headers.append(c)

        # Optional fail log
        fail_writer = None
        if fail_path:
            fail_headers = ["message_id", "subject", "fail_reason"]
            f = open(fail_path, "w", encoding="utf-8", newline="")
            fail_writer = csv.DictWriter(f, fieldnames=fail_headers)
            fail_writer.writeheader()

        with open(out_path, "w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=out_headers)
            writer.writeheader()

            for row in reader:
                total += 1
                subject = row.get("subject", "")
                parsed = parse_trade_subject(subject)

                # Write parsed fields into the row
                for k in needed_cols:
                    v = parsed.get(k)
                    # Convert values to strings for CSV (avoid 'None' strings)
                    if v is None:
                        row[k] = ""
                    else:
                        row[k] = str(v)

                writer.writerow(row)

                if not parsed.get("parse_ok"):
                    fails += 1
                    if fail_writer:
                        fail_writer.writerow({
                            "message_id": row.get("message_id", ""),
                            "subject": subject,
                            "fail_reason": parsed.get("fail_reason", ""),
                        })

        if fail_writer:
            fail_writer.fieldnames  # no-op to keep linter quiet
            fail_writer.writer  # ditto

    return total, fails


def main():
    ap = argparse.ArgumentParser(description="Re-parse subjects in an existing CSV and write a fixed CSV.")
    ap.add_argument("--in", dest="in_csv", required=True, help="Path to the input CSV with a 'subject' column.")
    ap.add_argument("--out", dest="out_csv", required=True, help="Path to write the fixed CSV.")
    ap.add_argument("--fail-log", dest="fail_log", default=None,
                    help="Optional CSV path to log rows that still fail to parse.")
    args = ap.parse_args()

    in_path = os.path.abspath(args.in_csv)
    out_path = os.path.abspath(args.out_csv)
    fail_path = os.path.abspath(args.fail_log) if args.fail_log else None

    total, fails = fix_csv(in_path, out_path, fail_path)
    print(f"Done. Rows processed: {total}, still failed: {fails}")
    print(f"Output: {out_path}")
    if fail_path:
        print(f"Fail log: {fail_path}")


if __name__ == "__main__":
    main()
