#!/usr/bin/env python3
"""
Upload a CSV (default: trades_fixed_final.csv) to a Supabase table using credentials from .env

Requirements (install in your venv):
  pip install supabase python-dotenv

Environment variables expected in .env:
  SUPABASE_URL=<your_supabase_url>
  SUPABASE_KEY=<your_service_role_or_anon_key_with_insert_perms>

Usage examples:
  python upload_to_supabase.py \
    --csv trades_fixed_final.csv \
    --table trades \
    --batch-size 500 \
    --upsert --on-conflict id

Notes:
- Upsert requires a unique constraint or primary key on the column(s) named in --on-conflict.
- If the table does not exist, this script will error. Create the table first or adjust schema accordingly.
"""
import argparse
import csv
import os
import sys
from pathlib import Path
from typing import List, Dict

from dotenv import load_dotenv
try:
    from supabase import create_client, Client
except Exception as e:
    print("supabase-py not installed. Run: pip install supabase python-dotenv", file=sys.stderr)
    raise


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload CSV to Supabase table")
    p.add_argument("--csv", dest="csv_path", default="trades_fixed_final.csv", help="Path to CSV file")
    p.add_argument("--table", dest="table", required=True, help="Target Supabase table name")
    p.add_argument("--batch-size", dest="batch_size", type=int, default=500, help="Insert batch size")
    p.add_argument("--upsert", dest="upsert", action="store_true", help="Use upsert instead of insert")
    p.add_argument("--on-conflict", dest="on_conflict", default=None, help="Comma-separated column(s) for ON CONFLICT when upserting")
    p.add_argument("--exclude-cols", dest="exclude_cols", default=None, help="Comma-separated columns to exclude from upload")
    p.add_argument("--include-cols", dest="include_cols", default=None, help="Comma-separated columns to include (if set, only these will be sent)")
    p.add_argument("--null-empty", dest="null_empty", action="store_true", default=True, help="Convert empty strings to null before upload (default: on)")
    return p.parse_args()


def chunked(rows: List[Dict], n: int):
    for i in range(0, len(rows), n):
        yield rows[i:i + n]


def main():
    args = parse_args()

    load_dotenv()  # loads from .env in CWD if present
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        print("Missing SUPABASE_URL/SUPABASE_KEY in environment (.env)", file=sys.stderr)
        sys.exit(2)

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path.resolve()}", file=sys.stderr)
        sys.exit(1)

    # Initialize client
    supabase: Client = create_client(url, key)

    # Read CSV fully (small-medium size). For very large CSVs, stream per batch.
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows: List[Dict] = list(reader)

    if not raw_rows:
        print("No rows to upload. Exiting.")
        return

    # Column selection
    include_set = None
    exclude_set = None
    if args.include_cols:
        include_set = {c.strip() for c in args.include_cols.split(",") if c.strip()}
    if args.exclude_cols:
        exclude_set = {c.strip() for c in args.exclude_cols.split(",") if c.strip()}

    def transform_row(r: Dict) -> Dict:
        if include_set is not None:
            r = {k: v for k, v in r.items() if k in include_set}
        if exclude_set:
            r = {k: v for k, v in r.items() if k not in exclude_set}
        return r

    rows: List[Dict] = [transform_row(r) for r in raw_rows]

    # Normalize empty strings to None (NULL in DB)
    if args.null_empty:
        for r in rows:
            for k, v in list(r.items()):
                if isinstance(v, str) and v == "":
                    r[k] = None

    # Coerce booleans by name (e.g., is_option)
    def to_bool(val):
        if val is None:
            return None
        s = str(val).strip().lower()
        if s in {"true", "1", "yes", "y", "t"}:
            return True
        if s in {"false", "0", "no", "n", "f"}:
            return False
        return val

    for r in rows:
        if "is_option" in r:
            r["is_option"] = to_bool(r.get("is_option"))

    # Debug: show final columns
    final_cols = sorted(rows[0].keys())
    print(f"Columns to upload ({len(final_cols)}): {final_cols}")

    print(f"Preparing to upload {len(rows)} rows to table '{args.table}' (batch={args.batch_size})")

    total = 0
    for batch in chunked(rows, args.batch_size):
        if args.upsert:
            q = supabase.table(args.table).upsert(batch)
            if args.on_conflict:
                # supabase-py v2 supports on_conflict
                q = q.on_conflict(args.on_conflict)
            resp = q.execute()
        else:
            resp = supabase.table(args.table).insert(batch).execute()

        # Basic error handling
        # resp.data may be None for insert; resp counts aren't always provided
        if getattr(resp, "error", None):
            print(f"Batch failed: {resp.error}", file=sys.stderr)
            sys.exit(3)
        total += len(batch)

    print(f"Done. Uploaded {total} rows to '{args.table}'.")


if __name__ == "__main__":
    main()
