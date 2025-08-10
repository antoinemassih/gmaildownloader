#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
import asyncio
import csv
import hashlib
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple

from gmaildownloader.db.uow import uow
from gmaildownloader.db.engine import set_statement_timeout
from gmaildownloader.db.repositories import (
    AccountRepo,
    InstrumentRepo,
    ContractRepo,
    TradeRepo,
)

# ---------- parsing helpers ----------

def parse_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "y", "yes"):
        return True
    if s in ("0", "false", "f", "n", "no"):
        return False
    return None

def booly(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "t", "y", "yes")

def parse_decimal(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "null":
        return None
    try:
        return str(Decimal(s))
    except (InvalidOperation, ValueError):
        return None

def parse_int(v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "null":
        return None
    try:
        return int(s)
    except ValueError:
        return None

def parse_dt_utc(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def clean_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None

def infer_asset_class(symbol: str) -> str:
    s = (symbol or "").upper()
    if s.startswith("/"):
        return "FUTURE"
    if s in ("SPX", "NDX", "RUT", "VIX"):
        return "INDEX"
    if s in ("SPY", "QQQ", "IWM"):
        return "ETF"
    return "EQUITY"

def compute_trade_hash(payload: Dict[str, Any]) -> str:
    parts = [
        str(payload.get("account_id") or ""),
        str(payload.get("contract_id") or ""),
        str(payload.get("side") or ""),
        str(payload.get("qty") or ""),
        str(payload.get("price") or ""),
        str(payload.get("dt") or ""),
        str(payload.get("message_id") or ""),
        str(payload.get("is_synthetic") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

# ---------- CSV mapping tuned for your sample ----------

COLUMNS = {
    "message_id": "message_id",
    "thread_id": "thread_id",
    "date_iso": "dt",
    "subject": "subject",
    "is_option": "is_option",
    "trade_id": "broker_trade_id",
    "side": "side",
    "qty_abs": "qty",                 # use absolute fill size; direction from side
    "symbol": "symbol",
    "contract_multiplier": "multiplier",
    "expiry_date": "expiry_date",
    "strike": "strike",
    "option_type": "option_type",
    "price": "price",
    "underlying_mark": "underlying_mark",
    "impl_vol": "impl_vol",
    "account": "account_code",
    "fut_root_symbol": "root",        # helps futures/opts identity
    # ignored but present:
    "date_raw": None,
    "epoch": None,
    "from_email": None,
    "to_email": None,
    "parse_ok": None,
    "fail_reason": None,
}

def remap_row(row: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        norm = COLUMNS.get(k, "__IGNORE__")
        if norm and norm != "__IGNORE__":
            out[norm] = v
    return out

# ---------- ingest ----------

BATCH_SIZE = 1000
PROGRESS_EVERY = 500

async def ingest_csv(csv_path: Path, errors_path: Path) -> Tuple[int, int, int, List[Tuple[int, str]]]:
    assert csv_path.exists(), f"CSV not found: {csv_path}"

    total = 0
    created_trades = 0
    updated_trades = 0
    error_rows: List[Dict[str, Any]] = []
    error_summaries: List[Tuple[int, str]] = []

    account_cache: Dict[str, Any] = {}
    instrument_cache: Dict[tuple, Any] = {}
    contract_cache: Dict[tuple, Any] = {}

    print("Opening DB session...")
    async with uow() as session:
        accounts = AccountRepo(session)
        instruments = InstrumentRepo(session)
        contracts = ContractRepo(session)
        trades = TradeRepo(session)
        print("Initialized repositories. Starting ingest...")

        # Set per-session statement timeout on Postgres side (do not allow this to hang)
        try:
            print("Setting statement timeout on session...")
            await asyncio.wait_for(set_statement_timeout(session), timeout=5)
            print("Statement timeout set on session.")
        except Exception as e:
            print(f"Warning: could not set statement timeout (continuing): {e}")

        def contract_key(instr_id: str, is_option: bool, option_type, expiry_date, strike, root, multiplier):
            return (
                instr_id,
                bool(is_option),
                option_type or None,
                expiry_date or None,
                strike or None,
                root or None,
                multiplier or None,
            )

        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            raw_headers = reader.fieldnames or []
            print(f"CSV loaded. Columns: {len(raw_headers)}. Beginning row processing...")

            for row in reader:
                total += 1
                try:
                    if total == 1 or total % 100 == 1:
                        print(f"Processing row {total}...")
                    r = remap_row(row)

                    # account
                    broker_code = clean_str(r.get("account_code"))
                    if not broker_code:
                        raise ValueError("missing account")
                    acct = account_cache.get(broker_code)
                    if not acct:
                        acct = await asyncio.wait_for(accounts.upsert(broker_code, None), timeout=15)
                        account_cache[broker_code] = acct

                    # instrument
                    symbol = clean_str(r.get("symbol"))
                    if not symbol:
                        raise ValueError("missing symbol")
                    asset_class = infer_asset_class(symbol)
                    ikey = (symbol, asset_class)
                    instr = instrument_cache.get(ikey)
                    if not instr:
                        instr = await asyncio.wait_for(instruments.find_or_create(symbol, asset_class), timeout=15)
                        instrument_cache[ikey] = instr

                    # contract
                    is_option = booly(r.get("is_option") or "false")
                    option_type = clean_str(r.get("option_type")) if is_option else None
                    expiry_date = clean_str(r.get("expiry_date")) if is_option else None
                    strike = parse_decimal(r.get("strike")) if is_option else None

                    root = clean_str(r.get("root"))
                    multiplier = parse_decimal(r.get("multiplier")) or None
                    exchange_code = None

                    ckey = contract_key(
                        str(instr.instrument_id),
                        is_option,
                        option_type,
                        expiry_date,
                        strike,
                        root,
                        multiplier,
                    )
                    contract = contract_cache.get(ckey)
                    if not contract:
                        contract = await asyncio.wait_for(contracts.find_or_create(
                            instrument_id=str(instr.instrument_id),
                            is_option=is_option,
                            option_type=option_type,
                            expiry_date=expiry_date,
                            strike=strike,
                            root=root,
                            multiplier=multiplier,
                            exchange_code=exchange_code,
                        ), timeout=15)
                        contract_cache[ckey] = contract

                    # trade
                    side = (r.get("side") or "").upper()
                    if side not in ("BUY", "SELL"):
                        raise ValueError(f"invalid side {side!r}")

                    qty = parse_int(r.get("qty"))
                    if qty is None:
                        raise ValueError("missing qty")
                    price = parse_decimal(r.get("price"))
                    if price is None:
                        raise ValueError("missing/invalid price")

                    dt_iso = parse_dt_utc(r.get("dt"))
                    if dt_iso is None:
                        raise ValueError("invalid date_iso")

                    broker_trade_id = clean_str(r.get("broker_trade_id"))
                    message_id = clean_str(r.get("message_id"))
                    subject = clean_str(r.get("subject"))

                    payload = {
                        "broker_trade_id": broker_trade_id,
                        "trade_hash": None,
                        "account_id": str(acct.account_id),
                        "contract_id": str(contract.contract_id),
                        "side": side,
                        "qty": qty,
                        "price": price,
                        "cashflow_per_unit": None,
                        "dt": dt_iso,
                        "is_synthetic": False,
                        "message_id": message_id,
                        "subject": subject,
                    }

                    if not broker_trade_id:
                        payload["trade_hash"] = compute_trade_hash(payload)

                    trade, created = await asyncio.wait_for(trades.upsert(payload), timeout=20)
                    if created:
                        created_trades += 1
                    else:
                        updated_trades += 1

                    if total % PROGRESS_EVERY == 0:
                        ok_so_far = total - len(error_rows)
                        print(f"Progress: rows={total} ok={ok_so_far} errors={len(error_rows)}")

                    if total % BATCH_SIZE == 0:
                        try:
                            await session.flush()
                            await session.commit()
                            print(f"Committed batch at row {total}")
                        except Exception as e:
                            print(f"Batch commit failed at row {total}: {e}. Rolling back and continuing...")
                            try:
                                await session.rollback()
                            except Exception as re:
                                print(f"Rollback after batch commit failure also failed: {re}")

                except Exception as e:
                    # capture original CSV row plus error details
                    row_copy = dict(row)
                    row_copy["_row"] = total
                    row_copy["_error"] = f"{type(e).__name__}: {e}"
                    error_rows.append(row_copy)
                    error_summaries.append((total, row_copy["_error"]))
                    # IMPORTANT: reset failed transaction so we can continue
                    try:
                        await session.rollback()
                    except Exception as re:
                        print(f"Warning: rollback after row {total} failed: {re}")

        # final commit and write errors at the end (inside session scope)
        try:
            await session.commit()
            print("Final commit complete.")
        except Exception as e:
            print(f"Warning: final commit failed: {e}. Attempting rollback...")
            try:
                await session.rollback()
            except Exception as re:
                print(f"Warning: final rollback failed: {re}")

        # write errors at the end (outside the loop but inside session scope is fine)
        if error_rows:
            # ensure stable header order: original + our two columns
            headers = (raw_headers or list(error_rows[0].keys())) + ["_row", "_error"]
            # de-dup if already present
            headers = [h for i, h in enumerate(headers) if h is not None and h not in headers[:i]]
            with open(errors_path, "w", newline="", encoding="utf-8") as ef:
                writer = csv.DictWriter(ef, fieldnames=headers)
                writer.writeheader()
                for er in error_rows:
                    writer.writerow(er)
            print(f"Wrote error rows to {errors_path} ({len(error_rows)} rows)")

    return total, created_trades, updated_trades, error_summaries

# ---------- CLI ----------

def main():
    import argparse
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    parser = argparse.ArgumentParser(description="Ingest trades_fixed_final.csv into Supabase Postgres via Async SQLAlchemy DAL.")
    parser.add_argument("--csv", required=True, help="Path to trades_fixed_final.csv")
    parser.add_argument("--errors", default=f"ingest_errors_{ts}.csv", help="Path to write rows that failed ingest")
    args = parser.parse_args()

    # Warn only if neither explicit DB URL nor derivable Supabase creds exist
    has_explicit = bool(
        os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
    )
    has_derivable = bool(os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_DB_PASSWORD"))
    if not (has_explicit or has_derivable):
        print("WARNING: No DB URL env found. Provide SUPABASE_DB_URL/DATABASE_URL or SUPABASE_URL + SUPABASE_DB_PASSWORD before running.")

    total, created, updated, error_summaries = asyncio.run(
        ingest_csv(Path(args.csv), Path(args.errors))
    )

    # ---- console report ----
    err_count = len(error_summaries)
    ok = total - err_count
    rate = (err_count / total * 100.0) if total else 0.0

    print("\n=== Ingest Report ===")
    print(f"Rows read         : {total}")
    print(f"Trades created    : {created}")
    print(f"Trades updated    : {updated}")
    print(f"Error rows        : {err_count} ({rate:.2f}%)")
    if err_count:
        print(f"Error CSV         : {args.errors}")
        print("Sample errors (up to 10):")
        for r, msg in error_summaries[:10]:
            print(f"  - Row {r}: {msg}")
    print("=====================\n")

if __name__ == "__main__":
    main()
