# prepare_trades_flatfiles.py
import csv
import sys
import argparse
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from pathlib import Path

# ---- helpers ----

NS = uuid.NAMESPACE_URL  # for stable deterministic UUIDs

def stable_id(*parts: str) -> str:
    key = "|".join("" if p is None else str(p) for p in parts)
    return str(uuid.uuid5(NS, key))

def parse_bool(s):
    if s is None:
        return None
    s = str(s).strip().lower()
    if s in ("true","t","1","yes","y"):
        return True
    if s in ("false","f","0","no","n"):
        return False
    return None

def parse_decimal(s, field, errs, rownum):
    if s in (None, "", "null", "None"):
        return None
    try:
        return Decimal(str(s))
    except (InvalidOperation, ValueError):
        errs.append(f"[row {rownum}] invalid decimal for {field}: {s}")
        return None

def parse_int(s, field, errs, rownum):
    if s in (None, "", "null", "None"):
        return None
    try:
        return int(s)
    except ValueError:
        errs.append(f"[row {rownum}] invalid int for {field}: {s}")
        return None

def parse_date(s, field, errs, rownum):
    if not s:
        return None
    try:
        # support YYYY-MM-DD or ISO with time/offset; we only keep date
        if "T" in s:
            return datetime.fromisoformat(s).date().isoformat()
        return date.fromisoformat(s).isoformat()
    except Exception:
        errs.append(f"[row {rownum}] invalid date for {field}: {s}")
        return None

def normalize_asset_class(symbol, is_option, fut_root_symbol):
    # naive asset-class guess; adjust as you like
    if is_option:
        # Option on index, ETF, equity, or future?
        if symbol.startswith("/"):            # e.g., /ES options in your file still show symbol "/ES"
            return "FUT_OPT"
        if symbol in ("SPX", "NDX", "RUT", "VIX"):
            return "INDEX"
        if symbol.isupper() and len(symbol) <= 5:
            return "EQUITY_ETF"
        return "UNKNOWN"
    else:
        if symbol.startswith("/"):
            return "FUT"
        return "EQUITY_ETF"

def root_from_symbol(symbol, fut_root_symbol, is_option):
    if is_option and symbol.startswith("/"):
        # for futures options keep the future root if provided
        return fut_root_symbol or symbol
    # for equity/index options, root is usually the symbol itself
    return symbol

# ---- main transform ----

def main():
    ap = argparse.ArgumentParser(description="Prepare normalized flatfiles from raw trade CSV.")
    ap.add_argument("--input", required=True, help="Path to raw trades CSV")
    ap.add_argument("--out", required=True, help="Output directory for flatfiles")
    args = ap.parse_args()

    inp = Path(args.input)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    # containers
    accounts = {}    # broker_code -> {account_id, broker_code, display_name}
    instruments = {} # (symbol, asset_class) -> {instrument_id, symbol, asset_class}
    contracts = {}   # identity tuple -> {contract_id, ...}
    trades = []      # list of normalized trade rows
    errors = []      # list of error messages

    required_cols = {
        "message_id","date_iso","is_option","trade_id","side","qty_signed","qty_abs",
        "symbol","contract_multiplier","price","account"
    }

    with inp.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        missing = required_cols - set(rdr.fieldnames or [])
        if missing:
            print(f"ERROR: CSV missing columns: {', '.join(sorted(missing))}", file=sys.stderr)
            sys.exit(1)

        for i, row in enumerate(rdr, start=2):  # header is line 1
            row_errs = []

            # Parse basics
            broker_code = (row.get("account") or "").strip()
            if not broker_code:
                row_errs.append(f"[row {i}] missing account/broker_code")

            is_option = parse_bool(row.get("is_option"))
            side = (row.get("side") or "").strip().upper()
            if side not in ("BUY","SELL"):
                row_errs.append(f"[row {i}] invalid side: {row.get('side')}")

            qty_abs = parse_int(row.get("qty_abs"), "qty_abs", row_errs, i)
            qty_signed = parse_int(row.get("qty_signed"), "qty_signed", row_errs, i)
            if qty_abs is None or qty_abs < 0:
                row_errs.append(f"[row {i}] qty_abs missing or negative")
            if qty_signed is None:
                row_errs.append(f"[row {i}] qty_signed missing")

            price = parse_decimal(row.get("price"), "price", row_errs, i)
            multiplier = parse_decimal(row.get("contract_multiplier"), "contract_multiplier", row_errs, i)

            symbol = (row.get("symbol") or "").strip()
            fut_root_symbol = (row.get("fut_root_symbol") or "").strip() or None
            option_type = (row.get("option_type") or "").strip().upper() or None

            expiry_date = parse_date(row.get("expiry_date"), "expiry_date", row_errs, i) if is_option else None
            strike = parse_decimal(row.get("strike"), "strike", row_errs, i) if is_option else None

            dt_iso = row.get("date_iso")
            trade_dt = None
            try:
                trade_dt = datetime.fromisoformat(dt_iso) if dt_iso else None
            except Exception:
                row_errs.append(f"[row {i}] invalid date_iso: {dt_iso}")

            underlying_mark = parse_decimal(row.get("underlying_mark"), "underlying_mark", row_errs, i)
            impl_vol = parse_decimal(row.get("impl_vol"), "impl_vol", row_errs, i)

            broker_trade_id = (row.get("trade_id") or "").strip() or None
            message_id = (row.get("message_id") or "").strip() or None
            subject = (row.get("subject") or "").strip() or None

            # account
            if broker_code and broker_code not in accounts:
                accounts[broker_code] = {
                    "account_id": stable_id("acct", broker_code),
                    "broker_code": broker_code,
                    "display_name": None,
                    "created_at": None,  # leave empty for DB default
                }
            acct_id = accounts.get(broker_code, {}).get("account_id")

            # instrument
            asset_class = normalize_asset_class(symbol, bool(is_option), fut_root_symbol)
            inst_key = (symbol, asset_class)
            if inst_key not in instruments:
                instruments[inst_key] = {
                    "instrument_id": stable_id("inst", symbol, asset_class),
                    "symbol": symbol,
                    "asset_class": asset_class,
                }
            inst_id = instruments[inst_key]["instrument_id"]

            # contract
            root = root_from_symbol(symbol, fut_root_symbol, bool(is_option))
            identity = (
                inst_id,
                "1" if is_option else "0",
                option_type or "",
                expiry_date or "",
                str(strike) if strike is not None else "",
                root or "",
                str(multiplier) if multiplier is not None else "",
            )
            if identity not in contracts:
                contracts[identity] = {
                    "contract_id": stable_id("ctrt", *identity),
                    "instrument_id": inst_id,
                    "is_option": bool(is_option),
                    "option_type": option_type,
                    "expiry_date": expiry_date,
                    "strike": str(strike) if strike is not None else None,
                    "root": root,
                    "multiplier": str(multiplier) if multiplier is not None else None,
                    "exchange_code": None,
                }
            contract_id = contracts[identity]["contract_id"]

            # trade record
            if not row_errs:
                # prefer broker_trade_id; also compute a trade_hash for idempotency if broker id missing
                natural_key = "|".join([
                    broker_code or "",
                    contract_id or "",
                    side or "",
                    str(qty_abs or ""),
                    str(price or ""),
                    trade_dt.isoformat() if trade_dt else "",
                    message_id or "",
                ])
                trade_hash = stable_id("tradehash", natural_key)
                trades.append({
                    "trade_id": stable_id("trade", broker_trade_id or trade_hash),
                    "broker_trade_id": broker_trade_id,
                    "trade_hash": trade_hash if broker_trade_id is None else None,
                    "account_id": acct_id,
                    "contract_id": contract_id,
                    "side": side,
                    "qty": qty_abs,
                    "price": str(price) if price is not None else None,
                    "cashflow_per_unit": None,  # derive later in DB/view if desired
                    "dt": trade_dt.isoformat() if trade_dt else None,
                    "is_synthetic": "false",     # all raw fills here are real fills
                    "message_id": message_id,
                    "subject": subject,
                })
            else:
                errors.extend(row_errs)

    # ---- write flatfiles ----
    def write_csv(path: Path, rows: list[dict], fieldnames: list[str]):
        if not rows:
            return
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k) for k in fieldnames})

    # accounts
    accounts_rows = list(accounts.values())
    write_csv(
        outdir / "accounts.csv",
        accounts_rows,
        ["account_id","broker_code","display_name","created_at"],
    )

    # instruments
    instruments_rows = list(instruments.values())
    write_csv(
        outdir / "instruments.csv",
        instruments_rows,
        ["instrument_id","symbol","asset_class"],
    )

    # contracts
    contracts_rows = list(contracts.values())
    write_csv(
        outdir / "contracts.csv",
        contracts_rows,
        ["contract_id","instrument_id","is_option","option_type","expiry_date","strike","root","multiplier","exchange_code"],
    )

    # trades
    trade_fields = [
        "trade_id","broker_trade_id","trade_hash","account_id","contract_id","side",
        "qty","price","cashflow_per_unit","dt","is_synthetic","message_id","subject"
    ]
    write_csv(outdir / "trades_prepared.csv", trades, trade_fields)

    # errors
    if errors:
        with (outdir / "errors.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["error"])
            for e in errors:
                w.writerow([e])

    # ---- console report ----
    print("=== Prep Report ===")
    print(f"Accounts:   {len(accounts_rows)}")
    print(f"Instruments:{len(instruments_rows)}")
    print(f"Contracts:  {len(contracts_rows)}")
    print(f"Trades OK:  {len(trades)}")
    print(f"Errors:     {len(errors)}")
    if errors:
        print(f"See: {outdir / 'errors.csv'}")
    print(f"Flatfiles written to: {outdir.resolve()}")

if __name__ == "__main__":
    main()
