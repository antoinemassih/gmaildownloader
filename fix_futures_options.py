#!/usr/bin/env python3
import csv
import re
from datetime import datetime, timezone
from pathlib import Path

IN_FILE = "trades_clean.csv"
OUT_FILE = "trades_fixed_final.csv"

# Futures roots you actually trade; extend as needed
FUT_ROOTS = ["/ES", "/NQ", "/MNQ", "/MES", "/CL", "/GC", "/YM", "/RTY", "/EW", "/QN"]
# Common multipliers for futures options noted in subjects (e.g., "1/50")
FUT_MULTIPLIER_MAP = {
    "1/50": 50,
    "1/20": 20,
    "1/10": 10,
}

MONTHS = {
    'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
    'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12
}

def parse_expiry(tokens):
    """
    Handles both equity-style '17 MAY 24' and single-day weekly '11 JUN 24'
    Returns ISO date (YYYY-MM-DD) or '' if unknown.
    """
    # tokens e.g. ['17', 'MAY', '24'] or ['6', 'MAY', '24']
    try:
        if len(tokens) >= 3:
            d = int(re.sub(r'\D','',tokens[0]))
            m = MONTHS.get(tokens[1].upper(), None)
            y_raw = tokens[2]
            y = int("20"+re.sub(r'\D', '', y_raw)[-2:])
            if m:
                return datetime(y, m, d, tzinfo=timezone.utc).date().isoformat()
    except Exception:
        pass
    return ""

def pick_fut_root(subject):
    for root in sorted(FUT_ROOTS, key=len, reverse=True):
        if root in subject:
            return root
    return ""

def extract_contract_multiplier(subject, existing):
    """
    Prefer explicit subject hints like '1/50', then accept existing only if sensible.
    Defaults to 100 for equity options when '100' is present in the subject.
    """
    # 1) Subject hint like '1/50', '1/20', etc.
    m = re.search(r'\b(\d+/\d+)\b', subject)
    if m:
        frac = m.group(1)
        mapped = FUT_MULTIPLIER_MAP.get(frac)
        if mapped:
            return float(mapped)

    # 2) Equity option default
    if " 100 " in subject or re.search(r"\b100\b", subject):
        return 100.0

    # 3) Existing value only if it’s clearly a real multiplier (>=10)
    try:
        cm = float(existing)
        if cm >= 10:
            return cm
    except:
        pass

    # 4) Fallback: unknown
    return ""

def extract_expiry(subject, existing_expiry, is_option):
    """
    If expiry missing or suspicious, try to re-derive from subject.
    Equity style: '17 MAY 24', weekly style: '11 JUN 24', etc.
    Futures weekly sometimes embeds the *trade* date; we keep what parser set
    unless it's empty.
    """
    if existing_expiry:
        return existing_expiry

    if not is_option:
        return ""

    # Try patterns like '(\d{1,2}) (JAN|FEB|...) (\d{2})'
    m = re.search(r"\b(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{2})\b", subject, re.I)
    if m:
        return parse_expiry([m.group(1), m.group(2), m.group(3)])

    return ""

def extract_price(subject, existing):
    """
    Use parsed price if present; else fallback to '@.xx' segment.
    """
    try:
        if existing != "" and existing is not None:
            return float(existing)
    except:
        pass
    m = re.search(r"@\s*([0-9]*\.?[0-9]+)", subject)
    if m:
        return float(m.group(1))
    return ""

def normalize_row(row):
    subj = row.get("subject", "") or ""
    is_option = str(row.get("is_option", "")).strip().lower() in ("true", "1", "yes")

    # Fix symbol & fut root
    fut_root = pick_fut_root(subj)
    symbol = row.get("symbol", "") or ""
    if fut_root and not symbol.startswith(fut_root):
        # For futures options we prefer the root in symbol column (e.g., '/ES')
        symbol = fut_root

    # Contract multiplier
    cm = extract_contract_multiplier(subj, row.get("contract_multiplier", ""))
    # Expiry
    expiry = extract_expiry(subj, row.get("expiry_date",""), is_option)

    # Price
    price = extract_price(subj, row.get("price", ""))

    # Option bits
    option_type = row.get("option_type","") or ("PUT" if " PUT " in f" {subj} " else ("CALL" if " CALL " in f" {subj} " else ""))
    strike = row.get("strike","")
    try:
        strike = float(strike) if strike not in ("", None) else ""
    except:
        strike = ""

    # Impl vol
    impl_vol = row.get("impl_vol","")
    try:
        impl_vol = float(impl_vol) if impl_vol not in ("", None) else ""
    except:
        # Try extract from subject: 'IMPL VOL=13.15%'
        m = re.search(r"IMPL\s+VOL\s*=\s*([0-9]*\.?[0-9]+)", subj, re.I)
        if m:
            impl_vol = float(m.group(1))
        else:
            impl_vol = ""

    # qty_signed / qty_abs as numbers
    qty_signed = row.get("qty_signed","") or ""
    qty_abs = row.get("qty_abs","") or ""
    try:
        qty_signed = int(qty_signed) if qty_signed != "" else ""
    except:
        # recover from prefixes like '+1' or '-3'
        m = re.search(r"([+-]?\d+)", str(qty_signed))
        qty_signed = int(m.group(1)) if m else ""
    try:
        qty_abs = int(qty_abs) if qty_abs != "" else abs(qty_signed) if isinstance(qty_signed, int) else ""
    except:
        m = re.search(r"(\d+)", str(qty_abs))
        qty_abs = int(m.group(1)) if m else (abs(qty_signed) if isinstance(qty_signed, int) else "")

    # side normalization
    side = (row.get("side","") or "").upper()
    if not side and isinstance(qty_signed, int):
        side = "BUY" if qty_signed > 0 else "SELL" if qty_signed < 0 else ""

    # Mark parse_ok
    parse_ok = True
    fail_reason = ""

    # sanity for options rows
    if is_option:
        if not symbol:
            parse_ok = False; fail_reason = (fail_reason + "; " if fail_reason else "") + "missing symbol"
        if not expiry:
            parse_ok = False; fail_reason = (fail_reason + "; " if fail_reason else "") + "missing expiry"
        if not option_type:
            parse_ok = False; fail_reason = (fail_reason + "; " if fail_reason else "") + "missing option_type"
        if strike == "":
            parse_ok = False; fail_reason = (fail_reason + "; " if fail_reason else "") + "missing strike"

    # write back
    row["symbol"] = symbol
    row["fut_root_symbol"] = fut_root
    row["contract_multiplier"] = cm
    row["expiry_date"] = expiry
    row["price"] = price
    row["option_type"] = option_type
    row["strike"] = strike
    row["impl_vol"] = impl_vol
    row["side"] = side
    row["qty_signed"] = qty_signed
    row["qty_abs"] = qty_abs
    row["parse_ok"] = str(parse_ok)
    row["fail_reason"] = fail_reason

    return row

def main():
    in_path = Path(IN_FILE)
    out_path = Path(OUT_FILE)

    with in_path.open(newline='', encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        # Make sure we preserve/ensure these headers
        fieldnames = list(reader.fieldnames or [])
        for must in [
            "fut_root_symbol","contract_multiplier","expiry_date","price",
            "option_type","strike","impl_vol","parse_ok","fail_reason"
        ]:
            if must not in fieldnames:
                fieldnames.append(must)

        with out_path.open("w", newline='', encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                writer.writerow(normalize_row(row))

    print(f"✅ Wrote {out_path} from {in_path}")

if __name__ == "__main__":
    main()
