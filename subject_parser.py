# subject_parser.py
# Robust parser for thinkorswim alert email subjects (equities/ETFs and some futures options).

from __future__ import annotations
import re
from typing import Optional, Dict

# number pattern that accepts "1", "1.23", ".45"
NUM = r"(?:\d+(?:\.\d+)?|\.\d+)"

# Some subjects include an exchange code (CBOE, EDGX, NASDAQ BX, etc.) right before MARK=
# Allow single- or multi-word prefixes ending with MARK=
MARK_PREFIX = r"(?:[A-Z]+(?:\s+[A-Z]+)*)?MARK="

MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def _iso_date(day: str, mon: str, yy: str) -> Optional[str]:
    mon = (mon or "").upper()
    if mon not in MONTHS:
        return None
    try:
        y = 2000 + int(yy)
        m = MONTHS[mon]
        d = int(day)
        return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        return None

def _normalize_side(side_raw: str) -> str:
    # Collapse BOT/SOLD to BUY/SELL
    return "BUY" if side_raw == "BOT" else "SELL"

# --- Regexes -----------------------------------------------------------------

# Equities / ETFs / Index options (SPX, SPY, QQQ, NVDA, META, MSFT, etc.)
EQUITY_RE = re.compile(
    rf"""
    ^\#(?P<trade_id>\d+)\s+
    (?:[A-Za-z]+\s+)*?                     # optional noise like 'tIP'
    (?P<side_raw>BOT|SOLD)\s+
    (?P<qty>[+\-]?\d+)\s+
    (?P<instrument>[A-Z]+)\s+              # e.g., NVDA, QQQ, SPX
    (?P<contract_code>\d+)
    (?:\s+\(Weeklys\))?\s+
    (?P<day>\d{{1,2}})\s+(?P<mon>[A-Z]{{3}})\s+(?P<yy>\d{{2}})\s+
    (?P<strike>{NUM})\s+
    (?P<option_type>PUT|CALL)\s*@\s*(?P<price>{NUM})
    \s*                                     # sometimes no space before MARK
    {MARK_PREFIX}(?P<mark>{NUM})\s+
    IMPL\ VOL=(?P<iv>{NUM})%\s*,\s*ACCOUNT\s+(?P<account>.+?)\s*$
    """,
    re.VERBOSE,
)

# Futures options (best-effort).
FUT_OPT_RE = re.compile(
    rf"""
    ^\#(?P<trade_id>\d+)\s+
    (?:[A-Za-z]+\s+)*?                         # optional noise like 'tIP'
    (?P<side_raw>BOT|SOLD)\s+
    (?P<qty>[+\-]?\d+)\s+
    (?P<future_ct>/[A-Z]{{1,3}}[FGHJKMNQUVXZ]\d{{2}})\s+
    (?P<contract_code>\d+/\d+)\s+              # e.g., 1/20, 1/50
    (?P<mon>[A-Z]{{3}})\s+(?P<yy>\d{{2}})
    (?:\s+\([^)]+\))*\s+                       # (Monday) (Wk4) etc.
    (?P<opt_root>/[A-Z0-9]+)\s+
    (?P<strike>{NUM})\s+(?P<option_type>PUT|CALL)\s*@\s*(?P<price>{NUM})
    \s*
    {MARK_PREFIX}(?P<mark>{NUM})\s+
    IMPL\ VOL=(?P<iv>{NUM})%\s*,\s*ACCOUNT\s+(?P<account>.+?)\s*$
    """,
    re.VERBOSE,
)

# --- Public API ---------------------------------------------------------------

def parse_trade_subject(subject: str) -> Dict[str, Optional[str]]:
    """
    Parse a thinkorswim subject line.

    Returns a dict with:
      parsed (bool), parse_ok (bool alias), trade_id, side, qty (abs int), signed_qty (int),
      instrument, contract_code, option_date (YYYY-MM-DD when present),
      strike (float), option_type, price (float), mark (float),
      implied_vol (float), account (str)

    On failure, returns parsed=False (and parse_ok=False) with fields as None.
    """
    subject = (subject or "").strip()

    def _empty(parsed: bool = False) -> Dict[str, Optional[str]]:
        return {
            "parsed": parsed,
            "parse_ok": parsed,  # alias for backward compatibility
            "trade_id": None,
            "side": None,
            "qty": None,
            "signed_qty": None,
            "instrument": None,
            "contract_code": None,
            "option_date": None,
            "strike": None,
            "option_type": None,
            "price": None,
            "mark": None,
            "implied_vol": None,
            "account": None,
        }

    # Try equity/ETF/index format
    m = EQUITY_RE.match(subject)
    if m:
        g = m.groupdict()
        side = _normalize_side(g["side_raw"])
        qty_raw = int(g["qty"])
        qty_abs = abs(qty_raw)
        signed_qty = qty_abs if side == "BUY" else -qty_abs

        option_date = _iso_date(g["day"], g["mon"], g["yy"])
        strike = _to_float(g["strike"])
        price = _to_float(g["price"])
        mark = _to_float(g["mark"])
        iv = _to_float(g["iv"])

        return {
            "parsed": True,
            "parse_ok": True,
            "trade_id": g["trade_id"],
            "side": side,
            "qty": qty_abs,
            "signed_qty": signed_qty,
            "instrument": g["instrument"],
            "contract_code": g["contract_code"],  # usually "100"
            "option_date": option_date,
            "strike": strike,
            "option_type": g["option_type"],
            "price": price,
            "mark": mark,
            "implied_vol": iv,
            "account": g["account"].strip(),
        }

    # Try futures options format (best effort)
    m = FUT_OPT_RE.match(subject)
    if m:
        g = m.groupdict()
        side = _normalize_side(g["side_raw"])
        qty_raw = int(g["qty"])
        qty_abs = abs(qty_raw)
        signed_qty = qty_abs if side == "BUY" else -qty_abs

        strike = _to_float(g["strike"])
        price = _to_float(g["price"])
        mark = _to_float(g["mark"])
        iv = _to_float(g["iv"])

        instrument = g["future_ct"].lstrip("/")  # e.g., 'NQZ23', 'ESZ23'

        return {
            "parsed": True,
            "parse_ok": True,
            "trade_id": g["trade_id"],
            "side": side,
            "qty": qty_abs,
            "signed_qty": signed_qty,
            "instrument": instrument,
            "contract_code": g["contract_code"],   # e.g., "1/20"
            "option_date": None,                   # no specific day present
            "strike": strike,
            "option_type": g["option_type"],
            "price": price,
            "mark": mark,
            "implied_vol": iv,
            "account": g["account"].strip(),
        }

    # No match
    return _empty(False)
