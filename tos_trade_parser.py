# tos_trade_parser.py
import re
import calendar
from datetime import date

# Map "JAN" -> 1, ..., "DEC" -> 12
MONTHS = {mon.upper(): idx for idx, mon in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1
)}

SIDE_MAP = {"BOT": "BUY", "BUY": "BUY", "SOLD": "SELL", "SELL": "SELL"}

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date | None:
    """weekday: Mon=0..Sun=6; n: 1..5. Return the nth weekday date or None."""
    count = 0
    for d in range(1, 32):
        try:
            dt = date(year, month, d)
        except ValueError:
            break
        if dt.weekday() == weekday:
            count += 1
            if count == n:
                return dt
    return None

def _parse_weekly_expiry(exp_month_yr: str, wk: str, weekday_hint: str | None) -> str | None:
    # exp_month_yr like "MAY 24"
    parts = exp_month_yr.strip().split()
    if len(parts) != 2:
        return None
    mon_txt, yr_txt = parts[0].upper(), parts[1]
    if mon_txt not in MONTHS:
        return None
    month = MONTHS[mon_txt]
    year = 2000 + int(yr_txt) if len(yr_txt) == 2 else int(yr_txt)

    # default weekly options expire Friday unless "(Thursday)" is present
    weekday = 3 if (weekday_hint and "THURSDAY" in weekday_hint.upper()) else 4  # Thu=3, Fri=4
    n = int(wk)
    dt = _nth_weekday(year, month, weekday, n)
    return dt.isoformat() if dt else None

def parse_trade_subject(subject: str) -> dict:
    s = subject.strip().strip('"')
    s = re.sub(r'\btIP\b\s*', '', s, flags=re.IGNORECASE)  # some subjects start with "tIP"
    s = re.sub(r'\s+', ' ', s)

    out = {
        "parse_ok": False,
        "trade_id": None,
        "side": None,
        "qty_signed": None,
        "qty_abs": None,
        "symbol": None,
        "contract_multiplier": None,
        "expiry_date": None,
        "strike": None,
        "option_type": None,
        "price": None,
        "underlying_mark": None,
        "impl_vol": None,
        "account": None,
    }

    # trade id
    m = re.search(r'#(\d+)', s)
    if m:
        out["trade_id"] = m.group(1)

    # side + quantity
    m = re.search(r'\b(BOT|BUY|SOLD|SELL)\s+([+-]?\d+)\b', s)
    if not m:
        return out
    side_raw, qty_txt = m.group(1), m.group(2)
    side = SIDE_MAP.get(side_raw)
    if not side:
        return out
    try:
        qty_val = int(qty_txt)
    except ValueError:
        return out
    qty_abs = abs(qty_val)
    qty_signed = qty_abs if side == "BUY" else -qty_abs
    out["side"] = side
    out["qty_abs"] = qty_abs
    out["qty_signed"] = qty_signed

    # symbol + contract multiplier
    # Equity/Index example: "SPX 100 ..."
    # Futures example:      "/ESM24 1/50 MAY 24 (Wk2) ..."
    m = re.search(r'\b([A-Z]{1,6}|/[A-Z]{1,3}[A-Z0-9]{0,4})\s+(\d+(?:/\d+)?)\b', s)
    if m:
        out["symbol"] = m.group(1).lstrip('/')  # normalize "/ESM24" -> "ESM24"
        out["contract_multiplier"] = m.group(2)
    else:
        # Fallback: at least capture a symbol if present before expiry
        m2 = re.search(r'\b([A-Z]{1,6})\s+(?:\d{1,2}\s+[A-Z]{3}\s+\d{2})\b', s)
        if m2:
            out["symbol"] = m2.group(1)

    # expiry (path A): explicit day like "17 MAY 24"
    exp_iso = None
    m = re.search(r'\b(\d{1,2})\s+([A-Z]{3})\s+(\d{2})\b', s)
    if m:
        d, mon, yy = int(m.group(1)), m.group(2).upper(), int(m.group(3))
        if mon in MONTHS:
            try:
                exp_iso = date(2000 + yy, MONTHS[mon], d).isoformat()
            except ValueError:
                exp_iso = None

    # expiry (path B): "MAY 24 (Thursday) (Wk2)" or "MAY 24 (Wk2)"
    if not exp_iso:
        m = re.search(
            r'\b([A-Z]{3}\s+\d{2})\s+(?:\((Thursday)\)\s+)?\((?:Wk|Wkly|Weeklys)\s*(\d)\)',
            s, re.IGNORECASE
        )
        if m:
            exp_iso = _parse_weekly_expiry(m.group(1), m.group(3), m.group(2))

    out["expiry_date"] = exp_iso

    # strike + type
    m = re.search(r'\b(\d+(?:\.\d+)?)\s+(PUT|CALL)\b', s)
    if m:
        out["strike"] = float(m.group(1))
        out["option_type"] = m.group(2).upper()

    # price after '@'
    m = re.search(r'@\s*([0-9]+(?:\.[0-9]+)?|\.[0-9]+)(?=\s*[A-Z]|,|$)', s)
    if m:
        out["price"] = float(m.group(1))

    # underlying mark like "MARK=520.79" or "EDGXMARK=" / "CBOEMARK=" / "NASDAQ BXMARK="
    m = re.search(r'[A-Z ]*MARK=([0-9]*\.?[0-9]+)\b', s)
    if m:
        out["underlying_mark"] = float(m.group(1))

    # implied vol "IMPL VOL=13.29%"
    m = re.search(r'IMPL\s+VOL=\s*([0-9]+(?:\.[0-9]+)?)\s*%', s)
    if m:
        out["impl_vol"] = float(m.group(1))

    # account
    m = re.search(r'ACCOUNT\s+([*0-9A-Z]+)', s)
    if m:
        out["account"] = m.group(1)

    # core completeness
    core_ok = all([
        out["trade_id"], out["side"], out["qty_abs"],
        out["symbol"], out["strike"], out["option_type"], out["price"]
    ])
    out["parse_ok"] = bool(core_ok)
    return out
