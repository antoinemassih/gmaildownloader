# subject_parser.py
import re
from datetime import datetime
from typing import Dict, Optional

_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "SEPT": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

# Handles things like:
# "#82180613275 BOT +1 SPX 100 (Weeklys) 11 JUN 24 5315 PUT @.15MARK=5354.70 IMPL VOL=13.15% , ACCOUNT *****750SCHW"
# "#66066620674 SOLD -12 SPY 100 17 MAY 24 500 PUT @.12MARK=520.79 IMPL VOL=13.29% , ACCOUNT *****0960TDA"
# Also allows "(Weeklys)/(Weekly/Weeklies)" token, + or - sign on qty, and short option types P/C.
_SUBJECT_RE = re.compile(r"""
    ^\s*\#?(?P<trade_id>\d+)\s+
    (?P<action>BOUGHT|BUY|BOT|SOLD|SELL|BTO|STO|BTC|STC)\s+
    (?P<qtysign>[+-])?(?P<quantity>\d+)\s+
    (?P<symbol>[A-Za-z0-9.\-]+)\s+
    (?P<code>\d{1,4})
    (?:\s+\((?:Weeklys?|Weeklies|Weekly)\)|\s+(?:Weeklys?|Weeklies|Weekly))?   # optional weekly token
    \s+(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]{3,})\s+(?P<year>\d{2,4})\s+
    (?P<strike>\d+(?:\.\d+)?)\s+
    (?P<otype>PUT|CALL|P|C)
    (?:\s*@\s*(?P<price>\d*\.?\d+))?                            # @.15 or @2.80
    (?:
        .*?MARK\s*=\s*(?P<mark>\d+(?:\.\d+)?)                   # MARK=5354.70
    )?
    (?:
        .*?\bIMPL\s*VOL\s*=\s*(?P<iv>\d*\.?\d+)%?               # IMPL VOL=13.29%
    )?
    (?:
        .*?\bACCOUNT\b[^\d]*(?P<account>[\*\w\-]+)              # *****0960TDA
    )?
    \s*$""", re.IGNORECASE | re.VERBOSE)

def _normalize_year(y: int) -> int:
    return y if y >= 100 else 2000 + y

def _month_to_int(m: str) -> Optional[int]:
    if not m:
        return None
    u = m.strip().upper()
    if u.startswith("SEPT"):
        u = "SEP"
    return _MONTHS.get(u[:3])

def parse_trade_subject(subject: str) -> Dict[str, Optional[str]]:
    """
    Returns DB-friendly fields (None when not parsed):
      parse_ok, subject_raw,
      trade_id, side (BUY/SELL),
      quantity_signed, quantity_abs,
      instrument, contract_code,
      option_expiry (YYYY-MM-DD), strike, option_type,
      fill_price, underlying_mark, implied_vol, account
    """
    out = {
        "parse_ok": False,
        "subject_raw": subject,

        "trade_id": None,
        "side": None,

        "quantity_signed": None,
        "quantity_abs": None,

        "instrument": None,
        "contract_code": None,

        "option_expiry": None,
        "strike": None,
        "option_type": None,

        "fill_price": None,
        "underlying_mark": None,
        "implied_vol": None,
        "account": None,
    }

    m = _SUBJECT_RE.search(subject or "")
    if not m:
        return out

    g = m.groupdict()

    # Side normalization
    action = (g.get("action") or "").upper()
    buy_actions = {"BOUGHT", "BUY", "BOT", "BTO", "BTC"}
    sell_actions = {"SOLD", "SELL", "STO", "STC"}
    side = "BUY" if action in buy_actions else ("SELL" if action in sell_actions else None)

    # Quantity (signed, abs); explicit sign wins, else infer from side
    qty = int(g.get("quantity") or "0")
    sgn = (g.get("qtysign") or "")
    if sgn == "-":
        qty = -qty
    elif sgn == "+":
        qty = +qty
    elif side == "SELL":
        qty = -qty
    qty_abs = abs(qty)

    # Expiry date
    day = int(g.get("day") or 1)
    mon = _month_to_int(g.get("month") or "")
    yr_raw = int(g.get("year") or 0)
    yr = _normalize_year(yr_raw) if yr_raw else None
    expiry_iso = None
    if mon and yr:
        try:
            expiry_iso = datetime(yr, mon, day).strftime("%Y-%m-%d")
        except ValueError:
            expiry_iso = None

    # Option type
    o = (g.get("otype") or "").upper()
    opt_type = "CALL" if o in ("CALL", "C") else ("PUT" if o in ("PUT", "P") else None)

    # Price, mark, iv
    price = g.get("price")
    mark = g.get("mark")
    iv = g.get("iv")

    # Fallbacks (be extra sure MARK / IV are found even if formats drift)
    if not mark:
        m2 = re.search(r"MARK\s*=\s*(\d+(?:\.\d+)?)", subject or "", re.IGNORECASE)
        if m2:
            mark = m2.group(1)
    if not iv:
        m3 = re.search(r"IMPL\s*VOL\s*=\s*(\d*\.?\d+)%?", subject or "", re.IGNORECASE)
        if m3:
            iv = m3.group(1)

    # Account (strip leading asterisks if present)
    account = g.get("account") or None
    if account:
        account = account.lstrip("*")

    out.update({
        "parse_ok": True,
        "trade_id": g.get("trade_id"),
        "side": side,

        "quantity_signed": qty,
        "quantity_abs": qty_abs,

        "instrument": (g.get("symbol") or "").upper(),
        "contract_code": g.get("code"),

        "option_expiry": expiry_iso,
        "strike": g.get("strike"),
        "option_type": opt_type,

        "fill_price": price,
        "underlying_mark": mark,
        "implied_vol": iv,
        "account": account,
    })
    return out
