#!/usr/bin/env python3
"""
Aggregate all fills for the same (account, symbol, expiry_date, strike, option_type, contract_multiplier)
into ONE round trip object with full leg metadata and cashflow-based P&L.

Usage:
    python build_round_trips.py trades_fixed_final.csv round_trips.json

Requires:
    pip install python-dateutil
"""

import csv
import json
import sys
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from dateutil import parser as dtparser
from datetime import datetime, time, timezone

# Safe decimal helper
def D(x):
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))

def parse_dt(s):
    return dtparser.isoparse(s)

def vwap(total_qty, total_value):
    if total_qty == 0:
        return None
    return (total_value / D(total_qty)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

def fmt_dec(x):
    if x is None:
        return None
    return format(x, 'f').rstrip('0').rstrip('.') if '.' in format(x, 'f') else format(x, 'f')

def normalize_multiplier(symbol, fut_root_symbol, cm):
    """Correct common contract multipliers."""
    if fut_root_symbol == "/ES":
        return 50.0
    if symbol in ("SPX", "SPY"):
        return 100.0
    return cm

def main(in_csv, out_json):
    groups = defaultdict(lambda: {
        "legs": [],
        "tzinfo": None,
        "qty_buy": 0,
        "qty_sell": 0,
        "buy_value": D(0),
        "sell_value": D(0),
        "cashflow": D(0),
        "min_dt": None,
        "max_dt": None,
        "any_expiry": None,
        "any_symbol": None,
        "any_option_type": None,
        "any_account": None,
        "any_multiplier": None,
        "is_option": None,
    })

    with open(in_csv, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("parse_ok", "").lower() not in ("true", "1", "t", "yes", "y"):
                continue

            account = row["account"]
            symbol = row["symbol"]
            is_option = row.get("is_option", "").lower() in ("true", "1", "t", "yes", "y")
            expiry_date = row.get("expiry_date") or None
            strike = row.get("strike") or None
            option_type = row.get("option_type") or None
            fut_root_symbol = row.get("fut_root_symbol") or None

            try:
                cm = float(row["contract_multiplier"]) if row.get("contract_multiplier") else 1.0
            except:
                cm = 1.0

            cm = normalize_multiplier(symbol, fut_root_symbol, cm)

            qty_signed = int(row["qty_signed"]) if row.get("qty_signed") else 0
            qty_abs = int(row["qty_abs"]) if row.get("qty_abs") else abs(qty_signed)
            price = D(row["price"]) if row.get("price") else D(0)

            dt = parse_dt(row["date_iso"])

            key = (
                account,
                symbol,
                str(cm),
                str(is_option),
                expiry_date or "",
                strike or "",
                option_type or "",
            )

            g = groups[key]
            if g["tzinfo"] is None:
                g["tzinfo"] = dt.tzinfo
            g["any_expiry"] = expiry_date
            g["any_symbol"] = symbol
            g["any_option_type"] = option_type
            g["any_account"] = account
            g["any_multiplier"] = cm
            g["is_option"] = is_option

            leg = {
                "message_id": row["message_id"],
                "trade_id": row["trade_id"],
                "side": row["side"],
                "qty": qty_abs,
                "price": float(price),
                "cashflow_per_unit": float(price * D(cm) * (1 if row["side"] == "SELL" else -1)),
                "contract_multiplier": cm,
                "underlying_mark": float(D(row["underlying_mark"])) if row.get("underlying_mark") else None,
                "impl_vol": float(D(row["impl_vol"])) if row.get("impl_vol") else None,
                "dt": row["date_iso"],
                "subject": row.get("subject"),
            }
            g["legs"].append(leg)

            if row["side"] == "BUY":
                g["qty_buy"] += qty_abs
                g["buy_value"] += price * D(qty_abs)
                g["cashflow"] -= price * D(qty_abs) * D(cm)
            elif row["side"] == "SELL":
                g["qty_sell"] += qty_abs
                g["sell_value"] += price * D(qty_abs)
                g["cashflow"] += price * D(qty_abs) * D(cm)

            g["min_dt"] = dt if g["min_dt"] is None or dt < g["min_dt"] else g["min_dt"]
            g["max_dt"] = dt if g["max_dt"] is None or dt > g["max_dt"] else g["max_dt"]

    now_utc = datetime.now(timezone.utc)
    out = []
    rid = 1

    for key, g in groups.items():
        account, symbol, cm_s, is_opt_s, expiry_date, strike, option_type = key
        cm = g["any_multiplier"] or 1.0
        net_qty = g["qty_buy"] - g["qty_sell"]
        synthetic_expiration = False

        if expiry_date and net_qty != 0:
            try:
                expiry_d = datetime.fromisoformat(expiry_date).date()
            except:
                expiry_d = None
            if expiry_d and datetime.combine(expiry_d, time(23, 59, 59), tzinfo=g["tzinfo"]) < now_utc.astimezone(g["tzinfo"]):
                synthetic_expiration = True
                close_side = "SELL" if net_qty > 0 else "BUY"
                synthetic_qty = abs(net_qty)
                synthetic_dt = datetime.combine(expiry_d, time(23, 59, 59), tzinfo=g["tzinfo"])
                synthetic_leg = {
                    "message_id": None,
                    "trade_id": "SYN_EXP",
                    "side": close_side,
                    "qty": synthetic_qty,
                    "price": 0.0,
                    "cashflow_per_unit": 0.0,
                    "contract_multiplier": cm,
                    "underlying_mark": None,
                    "impl_vol": None,
                    "dt": synthetic_dt.isoformat(),
                    "subject": "Synthetic expiration at $0.00"
                }
                g["legs"].append(synthetic_leg)
                net_qty = 0
                if g["max_dt"] is None or synthetic_dt > g["max_dt"]:
                    g["max_dt"] = synthetic_dt

        buy_vwap = vwap(g["qty_buy"], g["buy_value"])
        sell_vwap = vwap(g["qty_sell"], g["sell_value"])
        open_dt = g["min_dt"].isoformat() if g["min_dt"] else None
        close_dt = g["max_dt"].isoformat() if g["max_dt"] else None
        realized_pnl = g["cashflow"].quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        obj = {
            "round_trip_id": rid,
            "account": account,
            "symbol": symbol,
            "contract_multiplier": float(cm),
            "is_option": (is_opt_s.lower() == "true"),
            "expiry_date": expiry_date if expiry_date else None,
            "strike": float(strike) if strike not in (None, "",) else None,
            "option_type": option_type if option_type else None,
            "qty_buy": g["qty_buy"],
            "qty_sell": g["qty_sell"],
            "buy_vwap": float(buy_vwap) if buy_vwap is not None else None,
            "sell_vwap": float(sell_vwap) if sell_vwap is not None else None,
            "gross_buy_value": float(g["buy_value"]) if g["qty_buy"] else 0.0,
            "gross_sell_value": float(g["sell_value"]) if g["qty_sell"] else 0.0,
            "realized_pnl_cash": float(realized_pnl),
            "open_dt": open_dt,
            "close_dt": close_dt,
            "synthetic_expiration": synthetic_expiration,
            "legs": g["legs"],
        }

        out.append(obj)
        rid += 1

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {len(out)} aggregated round trips to {out_json}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python build_round_trips.py input.csv output.json")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
