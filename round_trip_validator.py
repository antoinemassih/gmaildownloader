#!/usr/bin/env python3
"""
round_trip_validator.py

Validate the integrity and calculations of a round_trips.json file.

Usage:
    python round_trip_validator.py round_trips.json
"""

import json
import sys
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from dateutil import parser as dtparser

TOL = Decimal("0.0001")  # tolerance for value comparison

def D(x):
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))

def vwap(total_qty, total_value):
    if total_qty == 0:
        return None
    return (total_value / D(total_qty)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

def iso_date_check(s):
    try:
        dtparser.isoparse(s)
        return True
    except Exception:
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python round_trip_validator.py round_trips.json")
        sys.exit(1)

    in_file = sys.argv[1]

    with open(in_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    issues = []
    for rt in data:
        rt_id = rt.get("round_trip_id")
        cm = D(rt.get("contract_multiplier", 1))
        legs = rt.get("legs", [])

        # --- Check missing critical fields ---
        for field in ["account", "symbol", "qty_buy", "qty_sell", "gross_buy_value", "gross_sell_value", "realized_pnl_cash"]:
            if field not in rt or rt[field] is None:
                issues.append(f"[RT {rt_id}] Missing field: {field}")

        # --- Check date integrity ---
        open_dt = rt.get("open_dt")
        close_dt = rt.get("close_dt")
        if open_dt and not iso_date_check(open_dt):
            issues.append(f"[RT {rt_id}] Invalid open_dt format: {open_dt}")
        if close_dt and not iso_date_check(close_dt):
            issues.append(f"[RT {rt_id}] Invalid close_dt format: {close_dt}")
        if open_dt and close_dt:
            try:
                odt = dtparser.isoparse(open_dt)
                cdt = dtparser.isoparse(close_dt)
                if odt > cdt:
                    issues.append(f"[RT {rt_id}] open_dt after close_dt")
            except Exception:
                pass

        # --- Recalculate from legs ---
        buy_value = D(0)
        sell_value = D(0)
        buy_qty = 0
        sell_qty = 0

        for leg in legs:
            # Identify synthetic/placeholder legs that should NOT affect qty/VWAP/value
            is_synth = (
                leg.get("trade_id") == "SYN_EXP"
                or (leg.get("subject") or "").lower().startswith("synthetic expiration")
                or (leg.get("message_id") is None and D(leg.get("price")) == 0)
    )

    side = (leg.get("side") or "").upper()
    qty = int(D(leg.get("qty", 0)))
    price = D(leg.get("price", 0))

    # Use synthetic only for the “position zeroed” sanity, not for P&L/VWAP/qty aggregates
    if not is_synth:
        if side == "BUY":
            buy_value += price * qty
            buy_qty += qty
        elif side == "SELL":
            sell_value += price * qty
            sell_qty += qty

    # Track net to confirm synthetic expiration closes the position
    # (this uses ALL legs including synthetic)
    # Keep a separate counter if you want to enforce net flat, e.g.:
    # net_qty += qty if side == "BUY" else -qty


        # VWAP checks
        calc_buy_vwap = vwap(buy_qty, buy_value)
        calc_sell_vwap = vwap(sell_qty, sell_value)

        if rt["qty_buy"] != buy_qty:
            issues.append(f"[RT {rt_id}] qty_buy mismatch: {rt['qty_buy']} vs {buy_qty}")
        if rt["qty_sell"] != sell_qty:
            issues.append(f"[RT {rt_id}] qty_sell mismatch: {rt['qty_sell']} vs {sell_qty}")

        if D(rt.get("gross_buy_value", 0)).quantize(TOL) != buy_value.quantize(TOL):
            issues.append(f"[RT {rt_id}] gross_buy_value mismatch: {rt['gross_buy_value']} vs {buy_value}")
        if D(rt.get("gross_sell_value", 0)).quantize(TOL) != sell_value.quantize(TOL):
            issues.append(f"[RT {rt_id}] gross_sell_value mismatch: {rt['gross_sell_value']} vs {sell_value}")

        if rt.get("buy_vwap") is not None and calc_buy_vwap is not None:
            if abs(D(rt["buy_vwap"]) - calc_buy_vwap) > TOL:
                issues.append(f"[RT {rt_id}] buy_vwap mismatch: {rt['buy_vwap']} vs {calc_buy_vwap}")
        if rt.get("sell_vwap") is not None and calc_sell_vwap is not None:
            if abs(D(rt["sell_vwap"]) - calc_sell_vwap) > TOL:
                issues.append(f"[RT {rt_id}] sell_vwap mismatch: {rt['sell_vwap']} vs {calc_sell_vwap}")

        # PnL check
        calc_pnl = (sell_value - buy_value) * cm
        if abs(D(rt.get("realized_pnl_cash", 0)) - calc_pnl) > Decimal("0.01"):
            issues.append(f"[RT {rt_id}] realized_pnl_cash mismatch: {rt['realized_pnl_cash']} vs {calc_pnl}")

        # Synthetic expiration sanity
        if rt.get("synthetic_expiration"):
            net_qty_after = buy_qty - sell_qty
            if net_qty_after != 0:
                issues.append(f"[RT {rt_id}] synthetic_expiration true but net_qty_after != 0")

    # --- Summary ---
    if not issues:
        print(f"PASS: {len(data)} round trips validated with no issues.")
    else:
        print(f"FAIL: Found {len(issues)} issues across {len(data)} round trips.")
        for issue in issues:
            print(" -", issue)

if __name__ == "__main__":
    main()
