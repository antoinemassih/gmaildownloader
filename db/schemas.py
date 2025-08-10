from __future__ import annotations
from datetime import datetime, date
from decimal import Decimal
from pydantic import BaseModel
from typing import Optional

class AccountIn(BaseModel):
    broker_code: str
    display_name: Optional[str] = None

class AccountOut(AccountIn):
    account_id: str
    created_at: datetime

class InstrumentIn(BaseModel):
    symbol: str
    asset_class: str

class InstrumentOut(InstrumentIn):
    instrument_id: str
    created_at: datetime

class ContractIn(BaseModel):
    instrument_id: str
    is_option: bool
    option_type: Optional[str] = None
    expiry_date: Optional[date] = None
    strike: Optional[Decimal] = None
    root: Optional[str] = None
    multiplier: Decimal
    exchange_code: Optional[str] = None

class ContractOut(ContractIn):
    contract_id: str
    created_at: datetime

class TradeIn(BaseModel):
    broker_trade_id: Optional[str] = None
    trade_hash: Optional[str] = None
    account_id: str
    contract_id: str
    side: str
    qty: int
    price: Decimal
    cashflow_per_unit: Optional[Decimal] = None
    dt: datetime
    is_synthetic: bool = False
    message_id: Optional[str] = None
    subject: Optional[str] = None

class TradeOut(TradeIn):
    trade_id: str
    created_at: datetime

class RoundTripIn(BaseModel):
    account_id: str
    contract_id: str
    open_dt: datetime
    close_dt: datetime
    synthetic_expiration: bool = False

class RoundTripOut(RoundTripIn):
    round_trip_id: str
    realized_pnl_cash: Optional[Decimal] = None
    qty_buy: Optional[int] = None
    qty_sell: Optional[int] = None
    buy_vwap: Optional[Decimal] = None
    sell_vwap: Optional[Decimal] = None
    created_at: datetime

class RoundTripLegIn(BaseModel):
    round_trip_id: str
    trade_id: str
    allocated_qty: int
    role_hint: Optional[str] = None

class RoundTripLegOut(RoundTripLegIn):
    rt_leg_id: str
    created_at: datetime
