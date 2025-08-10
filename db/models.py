from __future__ import annotations
import uuid
from datetime import datetime, date
from decimal import Decimal
from sqlalchemy.orm import Mapped, mapped_column, relationship, declarative_base
from sqlalchemy import ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID, ENUM, NUMERIC
from sqlalchemy import String, Integer, Date, DateTime, Boolean, Text, text

Base = declarative_base()

# Use existing Postgres types; do not re-create
trade_side = ENUM("BUY", "SELL", name="trade_side", create_type=False)
asset_class = ENUM("EQUITY","ETF","INDEX","FUTURE","OPTION","FX","CRYPTO", name="asset_class", create_type=False)
opt_type = ENUM("CALL","PUT", name="opt_type", create_type=False)
leg_role = ENUM("OPEN","CLOSE","ADJUST", name="leg_role", create_type=False)

class Account(Base):
    __tablename__ = "accounts"
    account_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    broker_code: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    display_name: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("timezone('UTC', now())"))

    trades: Mapped[list[Trade]] = relationship("Trade", back_populates="account")

class Instrument(Base):
    __tablename__ = "instruments"
    instrument_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    asset_class: Mapped[str] = mapped_column(asset_class, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("timezone('UTC', now())"))

    __table_args__ = (
        UniqueConstraint("symbol", "asset_class", name="ux_instrument_symbol_class"),
    )

    contracts: Mapped[list[Contract]] = relationship("Contract", back_populates="instrument")

class Contract(Base):
    __tablename__ = "contracts"
    contract_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instrument_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("instruments.instrument_id", ondelete="RESTRICT"), nullable=False)

    is_option: Mapped[bool] = mapped_column(Boolean, nullable=False)
    option_type: Mapped[str | None] = mapped_column(opt_type, nullable=True)
    expiry_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    strike: Mapped[Decimal | None] = mapped_column(NUMERIC(20, 6), nullable=True)

    root: Mapped[str | None] = mapped_column(String, nullable=True)
    multiplier: Mapped[Decimal] = mapped_column(NUMERIC(20, 6), nullable=False)
    exchange_code: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("timezone('UTC', now())"))

    instrument: Mapped[Instrument] = relationship("Instrument", back_populates="contracts")
    trades: Mapped[list[Trade]] = relationship("Trade", back_populates="contract")

    __table_args__ = (
        UniqueConstraint(
            "instrument_id","is_option","option_type","expiry_date","strike","root","multiplier",
            name="ux_contract_identity"
        ),
    )

class Trade(Base):
    __tablename__ = "trades"
    trade_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    broker_trade_id: Mapped[str | None] = mapped_column(String, unique=True, nullable=True)
    trade_hash: Mapped[str | None] = mapped_column(String, unique=True, nullable=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False)
    contract_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("contracts.contract_id", ondelete="RESTRICT"), nullable=False)

    side: Mapped[str] = mapped_column(trade_side, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(NUMERIC(20, 6), nullable=False)
    cashflow_per_unit: Mapped[Decimal | None] = mapped_column(NUMERIC(20, 6), nullable=True)

    dt: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_synthetic: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    message_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    subject: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default="now()")

    account: Mapped[Account] = relationship("Account", back_populates="trades")
    contract: Mapped[Contract] = relationship("Contract", back_populates="trades")
    rt_legs: Mapped[list[RoundTripLeg]] = relationship("RoundTripLeg", back_populates="trade")

class RoundTrip(Base):
    __tablename__ = "round_trips"
    round_trip_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.account_id", ondelete="RESTRICT"), nullable=False)
    contract_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("contracts.contract_id", ondelete="RESTRICT"), nullable=False)

    open_dt: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    close_dt: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    synthetic_expiration: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    realized_pnl_cash: Mapped[Decimal | None] = mapped_column(NUMERIC(20, 6), nullable=True)
    qty_buy: Mapped[int | None] = mapped_column(Integer, nullable=True)
    qty_sell: Mapped[int | None] = mapped_column(Integer, nullable=True)
    buy_vwap: Mapped[Decimal | None] = mapped_column(NUMERIC(20, 6), nullable=True)
    sell_vwap: Mapped[Decimal | None] = mapped_column(NUMERIC(20, 6), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=text("timezone('UTC', now())"))

    legs: Mapped[list[RoundTripLeg]] = relationship("RoundTripLeg", back_populates="round_trip")

    __table_args__ = (
        Index("ix_round_trips_account_contract", "account_id", "contract_id"),
    )

class RoundTripLeg(Base):
    __tablename__ = "round_trip_legs"
    rt_leg_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    round_trip_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("round_trips.round_trip_id", ondelete="CASCADE"), nullable=False)
    trade_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("trades.trade_id", ondelete="RESTRICT"), nullable=False)

    allocated_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    role_hint: Mapped[str | None] = mapped_column(leg_role, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default="now()")

    round_trip: Mapped[RoundTrip] = relationship("RoundTrip", back_populates="legs")
    trade: Mapped[Trade] = relationship("Trade", back_populates="rt_legs")

    __table_args__ = (
        UniqueConstraint("round_trip_id", "trade_id", "allocated_qty", name="ux_rt_legs_allocation"),
    )
