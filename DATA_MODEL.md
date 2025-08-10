# Object Model Overview

This document explains the domain model represented by the SQL schema in `SCHEMA.sql`. It’s intended as a reference for building CRUD endpoints and ORM models.

- Database: Postgres (Supabase)
- UUID primary keys via `gen_random_uuid()`
- Core enums: `trade_side('BUY','SELL')`, `asset_class(...)`, `opt_type('CALL','PUT')`, `leg_role('OPEN','CLOSE','ADJUST')`

---

## High-level Entities and Relationships

- **Account (`accounts`)** 1 — n **Trade (`trades`)**
- **Instrument (`instruments`)** 1 — n **Contract (`contracts`)**
- **Contract (`contracts`)** 1 — n **Trade (`trades`)**
- **RoundTrip (`round_trips`)** aggregates many **Trades (`trades`)** via **RoundTripLeg (`round_trip_legs`)** (n — n with allocation)

Text diagram:
```
accounts (1) ──< trades >── (1) contracts >── (1) instruments
                          \
                           \──< round_trip_legs >── round_trips
```

---

## accounts
Represents a broker sub-account or funding account where trades are executed.

- PK: `account_id (uuid)`
- Fields: `broker_code(text)`, `display_name(text)`, `created_at(timestamptz)`
- Typical examples: `"*****0960TDA"`, `"*****750SCHW"`

CRUD notes:
- Create on first encounter of a new broker code.
- Usually immutable except `display_name`.

ORM sketch (Python/pydantic-ish):
```python
class Account(Base):
    account_id: UUID
    broker_code: str
    display_name: Optional[str]
    created_at: datetime
```

---

## instruments
Underlying symbols such as tickers or futures roots.

- PK: `instrument_id (uuid)`
- Unique: `(symbol, asset_class)`
- Fields: `symbol(text)`, `asset_class(asset_class)`, `created_at`
- Examples: `("SPX","INDEX")`, `("NVDA","EQUITY")`, `("/ES","FUTURE")`

CRUD notes:
- Insert if not found when ingesting trades/contracts.

ORM sketch:
```python
class Instrument(Base):
    instrument_id: UUID
    symbol: str
    asset_class: Literal['EQUITY','ETF','INDEX','FUTURE','OPTION','FX','CRYPTO']
    created_at: datetime
```

---

## contracts
A specific tradable contract. For options, includes type/expiry/strike; for non-options, those fields are NULL. Uniqueness helps prevent duplicates.

- PK: `contract_id (uuid)`
- FK: `instrument_id -> instruments.instrument_id`
- Unique index: `(instrument_id, is_option, option_type, expiry_date, strike, root, multiplier)`
- Fields:
  - Common: `is_option(bool)`, `multiplier(numeric)`, `root(text)`, `exchange_code(text)`, `created_at`
  - Options: `option_type(opt_type)`, `expiry_date(date)`, `strike(numeric)`
- Constraints:
  - If `is_option=true` then `option_type`, `expiry_date`, `strike` must be present

CRUD notes:
- On ingest: find-or-create contract by the unique identity.

ORM sketch:
```python
class Contract(Base):
    contract_id: UUID
    instrument_id: UUID
    is_option: bool
    option_type: Optional[Literal['CALL','PUT']]
    expiry_date: Optional[date]
    strike: Optional[Decimal]
    root: Optional[str]
    multiplier: Decimal
    exchange_code: Optional[str]
    created_at: datetime
```

---

## trades
Immutable fills or broker-reported executions. Source-of-truth for cashflows and quantities.

- PK: `trade_id (uuid)`
- Unique optional: `broker_trade_id(text)` (maps to external/broker id or synthetic marker like `SYN_EXP`)
- FKs: `account_id -> accounts`, `contract_id -> contracts`
- Fields: `side(trade_side)`, `qty(int>0)`, `price(numeric)`, `cashflow_per_unit(numeric)`, `dt(timestamptz)`, `is_synthetic(bool)`, `message_id(text)`, `subject(text)`, `created_at`

Semantics:
- `side`: BUY increases position units, SELL decreases.
- `price`: per-contract premium/price.
- `cashflow_per_unit`: precomputed from broker if available; optional.
- `is_synthetic`: true for synthetic expirations or broker adjustments.

CRUD notes:
- Inserts only (immutable). If deduplicating, upsert by `broker_trade_id` if provided.

ORM sketch:
```python
class Trade(Base):
    trade_id: UUID
    broker_trade_id: Optional[str]
    account_id: UUID
    contract_id: UUID
    side: Literal['BUY','SELL']
    qty: int
    price: Decimal
    cashflow_per_unit: Optional[Decimal]
    dt: datetime
    is_synthetic: bool
    message_id: Optional[str]
    subject: Optional[str]
    created_at: datetime
```

---

## round_trips
A derived grouping of trades for a specific account+contract that opens and eventually closes a position. Caches useful metrics that are recomputable from legs/trades.

- PK: `round_trip_id (uuid)`
- FKs: `account_id -> accounts`, `contract_id -> contracts`
- Fields:
  - Lifecycle: `open_dt`, `close_dt`, `synthetic_expiration(bool)`
  - Metrics: `realized_pnl_cash(numeric)`, `qty_buy(int)`, `qty_sell(int)`, `buy_vwap(numeric)`, `sell_vwap(numeric)`
  - `created_at`
- Constraint: `close_dt >= open_dt`

Semantics:
- A round trip contains at least one BUY or SELL and returns to (near) flat by `close_dt`. Synthetic expirations may be used to force flatness.

CRUD notes:
- Typically write-only from an aggregator job; update if re-aggregation occurs.

ORM sketch:
```python
class RoundTrip(Base):
    round_trip_id: UUID
    account_id: UUID
    contract_id: UUID
    open_dt: datetime
    close_dt: datetime
    synthetic_expiration: bool
    realized_pnl_cash: Optional[Decimal]
    qty_buy: Optional[int]
    qty_sell: Optional[int]
    buy_vwap: Optional[Decimal]
    sell_vwap: Optional[Decimal]
    created_at: datetime
```

---

## round_trip_legs
Mapping table linking trades into round trips, with optional partial allocations.

- PK: `rt_leg_id (uuid)`
- FKs: `round_trip_id -> round_trips`, `trade_id -> trades`
- Unique: `(round_trip_id, trade_id, allocated_qty)` prevents exact duplicates
- Fields: `allocated_qty(int>0)`, `role_hint(leg_role)`, `created_at`

Semantics:
- One trade can be split across multiple round trips via multiple legs summing up to `trade.qty` if you support partial allocations.
- `role_hint` is optional (OPEN/CLOSE/ADJUST) and can be used by UI logic.

CRUD notes:
- Insert during aggregation. Update if rebalancing allocation.

ORM sketch:
```python
class RoundTripLeg(Base):
    rt_leg_id: UUID
    round_trip_id: UUID
    trade_id: UUID
    allocated_qty: int
    role_hint: Optional[Literal['OPEN','CLOSE','ADJUST']]
    created_at: datetime
```

---

## Views
- `v_round_trip_pnl`: recomputes realized cash PnL per round trip using trades and contract multipliers.
- `v_round_trip_vwaps`: computed buy/sell VWAPs, excluding synthetic sells.

---

## Common CRUD Patterns

- **Create Account/Instrument/Contract**: upsert by natural keys (`broker_code`, `(symbol,asset_class)`, contract unique identity).
- **Ingest Trade**: insert-only; dedupe by `broker_trade_id` when available.
- **Aggregate to RoundTrip**: compute groups by `(account_id, contract_id)`, allocate trades to a `round_trip_id` via `round_trip_legs`, compute metrics.
- **Query PnL**: use `v_round_trip_pnl` or join `round_trips` → `round_trip_legs` → `trades` → `contracts`.

---

## Suggested API Shapes

- `GET /accounts`: list accounts
- `GET /instruments?symbol=SPX`
- `POST /contracts/find_or_create` with identity payload
- `POST /trades` bulk insert (idempotent on `broker_trade_id`)
- `POST /round_trips/recompute?from=...&to=...`
- `GET /round_trips?account_id=...&instrument_id=...`
- `GET /round_trips/{id}/legs`

---

## Mapping from CSV fields (when ingesting)

Typical CSV columns → entities:
- Account: `account`/`broker_code`
- Instrument: `symbol` (and infer `asset_class`)
- Contract: `is_option`, `option_type`, `expiry_date`, `strike`, `multiplier/root`
- Trade: `side`, `qty`, `price`, `dt`, `message_id`, `subject`, `is_synthetic` (derived)

---

## Migration/Versioning Notes

- Keep `SCHEMA.sql` as the source of truth.
- For DB changes, create a new migration file (e.g., `migrations/2025-08-10_add_xyz.sql`).
- Update this `DATA_MODEL.md` whenever you add/modify entities.
