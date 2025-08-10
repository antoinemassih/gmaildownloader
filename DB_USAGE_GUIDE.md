# Database Usage Guide (Async SQLAlchemy DAL)

This guide shows how to use the async SQLAlchemy DAL implemented under `gmaildownloader/db/` to build ingestion and business logic. It focuses on the Unit of Work pattern, repositories, idempotent trade upserts, round trip creation/metrics, and safe read/query patterns.

- Core modules:
  - `db/config.py` – loads DB URL and settings
  - `db/engine.py` – async engine and session factory; per-session statement timeout
  - `db/uow.py` – Unit of Work with optional read-only mode
  - `db/models.py` – ORM models mapped to Supabase Postgres schema
  - `db/schemas.py` – Pydantic DTOs for inputs/outputs
  - `db/repositories/` – entity repositories (accounts, instruments, contracts, trades, round_trips, round_trip_legs)

- Schema highlights (see `SCHEMA.sql`):
  - Precision: numeric(20,6) for monetary and VWAP/PNL fields
  - Idempotency: `trades.broker_trade_id` UNIQUE and optional `trades.trade_hash` (partial unique index)
  - Views: `v_round_trip_pnl`, `v_round_trip_vwaps`, `v_cash_pnl_incl_synth`, and materialized `v_round_trip_metrics`


## Quickstart

```python
import asyncio
from gmaildownloader.db.uow import uow
from gmaildownloader.db.repositories import (
    AccountRepo, InstrumentRepo, ContractRepo,
    TradeRepo, RoundTripRepo, RoundTripLegRepo,
)

async def main():
    async with uow() as session:
        # Repos
        accounts = AccountRepo(session)
        instruments = InstrumentRepo(session)
        contracts = ContractRepo(session)
        trades = TradeRepo(session)
        round_trips = RoundTripRepo(session)
        legs = RoundTripLegRepo(session)

        # Example: ensure account, instrument, contract exist
        acct = await accounts.upsert({"broker_code": "****0960TDA", "display_name": "Primary"})
        instr = await instruments.find_or_create({"symbol": "SPY", "asset_class": "ETF"})
        contract = await contracts.find_or_create({
            "instrument_id": str(instr.instrument_id),
            "is_option": False,
            "option_type": None,
            "expiry_date": None,
            "strike": None,
            "root": None,
            "multiplier": "100",
            "exchange_code": "ARCA",
        })

        # Idempotent trade upsert (broker id OR trade_hash required)
        trade, created = await trades.upsert({
            "broker_trade_id": "65812608990",  # or use trade_hash if broker id missing
            "account_id": str(acct.account_id),
            "contract_id": str(contract.contract_id),
            "side": "BUY",
            "qty": 5,
            "price": "1.250000",
            "cashflow_per_unit": None,
            "dt": "2025-08-10T10:05:00Z",
            "is_synthetic": False,
            "message_id": "<msg-123>",
            "subject": "Fill notice",
        })
        print("trade id:", trade.trade_id, "created:", created)

asyncio.run(main())
```


## Unit of Work (`db/uow.py`)

- `uow(read_only: bool = False)` yields a transactional async session.
- Statement timeout is applied per-session.
- Set `read_only=True` for safe read-only blocks (guards accidental writes).

```python
from gmaildownloader.db.uow import uow

# Read-only query block
async with uow(read_only=True) as session:
    # perform SELECTs only
    ...
```


## Repositories Overview (`db/repositories/*`)

- Common pattern: initialize per session, then call APIs.
- Methods return ORM objects (`db/models.py`), suitable for later serialization via DTOs in `db/schemas.py` if needed.

```python
accounts = AccountRepo(session)
instruments = InstrumentRepo(session)
contracts = ContractRepo(session)
trades = TradeRepo(session)
round_trips = RoundTripRepo(session)
legs = RoundTripLegRepo(session)
```

### Repository Pattern (why this over plain functions?)

- **Transactional safety (UoW)**: Repos accept a session from `uow()` so multiple calls share the same transaction and timeout settings.
- **Cohesion per entity**: Each entity’s queries/upserts live together (e.g., `TradeRepo.upsert()`), easier to discover and maintain.
- **Encapsulation of DB specifics**: `ON CONFLICT`, indexes, and retry/merge rules are hidden behind a stable Python API.
- **Testability**: Swap in a test session; unit/integration tests don’t need to manage engines or duplicate SQL.
- **Easier schema evolution**: Changing constraints/indices usually requires edits only inside the repo, not all callers.

Where to look:
- `gmaildownloader/db/repositories/accounts.py`
- `gmaildownloader/db/repositories/instruments.py`
- `gmaildownloader/db/repositories/contracts.py`
- `gmaildownloader/db/repositories/trades.py`
- `gmaildownloader/db/repositories/round_trips.py`
- `gmaildownloader/db/repositories/round_trip_legs.py`

### Accounts (`accounts.py`)
- `upsert(payload: dict) -> Account`
  - Insert or update an account (by `broker_code`).
- `get_by_broker_code(broker_code: str) -> Account | None`

Example:
```python
acct = await accounts.upsert({"broker_code": "****0960TDA", "display_name": "Primary"})
```

### Instruments (`instruments.py`)
- `find_or_create(payload: dict) -> Instrument`
  - Uses unique `(symbol, asset_class)`.

Example:
```python
instr = await instruments.find_or_create({"symbol": "SPX", "asset_class": "INDEX"})
```

### Contracts (`contracts.py`)
- `find_or_create(payload: dict) -> Contract`
  - Uniqueness: `(instrument_id, is_option, option_type, expiry_date, strike, root, multiplier)`.

Example:
```python
contract = await contracts.find_or_create({
    "instrument_id": str(instr.instrument_id),
    "is_option": True,
    "option_type": "CALL",
    "expiry_date": "2025-12-19",
    "strike": "5000.000000",
    "root": "SPX",
    "multiplier": "100",
    "exchange_code": "CBOE",
})
```

### Trades (`trades.py`)
- `upsert(payload: dict) -> tuple[Trade, bool]`
  - Idempotent insert/update using `broker_trade_id` or `trade_hash`.
  - On conflict, updates: `price, qty, dt, cashflow_per_unit, is_synthetic, message_id, subject, account_id, contract_id`.
  - Returns `(Trade, created)`; `created` is conservative. If you need strict created/updated detection, ask to enable system-column-based detection.

Payload shape (subset):
```python
{
  "broker_trade_id": "65812608990",  # OR supply "trade_hash"
  "trade_hash": None,
  "account_id": "<uuid>",
  "contract_id": "<uuid>",
  "side": "BUY" | "SELL",
  "qty": 1,
  "price": "1.230000",
  "cashflow_per_unit": null,
  "dt": "2025-08-10T10:05:00Z",
  "is_synthetic": false,
  "message_id": "<id>",
  "subject": "<text>"
}
```

### Round Trips (`round_trips.py`) and Legs (`round_trip_legs.py`)
- Common flow:
  1) Create a `round_trip` for an `account_id` + `contract_id` and open/close window.
  2) Add one or more `round_trip_legs` mapping trades to the round trip with `allocated_qty`.
  3) Optionally compute/display metrics via the views/materialized view.

Pseudo-usage:
```python
rt = await round_trips.create({
  "account_id": str(acct.account_id),
  "contract_id": str(contract.contract_id),
  "open_dt": "2025-08-10T13:00:00Z",
  "close_dt": "2025-08-11T21:00:00Z",
  "synthetic_expiration": False,
})

await legs.add({
  "round_trip_id": str(rt.round_trip_id),
  "trade_id": str(trade.trade_id),
  "allocated_qty": 5,
  "role_hint": "OPEN",
})
```


## Read Patterns and Analytics

- Use read-only UoW for dashboards/queries:
```python
async with uow(read_only=True) as session:
    # Example: raw SQL against a view
    from sqlalchemy import text
    res = await session.execute(text("SELECT * FROM v_round_trip_metrics WHERE account_id = :aid"), {"aid": str(acct.account_id)})
    rows = res.mappings().all()
```

- Views:
  - `v_round_trip_pnl` – cash PnL per round trip
  - `v_round_trip_vwaps` – BUY VWAP, SELL VWAP (SELL excludes synthetic)
  - `v_cash_pnl_incl_synth` – explicit PnL including synthetic effects
  - `v_round_trip_metrics` (materialized) – aggregates key metrics; index provided for `(account_id, contract_id)`

- Refresh materialized view (run in SQL client or via engine):
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY v_round_trip_metrics;
```


## Idempotency Guidance

- Prefer `broker_trade_id` for uniqueness when available.
- When missing, compute a stable `trade_hash` across the fields that define a logical unique fill (e.g., account_id, contract_id, dt to the second, side, qty, price, broker/source tag). Use that in `TradeRepo.upsert()`.
- Re-running the same ingestion becomes safe and idempotent.


## DTOs (`db/schemas.py`)

- Use DTOs to validate API payloads before calling repos. Example:
```python
from gmaildownloader.db.schemas import TradeIn

payload = TradeIn(
  broker_trade_id = "65812608990",
  trade_hash = None,
  account_id = str(acct.account_id),
  contract_id = str(contract.contract_id),
  side = "BUY",
  qty = 5,
  price = "1.250000",
  cashflow_per_unit = None,
  dt = "2025-08-10T10:05:00Z",
  is_synthetic = False,
).model_dump()

trade, created = await trades.upsert(payload)
```


## Error Handling Patterns

- Wrap ingestion steps per batch in a single UoW if you want all-or-nothing semantics.
- On unique conflicts from concurrent writers, `upsert` handles it, but you may still see transient errors in other entities; add retry with backoff around the UoW block if needed.
- Use statement timeouts (already configured) to avoid long-running queries blocking the system.


## Testing Hooks

- For `pytest-asyncio`, wrap each test in `async with uow():` and use repos directly.
- Seed minimal fixtures via repos (accounts, instruments, contracts) and assert on views for aggregates.


## Common Recipes

- Ingest a CSV of fills idempotently
```python
async with uow() as session:
    trades = TradeRepo(session)
    for row in csv_rows:
        payload = {
            "broker_trade_id": row.get("broker_id") or None,
            "trade_hash": row.get("hash") or None,
            "account_id": row["account_id"],
            "contract_id": row["contract_id"],
            "side": row["side"].upper(),
            "qty": int(row["qty"]),
            "price": str(row["price"]),
            "dt": row["dt_iso"],
            "is_synthetic": bool(row.get("is_synth", False)),
        }
        await trades.upsert(payload)
```

- Build a round trip from existing trades
```python
async with uow() as session:
    rtr = RoundTripRepo(session)
    legs = RoundTripLegRepo(session)
    rt = await rtr.create({
        "account_id": account_id,
        "contract_id": contract_id,
        "open_dt": open_dt,
        "close_dt": close_dt,
        "synthetic_expiration": False,
    })
    for t in trade_list:
        await legs.add({
            "round_trip_id": str(rt.round_trip_id),
            "trade_id": str(t.trade_id),
            "allocated_qty": t.qty,
            "role_hint": None,
        })
```


## Where to look in code

- `gmaildownloader/db/models.py` – ORM types, relationships, constraints
- `gmaildownloader/db/repositories/*.py` – CRUD/upsert patterns
- `gmaildownloader/db/uow.py` – transaction boundaries and read-only mode
- `gmaildownloader/SCHEMA.sql` – source of truth for schema, indexes, and views
- `gmaildownloader/migrations/README.md` – Alembic setup guidance (optional)


## Next steps (optional)

- Exact insert/update detection in `TradeRepo.upsert` via system columns or pre-check
- Bulk upserts for large imports
- Structured logging/tracing for observability
- Scheduled refresh of `v_round_trip_metrics`
