# DB scaffolding overview (for future prompts)

This is a concise guide to the async SQLAlchemy data layer that mirrors `SCHEMA.sql` and runs against Supabase Postgres. It enables async operations, idempotent writes, and transactional unit-of-work.

## Structure

Directory: `gmaildownloader/db/`

- `__init__.py` — exports `engine`, `SessionLocal`, `models`, `uow`
- `config.py` — loads `.env`, normalizes DB URL to async driver
- `engine.py` — async engine + session factory + optional statement timeout
- `models.py` — SQLAlchemy ORM models for all tables (enums mapped, constraints)
- `schemas.py` — Pydantic DTOs for I/O payloads
- `uow.py` — Unit-of-Work: transactional context manager
- `repositories/` — per-entity repositories encapsulating DB ops
  - `accounts.py`, `instruments.py`, `contracts.py`, `trades.py`, `round_trips.py`, `round_trip_legs.py`
  - `__init__.py` — exports repos
- `migrations/README.md` — Alembic usage notes

## Files and roles

- `db/config.py`
  - Loads `.env` and picks DB URL from `SUPABASE_DB_URL` or fallback envs.
  - Normalizes scheme to `postgresql+asyncpg://` for SQLAlchemy async.
  - Exposes pool sizes and `DB_STATEMENT_TIMEOUT_MS`.

- `db/engine.py`
  - Creates async engine via `create_async_engine(DB_URL, ...)`.
  - `SessionLocal = async_sessionmaker(..., expire_on_commit=False)`.
  - `set_statement_timeout(session)` to apply per-transaction timeout.

- `db/models.py`
  - `Base = declarative_base()`.
  - Uses existing Postgres enums with `create_type=False` (types must exist from `SCHEMA.sql`): `trade_side`, `asset_class`, `opt_type`, `leg_role`.
  - Tables and constraints:
    - `Account` (unique `broker_code`, `created_at now()`)
    - `Instrument` (unique `(symbol, asset_class)`)
    - `Contract` (unique identity `ux_contract_identity` on `(instrument_id,is_option,option_type,expiry_date,strike,root,multiplier)`)
    - `Trade` (optional unique `broker_trade_id`, FKs to account/contract)
    - `RoundTrip` (metrics/dates, index on `(account_id,contract_id)`)
    - `RoundTripLeg` (unique `(round_trip_id,trade_id,allocated_qty)`, FKs to round_trips/trades)
  - Relationships: `Account.trades`, `Instrument.contracts`, `Contract.trades`, `RoundTrip.legs`, `Trade.rt_legs`.

- `db/schemas.py`
  - Pydantic DTOs: `AccountIn/Out`, `InstrumentIn/Out`, `ContractIn/Out`, `TradeIn/Out`, `RoundTripIn/Out`, `RoundTripLegIn/Out`.
  - Types: `datetime`, `date`, `Decimal`, `Optional[...]`.

- `db/uow.py`
  - `async with uow() as session:` opens a transaction (`session.begin()`), sets statement timeout, and ensures commit/rollback.

- `db/repositories/*`
  - Encapsulate reads/writes using SQLAlchemy Core/ORM.
  - Patterns: PostgreSQL upserts (`insert().on_conflict_do_*`), `find_or_create`, and idempotent inserts.
  - Key methods:
    - `AccountRepo.upsert(broker_code, display_name=None) -> Account`
    - `InstrumentRepo.find_or_create(symbol, asset_class) -> Instrument`
    - `ContractRepo.find_or_create(**identity) -> Contract` (identity: `instrument_id,is_option,option_type,expiry_date,strike,root,multiplier`)
    - `TradeRepo.insert_idempotent(payload) -> Trade` (upsert by `broker_trade_id`, updates `price`, `qty`, `dt` on conflict)
    - `RoundTripRepo.create(payload) -> RoundTrip`, `get(round_trip_id)`
    - `RoundTripLegRepo.add(payload) -> RoundTripLeg`

- `migrations/README.md`
  - Quick Alembic setup to manage schema changes going forward.

## Key design choices

- Async with `sqlalchemy[asyncio]` + `asyncpg` for performance.
- Repositories hide SQL/ORM details; services call repos.
- Unit of Work centralizes transaction scope and session lifecycle.
- Postgres-native upserts for idempotent ingestion and find-or-create.
- Enums map to existing DB types (`create_type=False`) aligned to `SCHEMA.sql`.
- URL normalization handles `postgres://` → `postgresql+asyncpg://`.

## How to use

- Ensure `.env` has one of (normalized automatically):
  - `SUPABASE_DB_URL=postgresql+asyncpg://user:pass@host:port/dbname`
  - or `DATABASE_URL` / `SUPABASE_DATABASE_URL`

- Example flow:

```python
from datetime import datetime, date
from decimal import Decimal
from gmaildownloader.db.uow import uow
from gmaildownloader.db.repositories import InstrumentRepo, ContractRepo, TradeRepo

async with uow() as session:
    instrument = await InstrumentRepo(session).find_or_create("SPX","INDEX")
    contract = await ContractRepo(session).find_or_create(
        instrument_id=instrument.instrument_id,
        is_option=True, option_type="CALL",
        expiry_date=date(2025,1,17), strike=Decimal("5000"),
        root=None, multiplier=Decimal("100"), exchange_code=None
    )
    trade = await TradeRepo(session).insert_idempotent({
        "broker_trade_id": "MID123",
        "account_id": "...uuid...",
        "contract_id": contract.contract_id,
        "side": "BUY",
        "qty": 1,
        "price": Decimal("10.25"),
        "dt": datetime.utcnow(),
        "is_synthetic": False,
        "message_id": "m-1",
        "subject": "ingest",
    })
```

## Common tasks

- Upsert/rename account: `AccountRepo.upsert(broker_code, display_name)`
- Ensure instrument exists: `InstrumentRepo.find_or_create(symbol, asset_class)`
- Ensure contract exists: `ContractRepo.find_or_create(**identity)`
- Idempotent trade ingestion: `TradeRepo.insert_idempotent(payload)`
- Create round trip and legs: `RoundTripRepo.create({...})`, `RoundTripLegRepo.add({...})`

## Notes for future extensions

- Use Alembic for incremental schema changes; keep `SCHEMA.sql` as doc.
- If adding enums/columns: update `SCHEMA.sql`, generate migration, update `models.py` & `schemas.py`.
- Consider DTO enum typing via `Literal[...]`.
- Add bulk ops and view helpers (e.g., `v_round_trip_pnl`, `v_round_trip_vwaps`).
- Add retry/backoff for transient DB errors.
