-- Enable UUID generation (Supabase has pgcrypto available)
create extension if not exists pgcrypto;

-- =========================
-- Enums
-- =========================
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'trade_side') THEN
    CREATE TYPE trade_side AS ENUM ('BUY','SELL');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'asset_class') THEN
    CREATE TYPE asset_class AS ENUM ('EQUITY','ETF','INDEX','FUTURE','OPTION','FX','CRYPTO');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'opt_type') THEN
    CREATE TYPE opt_type AS ENUM ('CALL','PUT');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'leg_role') THEN
    CREATE TYPE leg_role AS ENUM ('OPEN','CLOSE','ADJUST');
  END IF;
END$$;

-- =========================
-- Core reference tables
-- =========================

-- Accounts (broker sub-accounts)
CREATE TABLE IF NOT EXISTS accounts (
  account_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  broker_code text NOT NULL,           -- e.g. "*****0960TDA"
  display_name text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Underlyings (SPX, SPY, NVDA, /ES, etc.)
CREATE TABLE IF NOT EXISTS instruments (
  instrument_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol text NOT NULL,                -- e.g. "SPX", "NVDA", "/ES"
  asset_class asset_class NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (symbol, asset_class)
);

-- Tradable contracts (specific option/future/etc.)
CREATE TABLE IF NOT EXISTS contracts (
  contract_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  instrument_id uuid NOT NULL REFERENCES instruments(instrument_id) ON DELETE RESTRICT,
  is_option boolean NOT NULL,
  option_type opt_type NULL,           -- null for non-options
  expiry_date date NULL,
  strike numeric(20,6) NULL,
  root text,                           -- broker/root code if needed
  multiplier numeric(20,6) NOT NULL,   -- 100, 50, 20, etc.
  exchange_code text,
  created_at timestamptz NOT NULL DEFAULT now(),

  -- Basic sanity checks
  CHECK ((is_option = true AND option_type IS NOT NULL) OR (is_option = false AND option_type IS NULL)),
  CHECK ((is_option = true AND expiry_date IS NOT NULL) OR is_option = false),
  CHECK ((is_option = true AND strike IS NOT NULL) OR is_option = false)
);

-- Helpful uniqueness to avoid accidental dup contracts (NULLs are distinct in PG)
CREATE UNIQUE INDEX IF NOT EXISTS ux_contract_identity
ON contracts(instrument_id, is_option, option_type, expiry_date, strike, root, multiplier);

-- =========================
-- Trades / fills (immutable source-of-truth)
-- =========================
CREATE TABLE IF NOT EXISTS trades (
  trade_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),  -- internal id
  broker_trade_id text UNIQUE,                          -- optional external id (e.g. "65812608990" or "SYN_EXP")
  trade_hash text,                                      -- surrogate idempotency key when broker_trade_id is absent
  account_id uuid NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  contract_id uuid NOT NULL REFERENCES contracts(contract_id) ON DELETE RESTRICT,
  side trade_side NOT NULL,
  qty integer NOT NULL CHECK (qty > 0),
  price numeric(20,6) NOT NULL,                         -- per-unit (per contract) price/premium
  cashflow_per_unit numeric(20,6),                      -- as provided by broker (often price * +/-1 * multiplier)
  dt timestamptz NOT NULL,
  is_synthetic boolean NOT NULL DEFAULT false,          -- true for synthetic expirations / auto-assignments
  message_id text,
  subject text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS ix_trades_account_contract_dt ON trades(account_id, contract_id, dt);
CREATE INDEX IF NOT EXISTS ix_trades_contract_dt ON trades(contract_id, dt);
CREATE INDEX IF NOT EXISTS ix_trades_is_synthetic ON trades(is_synthetic);
-- Unique surrogate key for trade_hash when present
CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_trade_hash ON trades(trade_hash) WHERE trade_hash IS NOT NULL;

-- =========================
-- Round trips (derived grouping of trades)
-- =========================
CREATE TABLE IF NOT EXISTS round_trips (
  round_trip_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id uuid NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  contract_id uuid NOT NULL REFERENCES contracts(contract_id) ON DELETE RESTRICT,

  open_dt timestamptz NOT NULL,
  close_dt timestamptz NOT NULL,
  synthetic_expiration boolean NOT NULL DEFAULT false,

  -- Cached metrics (recomputable from trades)
  realized_pnl_cash numeric(20,6),
  qty_buy integer,
  qty_sell integer,
  buy_vwap numeric(20,6),
  sell_vwap numeric(20,6),

  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (close_dt >= open_dt)
);

CREATE INDEX IF NOT EXISTS ix_round_trips_account_contract ON round_trips(account_id, contract_id);
CREATE INDEX IF NOT EXISTS ix_round_trips_open_close ON round_trips(open_dt, close_dt);

-- Map many trades to a single round trip (with optional split allocations)
CREATE TABLE IF NOT EXISTS round_trip_legs (
  rt_leg_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  round_trip_id uuid NOT NULL REFERENCES round_trips(round_trip_id) ON DELETE CASCADE,
  trade_id uuid NOT NULL REFERENCES trades(trade_id) ON DELETE RESTRICT,
  allocated_qty integer NOT NULL CHECK (allocated_qty > 0),
  role_hint leg_role,                                     -- optional hint

  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_round_trip_legs_rt ON round_trip_legs(round_trip_id);
CREATE INDEX IF NOT EXISTS ix_round_trip_legs_trade ON round_trip_legs(trade_id);

-- Prevent exact duplicate mapping rows
CREATE UNIQUE INDEX IF NOT EXISTS ux_rtleg_unique ON round_trip_legs(round_trip_id, trade_id, allocated_qty);

-- =========================
-- (Optional) simple views
-- =========================

-- Cash PnL per round trip (recomputable)
CREATE OR REPLACE VIEW v_round_trip_pnl AS
SELECT
  rt.round_trip_id,
  SUM(CASE WHEN t.side = 'SELL' THEN t.price * t.qty * c.multiplier
           ELSE -t.price * t.qty * c.multiplier END) AS realized_pnl_cash
FROM round_trips rt
JOIN round_trip_legs rtl ON rtl.round_trip_id = rt.round_trip_id
JOIN trades t ON t.trade_id = rtl.trade_id
JOIN contracts c ON c.contract_id = t.contract_id
GROUP BY rt.round_trip_id;

-- VWAPs per round trip (SELL excludes synthetic fills)
CREATE OR REPLACE VIEW v_round_trip_vwaps AS
SELECT
  rt.round_trip_id,
  (SUM(CASE WHEN t.side='BUY' THEN t.price * rtl.allocated_qty ELSE 0 END)
    / NULLIF(SUM(CASE WHEN t.side='BUY' THEN rtl.allocated_qty END),0))::numeric(20,6) AS buy_vwap,
  (SUM(CASE WHEN t.side='SELL' AND NOT t.is_synthetic THEN t.price * rtl.allocated_qty ELSE 0 END)
    / NULLIF(SUM(CASE WHEN t.side='SELL' AND NOT t.is_synthetic THEN rtl.allocated_qty END),0))::numeric(20,6) AS sell_vwap
FROM round_trips rt
JOIN round_trip_legs rtl ON rtl.round_trip_id = rt.round_trip_id
JOIN trades t ON t.trade_id = rtl.trade_id
GROUP BY rt.round_trip_id;

-- Exclude synthetic trades: contract-level rollups with VWAPs (not tied to round trips)
CREATE OR REPLACE VIEW v_trade_rollups_ex_synth AS
SELECT
  t.account_id,
  t.contract_id,
  (SUM(CASE WHEN t.side='BUY' THEN t.price * t.qty ELSE 0 END)
    / NULLIF(SUM(CASE WHEN t.side='BUY' THEN t.qty END),0))::numeric(20,6) AS buy_vwap,
  (SUM(CASE WHEN t.side='SELL' AND NOT t.is_synthetic THEN t.price * t.qty ELSE 0 END)
    / NULLIF(SUM(CASE WHEN t.side='SELL' AND NOT t.is_synthetic THEN t.qty END),0))::numeric(20,6) AS sell_vwap_ex_synth,
  SUM(CASE WHEN t.side='BUY' THEN t.qty ELSE 0 END) AS buy_qty,
  SUM(CASE WHEN t.side='SELL' AND NOT t.is_synthetic THEN t.qty ELSE 0 END) AS sell_qty_ex_synth
FROM trades t
GROUP BY t.account_id, t.contract_id;

-- Cash PnL including synthetic effects at round trip level (explicit, mirrors inclusion)
CREATE OR REPLACE VIEW v_cash_pnl_incl_synth AS
SELECT
  rt.round_trip_id,
  SUM(CASE WHEN t.side = 'SELL' THEN t.price * rtl.allocated_qty * c.multiplier
           ELSE -t.price * rtl.allocated_qty * c.multiplier END)::numeric(20,6) AS realized_pnl_cash
FROM round_trips rt
JOIN round_trip_legs rtl ON rtl.round_trip_id = rt.round_trip_id
JOIN trades t ON t.trade_id = rtl.trade_id
JOIN contracts c ON c.contract_id = t.contract_id
GROUP BY rt.round_trip_id;

-- Materialized view for aggregated round trip metrics (refresh as needed)
CREATE MATERIALIZED VIEW IF NOT EXISTS v_round_trip_metrics AS
SELECT
  rt.round_trip_id,
  rt.account_id,
  rt.contract_id,
  rt.open_dt,
  rt.close_dt,
  rt.synthetic_expiration,
  vw.buy_vwap,
  vw.sell_vwap,
  pnl.realized_pnl_cash
FROM round_trips rt
LEFT JOIN v_round_trip_vwaps vw ON vw.round_trip_id = rt.round_trip_id
LEFT JOIN v_cash_pnl_incl_synth pnl ON pnl.round_trip_id = rt.round_trip_id;

CREATE INDEX IF NOT EXISTS ix_v_round_trip_metrics_acct_contract ON v_round_trip_metrics(account_id, contract_id);

-- Helpful covering index for contract lookups by expiry/strike
CREATE INDEX IF NOT EXISTS ix_contracts_instrument_expiry_strike ON contracts(instrument_id, expiry_date, strike);
