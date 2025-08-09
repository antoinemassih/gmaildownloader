# Gmail Thinkorswim Trade Exporter & Parser

Export trade alert emails from Gmail, parse Thinkorswim (TOS) **subject lines** into structured fields, fix messy CSVs, and isolate failures for review.

It handles:
- **Equity & ETF trades** (e.g., `SQQQ`, `AMD`, `SHOP`)
- **Futures** (e.g., `/ESZ22`, `/MNQH24`, `/MESU23`)
- **Equity options** (incl. Weeklys, contract multipliers, strikes, expiries)
- Graceful skipping of **non-trade alerts** (e.g., “price alert”)

---

## Features

- **gmail_subject_exporter.py** – pull Gmail messages into a CSV (IDs, dates, from/to, subject, etc.)
- **tos_trade_parser.py** – robust parser for TOS subjects (options, futures, equities)
- **csv_fixer.py** – re-parse an existing subjects CSV and write a clean, “fixed” CSV
- **extract_failures.py** – filter rows with `parse_ok=false` into a separate CSV for triage

---

## Quick start

```bash
git clone <your-repo-url>
cd <repo>

# (Recommended) create a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

# Install deps
pip install -r requirements.txt
```

> Don’t have a `requirements.txt` yet? Typical packages:
> `pandas`, `python-dateutil`, and for Gmail exporting:  
> `google-api-python-client`, `google-auth-httplib2`, `google-auth-oauthlib`.

---

## Usage

### 1) Export (optional)
If you want to (re)download subjects from Gmail:

```bash
python gmail_subject_exporter.py --output subjects_raw.csv
# (Use your script’s flags like --limit, --query, --since, etc., as needed)
```

> First run will prompt Google OAuth and create `token.json`.  
> Put your Google `credentials.json` in the repo root (or wherever your script expects it).

### 2) Fix an existing CSV (re-parse subjects you already have)
If you **already have** a CSV with a `subject` column, run:

```bash
python csv_fixer.py subjects_raw.csv subjects_fixed.csv
```

- Reads the input CSV
- Parses each `subject` via `tos_trade_parser.parse_trade_subject`
- Writes `subjects_fixed.csv` with structured columns + `parse_ok` and `fail_reason`

### 3) Extract failures for review
```bash
python extract_failures.py subjects_fixed.csv subjects_failed.csv
```

This writes only the rows where `parse_ok=false` (often non-trade alerts or truly odd formats).

---

## Input CSV expectations

Your input CSV **must** include a `subject` column.  
Typical columns from the exporter look like:

```
message_id,thread_id,date_raw,date_iso,epoch,from_email,to_email,subject
```

If your file is missing the header row, add one that matches your data. Example:

```csv
message_id,thread_id,date_raw,date_iso,epoch,from_email,to_email,subject
190086d8e8296f7a,190086d8e8296f7a,"Tue, 11 Jun 2024 13:51:55 -0400 (EDT)",2024-06-11T13:51:55-04:00,1718128315,alerts@thinkorswim.com,me@example.com,"#82180613275 BOT +1 SPX 100 (Weeklys) 11 JUN 24 5315 PUT @.15MARK=5354.70 IMPL VOL=13.15% , ACCOUNT *****750SCHW"
```

---

## Output schema (key columns)

`csv_fixer.py` appends structured fields for each parsed subject:

- `parse_ok` (bool) – `true` if the parser recognized the trade
- `fail_reason` (str) – reason when `parse_ok=false` (e.g., `non_trade_alert`, `not_a_trade`, `unrecognized_trade_format`)
- `is_option` (bool) – whether the row is an option trade
- `trade_id` (int)
- `side` (`BOT` / `SOLD`)
- `qty_signed` (int) and `qty_abs` (int)
- `symbol` (str)
- `contract_multiplier` (str or empty) – set for options (e.g., `100`, `1/50`)
- `expiry_date` (YYYY-MM-DD, options only)
- `strike` (float, options only)
- `option_type` (`PUT`/`CALL`, options only)
- `price` (float after `@`)
- `underlying_mark` (float after `MARK=`, tolerates exchange tags like `CBOE`, `NYSE`, `BATS` before `MARK=`)
- `impl_vol` (float)
- `account` (str)

> For **non-options** (futures/equities), `contract_multiplier`, `expiry_date`, `strike`, and `option_type` are left blank by design.

---

## What the parser understands

Examples it parses successfully:

- **Options**  
  `#66066620674 SOLD -12 SPY 100 17 MAY 24 500 PUT @.12MARK=520.79 IMPL VOL=13.29% , ACCOUNT *****0960TDA`  
  `#82180613275 BOT +1 SPX 100 (Weeklys) 11 JUN 24 5315 PUT @.15MARK=5354.70 IMPL VOL=13.15% , ACCOUNT *****750SCHW`

- **Futures**  
  `#9917289343 BOT +1 /ESM23 @4160.25MARK=4160.00 IMPL VOL=19.29% , ACCOUNT 49*****60`

- **Equities/ETFs**  
  `#9977956423 BOT +1,000 SQQQ @19.2499MARK=19.2499 IMPL VOL=60.67% , ACCOUNT 49*****60`

- **Non-trade alerts** are flagged cleanly (e.g., `.AMZN201204C3340 price alert` → `parse_ok=false`, `fail_reason=non_trade_alert`).

---

## Repo layout

```
/repo
├─ gmail_subject_exporter.py   # export Gmail messages (optional step)
├─ tos_trade_parser.py         # core subject parser (options, futures, equities)
├─ csv_fixer.py                # re-parse an existing CSV -> subjects_fixed.csv
├─ extract_failures.py         # filter parse_ok=false -> subjects_failed.csv
├─ requirements.txt            # (recommended) Python deps
└─ README.md                   # this file
```

---

## Development

- Python 3.9+ recommended
- Style: `black`/`isort` friendly
- Regexes live in `tos_trade_parser.py` and are easy to extend

---

## License

MIT (feel free to change if you prefer another license).

---

## Contributing

Issues and PRs welcome! If you hit a subject line that doesn’t parse, include:
- The full `subject` string
- Whether it’s an option, future, equity, or alert
- A couple neighbor lines for context if available
