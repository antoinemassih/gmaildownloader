# Migrations (Alembic)

This project uses Alembic for schema migrations. The database is Supabase Postgres.

## Setup

```bash
pip install alembic
alembic init migrations
```

Edit `alembic.ini`:

```
sqlalchemy.url = postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DB
```

Or load from env in `env.py`:

```python
import os
from dotenv import load_dotenv
load_dotenv()
config.set_main_option("sqlalchemy.url", os.environ["SUPABASE_DB_URL"])
```

## Generate a migration

```bash
alembic revision -m "describe change"
```

## Apply migrations

```bash
alembic upgrade head
```

Note: Enums and base tables already exist from `SCHEMA.sql`. Prefer migrations for incremental changes going forward.
