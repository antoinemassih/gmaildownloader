import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Robust .env discovery: try common locations relative to this file and CWD
def _load_env():
    here = Path(__file__).resolve()
    candidates = [
        Path.cwd() / ".env",                    # current working directory
        here.parent / ".env",                   # .../db/.env
        here.parents[1] / ".env",               # .../gmaildownloader/.env
        here.parents[2] / ".env",               # project root .env
    ]
    for p in candidates:
        if p.exists():
            load_dotenv(p, override=False)
            return
    # Fallback: walk upwards from CWD
    found = find_dotenv(usecwd=True)
    if found:
        load_dotenv(found, override=False)
    else:
        # Final fallback loads OS env only (no file)
        load_dotenv()

_load_env()

# Prefer explicit SUPABASE_DB_URL, else fallback to DATABASE_URL or SUPABASE_DATABASE_URL
RAW_DB_URL = (
    os.getenv("SUPABASE_DB_URL")
    or os.getenv("DATABASE_URL")
    or os.getenv("SUPABASE_DATABASE_URL")
)

# Derive from env if no explicit DB URL provided. Supports both direct and pooled endpoints.
if not RAW_DB_URL:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    supabase_db_password = os.getenv("SUPABASE_DB_PASSWORD", "").strip()
    # Optional explicit overrides (works for pooled endpoints too)
    db_host_override = os.getenv("SUPABASE_DB_HOST", "").strip()
    db_port_override = os.getenv("SUPABASE_DB_PORT", os.getenv("DB_PORT", "").strip()).strip()
    db_user = os.getenv("SUPABASE_DB_USER", "postgres").strip()
    db_name = os.getenv("SUPABASE_DB_NAME", "postgres").strip()

    if supabase_db_password and (db_host_override or supabase_url):
        try:
            if db_host_override:
                db_host = db_host_override
            else:
                # SUPABASE_URL is like https://<project-ref>.supabase.co
                # DB host is db.<project-ref>.supabase.co
                host_ref = supabase_url.split("https://", 1)[-1].split(".supabase.co", 1)[0]
                db_host = f"db.{host_ref}.supabase.co"
            port = db_port_override or "5432"
            RAW_DB_URL = f"postgresql://{db_user}:{supabase_db_password}@{db_host}:{port}/{db_name}"
        except Exception:
            pass
if not RAW_DB_URL:
    raise RuntimeError(
        "No database URL found. Set SUPABASE_DB_URL or DATABASE_URL (or provide SUPABASE_URL + SUPABASE_DB_PASSWORD) in .env"
    )

# Ensure async driver scheme for SQLAlchemy
# Convert postgresql://... to postgresql+asyncpg://...
if RAW_DB_URL.startswith("postgresql://") and "+asyncpg" not in RAW_DB_URL:
    DB_URL = RAW_DB_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif RAW_DB_URL.startswith("postgres://") and "+asyncpg" not in RAW_DB_URL:
    DB_URL = RAW_DB_URL.replace("postgres://", "postgresql+asyncpg://", 1)
else:
    DB_URL = RAW_DB_URL

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "5"))
STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "30000"))
