from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text
from .config import DB_URL, POOL_SIZE, MAX_OVERFLOW, STATEMENT_TIMEOUT_MS
import os
import ssl
try:
    import certifi  # type: ignore
    _SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:  # pragma: no cover
    _SSL_CONTEXT = ssl.create_default_context()

# Optional insecure SSL (encryption without verification) for environments with intercepting proxies/self-signed chains
if os.getenv("SUPABASE_SSL_INSECURE") in {"1", "true", "TRUE", "yes", "on"}:
    _SSL_CONTEXT.check_hostname = False
    _SSL_CONTEXT.verify_mode = ssl.CERT_NONE

engine = create_async_engine(
    DB_URL,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_pre_ping=True,
    connect_args={"ssl": _SSL_CONTEXT, "timeout": 30},  # Verified SSL with certifi bundle
)

SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def set_statement_timeout(session: AsyncSession):
    # Apply a session-level statement timeout so it doesn't rely on an open transaction
    await session.execute(text(f"SET SESSION statement_timeout = {STATEMENT_TIMEOUT_MS}"))
