from contextlib import asynccontextmanager
from sqlalchemy import text
from .engine import SessionLocal
from .engine import set_statement_timeout

@asynccontextmanager
async def uow(read_only: bool = False):
    async with SessionLocal() as session:
        # Apply session-level settings before any statements
        try:
            await set_statement_timeout(session)
        except Exception:
            # Non-fatal; continue without a server-side timeout
            pass

        # Optionally set read-only mode at the session level
        if read_only:
            try:
                await session.execute(text("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"))
            except Exception:
                pass

        try:
            yield session
        finally:
            # No implicit commit/rollback; caller controls transaction boundaries
            pass
