#!/usr/bin/env python3
"""
Simple Supabase Postgres connection tester.

Usage examples:
  # Use URL from env via our config (recommended)
  python -m gmaildownloader.test_db_connection

  # Or pass a URL explicitly
  python -m gmaildownloader.test_db_connection --url "postgresql://postgres:PASS@HOST:5432/postgres"

Notes:
- Loads .env using the same logic as db/config.py
- Uses asyncpg directly to test reachability, SSL, and credentials
"""
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

# Ensure parent directory is importable if run as a file
sys.path.append(str(Path(__file__).resolve().parents[1]))

import asyncpg  # type: ignore
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


def _get_url(cli_url: Optional[str]) -> str:
    if cli_url:
        url = cli_url.strip()
    else:
        # Import our config to load .env and derive DB_URL if needed
        from gmaildownloader.db.config import DB_URL  # noqa: WPS433
        url = DB_URL
    # asyncpg expects postgresql:// scheme (not postgresql+asyncpg://)
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return url


async def _test(url: str, timeout: float, ssl_required: bool) -> int:
    print("Testing connection to:")
    safe_url = url
    # redact password segment
    if "://" in safe_url and "@" in safe_url:
        scheme, rest = safe_url.split("://", 1)
        if ":" in rest and "@" in rest:
            before_at = rest.split("@", 1)[0]
            if ":" in before_at:
                user, _ = before_at.split(":", 1)
                rest = rest.replace(before_at + "@", f"{user}:***@", 1)
        safe_url = f"{scheme}://{rest}"
    print(f"  {safe_url}")
    try:
        ssl_arg = _SSL_CONTEXT if ssl_required else False
        conn = await asyncpg.connect(dsn=url, ssl=ssl_arg, timeout=timeout)
        try:
            row = await conn.fetchrow("select version(), current_user, current_database(), inet_client_addr(), inet_server_addr();")
            print("SUCCESS: Connected")
            print(f"  version       : {row['version']}")
            print(f"  current_user  : {row['current_user']}")
            print(f"  database      : {row['current_database']}")
            print(f"  client_addr   : {row['inet_client_addr']}")
            print(f"  server_addr   : {row['inet_server_addr']}")
            return 0
        finally:
            await conn.close()
    except Exception as e:  # noqa: BLE001
        print("FAIL: Could not connect")
        print(f"  error: {type(e).__name__}: {e}")
        print("  hints: \n    - Check network/VPN/firewall allows outbound 5432\n    - Verify host/port from Supabase Dashboard (pooled host if provided)\n    - Ensure correct DB password (not service role key)\n    - If using a corporate network, try hotspot or different network")
        return 2


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Test Supabase Postgres connectivity using asyncpg")
    parser.add_argument("--url", help="Postgres URL (overrides env)")
    parser.add_argument("--timeout", type=float, default=15.0, help="Connect timeout seconds (default: 15)")
    parser.add_argument("--no-ssl", action="store_true", help="Disable SSL (not recommended; Supabase requires SSL)")
    args = parser.parse_args()

    url = _get_url(args.url)
    ssl_required = not args.no_ssl

    return asyncio.run(_test(url, args.timeout, ssl_required))


if __name__ == "__main__":
    raise SystemExit(main())
