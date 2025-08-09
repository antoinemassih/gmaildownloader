# db_writer.py
from typing import List, Dict
from db_client import get_supabase_client

DEFAULT_CHUNK = 500

def upsert_rows(table: str, rows: List[Dict], chunk_size: int = DEFAULT_CHUNK) -> None:
    if not rows:
        return
    sb = get_supabase_client()
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        try:
            res = sb.table(table).upsert(chunk, on_conflict="message_id").execute()
            data = getattr(res, "data", None)
            total += len(data or [])
        except Exception as e:
            print(f"[Supabase] Upsert error on rows {i}-{i+len(chunk)-1}: {e}")
            raise
    print(f"[Supabase] Upserted {total} rows into {table}.")
