# verify_supabase.py
import time
from db_client import get_supabase_client

sb = get_supabase_client()

# Make sure the table exists first (see step 6)
row = {"message_id": f"healthcheck-{int(time.time())}", "thread_id": "hc"}
res = sb.table("trades").upsert(row, on_conflict="message_id").execute()
print("Inserted/updated rows:", len(getattr(res, "data", []) or []))
print("OK")
