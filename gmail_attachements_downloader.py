#!/usr/bin/env python3
"""
Download all Gmail attachments from a given sender, handling pagination and rate limits.

Usage examples:
  python gmail_attachments_downloader.py --sender "invoices@vendor.com" --out "/path/to/folder"
  python gmail_attachments_downloader.py --query 'from:invoices@vendor.com has:attachment is:anywhere' --out "./out"

Notes:
- Place your OAuth client file as ./credentials.json (created in Google Cloud Console).
- A ./token.json will be created after first auth and reused on subsequent runs.
- Safe to re-run: processed message IDs are persisted; already-handled messages are skipped.
"""

import argparse
import base64
import csv
import hashlib
import json
import os
import random
import re
import sys
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Read-only is enough
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

RATE_LIMIT_HTTP_STATUSES = {429, 500, 503}
RATE_LIMIT_REASONS = {
    "rateLimitExceeded",
    "userRateLimitExceeded",
    "backendError",
    "quotaExceeded",
}

SAFE_FILENAME_CHARS = "-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

def sanitize_filename(name: str, substitute: str = "_") -> str:
    name = name.strip()
    return "".join(c if c in SAFE_FILENAME_CHARS else substitute for c in name) or "file"

def short_id(s: str) -> str:
    return s[:10]

def load_or_create_service(credentials_path: Path, token_path: Path):
    creds: Optional[Credentials] = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Missing {credentials_path}. Create a Desktop OAuth client in Google Cloud Console and save it here."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
    # cache_discovery=False avoids file cache warnings
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def is_rate_limited_error(e: HttpError) -> bool:
    try:
        status = e.resp.status
    except Exception:
        status = None
    if status in RATE_LIMIT_HTTP_STATUSES:
        return True
    try:
        err = e.error_details  # newer clients
    except Exception:
        err = None
    # Fallback to parsing content
    try:
        data = e.content.decode("utf-8") if hasattr(e, "content") and e.content else ""
        for reason in RATE_LIMIT_REASONS:
            if reason in data:
                return True
    except Exception:
        pass
    return False

def with_backoff(callable_fn, *args, max_tries: int = 12, base_sleep: float = 1.0, **kwargs):
    """
    Generic exponential backoff with full jitter for Gmail API calls.
    """
    attempt = 0
    while True:
        try:
            return callable_fn(*args, **kwargs)
        except HttpError as e:
            attempt += 1
            if attempt >= max_tries or not is_rate_limited_error(e):
                raise
            sleep_s = random.uniform(0, base_sleep * (2 ** (attempt - 1)))
            time.sleep(sleep_s)
        except (TimeoutError, OSError, ConnectionError) as e:
            attempt += 1
            if attempt >= max_tries:
                raise
            sleep_s = random.uniform(0, base_sleep * (2 ** (attempt - 1)))
            time.sleep(sleep_s)

def list_message_ids(service, query: str, max_results: int = 500) -> Generator[str, None, None]:
    """
    Generator that yields all message IDs matching the query, across all pages.
    """
    next_page_token = None
    while True:
        resp = with_backoff(
            service.users().messages().list(userId="me", q=query, maxResults=max_results, pageToken=next_page_token).execute
        )
        msgs = resp.get("messages", [])
        for m in msgs:
            yield m["id"]
        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

def fetch_message(service, msg_id: str) -> dict:
    return with_backoff(service.users().messages().get(userId="me", id=msg_id, format="full").execute)

def fetch_attachment(service, msg_id: str, attach_id: str) -> bytes:
    resp = with_backoff(
        service.users().messages().attachments().get(userId="me", messageId=msg_id, id=attach_id).execute
    )
    data = resp.get("data")
    if not data:
        return b""
    return base64.urlsafe_b64decode(data.encode("utf-8"))

def iter_parts(part: dict) -> Iterable[dict]:
    """
    Recursively yield all leaf parts.
    """
    if not part:
        return
    parts = part.get("parts")
    if parts:
        for p in parts:
            yield from iter_parts(p)
    else:
        yield part

def message_headers_index(msg: dict) -> Dict[str, str]:
    headers = msg.get("payload", {}).get("headers", [])
    idx = {}
    for h in headers:
        name = h.get("name", "").lower()
        value = h.get("value", "")
        if name:
            idx[name] = value
    return idx

def decode_inline_data(part: dict) -> Optional[bytes]:
    body = part.get("body", {})
    data = body.get("data")
    if data:
        try:
            return base64.urlsafe_b64decode(data.encode("utf-8"))
        except Exception:
            return None
    return None

def compute_sha256(content: bytes) -> str:
    h = hashlib.sha256()
    h.update(content)
    return h.hexdigest()

def build_filename_prefix(msg: dict) -> str:
    idx = message_headers_index(msg)
    subject = sanitize_filename(idx.get("subject", "No Subject"))
    date_str = idx.get("date", "")
    try:
        dt = parsedate_to_datetime(date_str)
        # Normalize timezone-aware -> date string
        date_tag = dt.strftime("%Y%m%d")
    except Exception:
        date_tag = "unknown_date"
    return f"{date_tag}__{subject}__{short_id(msg.get('id',''))}"

def save_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)

def main():
    parser = argparse.ArgumentParser(description="Download Gmail attachments from a sender (handles pagination & rate limits).")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--sender", type=str, help="Email address to pull from (we'll build a Gmail query for you).")
    g.add_argument("--query", type=str, help="Raw Gmail search query (e.g., 'from:foo@bar.com has:attachment is:anywhere').")
    parser.add_argument("--out", type=Path, required=True, help="Output folder where attachments will be saved.")
    parser.add_argument("--max-results", type=int, default=500, help="Messages per page (max 500).")
    parser.add_argument("--base-sleep", type=float, default=1.0, help="Base sleep for backoff (seconds).")
    parser.add_argument("--flat", action="store_true", help="Save all attachments flat into --out (default).")
    parser.add_argument("--structured", action="store_true", help="Save attachments into per-message subfolders.")
    parser.add_argument("--include-inline", action="store_true", help="Also save inline parts that have filenames.")
    parser.add_argument("--resume-file", type=Path, default=Path("processed_ids.txt"), help="File to persist processed message IDs.")
    parser.add_argument("--manifest", type=Path, default=Path("manifest.csv"), help="CSV manifest path.")
    args = parser.parse_args()

    if not args.flat and not args.structured:
        args.flat = True  # default behavior

    credentials_path = Path("credentials.json")
    token_path = Path("token.json")
    service = load_or_create_service(credentials_path, token_path)

    # Build Gmail query
    if args.query:
        query = args.query
    else:
        # include has:attachment and search anywhere (including spam/trash if you want)
        query = f'from:{args.sender} has:attachment is:anywhere'

    # Prepare resume set
    processed: set = set()
    if args.resume_file.exists():
        processed.update(x.strip() for x in args.resume_file.read_text().splitlines() if x.strip())

    # Prepare manifest CSV
    manifest_exists = args.manifest.exists()
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest_fp = open(args.manifest, "a", newline="", encoding="utf-8")
    manifest_csv = csv.writer(manifest_fp)
    if not manifest_exists:
        manifest_csv.writerow([
            "message_id",
            "thread_id",
            "date_iso",
            "from",
            "to",
            "subject",
            "attachment_filename",
            "sha256",
            "bytes",
            "saved_path"
        ])

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    count_msgs = 0
    count_atts = 0

    try:
        for msg_id in list_message_ids(service, query, max_results=args.max_results):
            if msg_id in processed:
                continue

            msg = fetch_message(service, msg_id)
            payload = msg.get("payload", {})
            idx = message_headers_index(msg)

            subject = idx.get("subject", "No Subject")
            msg_date_iso = ""
            try:
                msg_date = parsedate_to_datetime(idx.get("date", ""))
                msg_date_iso = msg_date.isoformat()
            except Exception:
                pass

            prefix = build_filename_prefix(msg)  # date__subject__shortid

            # Decide base path for attachments
            if args.structured:
                base_path = out_dir / sanitize_filename(prefix)
            else:
                base_path = out_dir

            # Walk parts and save attachments
            saved_any = False
            for part in iter_parts(payload):
                filename = part.get("filename") or ""
                body = part.get("body", {}) or {}

                # Decide if this is an attachment we want:
                # Gmail uses filename != "" as the indicator; inline files may also have filenames.
                if not filename:
                    continue
                if not args.include_inline:
                    # If no attachmentId and data is inline, skip unless user opted in
                    if not body.get("attachmentId"):
                        continue

                data_bytes: Optional[bytes] = None
                if body.get("attachmentId"):
                    data_bytes = fetch_attachment(service, msg_id, body["attachmentId"])
                else:
                    # inline data (if include-inline specified)
                    data_bytes = decode_inline_data(part)

                if not data_bytes:
                    continue

                # Build a collision-safe filename
                clean_name = sanitize_filename(filename)
                # Add a robust prefix so a flat directory won't collide
                if args.flat:
                    fname = f"{prefix}__{clean_name}"
                else:
                    fname = clean_name

                dest_path = base_path / fname

                # If exists, disambiguate
                if dest_path.exists():
                    stem = dest_path.stem
                    suffix = dest_path.suffix
                    i = 1
                    while True:
                        alt = dest_path.with_name(f"{stem}__{i}{suffix}")
                        if not alt.exists():
                            dest_path = alt
                            break
                        i += 1

                # Save and log
                sha = compute_sha256(data_bytes)
                save_bytes(dest_path, data_bytes)
                count_atts += 1
                saved_any = True

                manifest_csv.writerow([
                    msg.get("id", ""),
                    msg.get("threadId", ""),
                    msg_date_iso,
                    idx.get("from", ""),
                    idx.get("to", ""),
                    subject,
                    filename,
                    sha,
                    len(data_bytes),
                    str(dest_path.resolve())
                ])

            # Mark message processed (even if no attachments saved, we skip next time)
            processed.add(msg_id)
            with open(args.resume_file, "a", encoding="utf-8") as rf:
                rf.write(msg_id + "\n")

            count_msgs += 1

    except KeyboardInterrupt:
        print("\nInterrupted by user. Progress saved.", file=sys.stderr)
    finally:
        manifest_fp.flush()
        manifest_fp.close()

    print(f"Done. Messages scanned: {count_msgs}, attachments saved: {count_atts}")
    print(f"Manifest: {args.manifest.resolve()}")
    print(f"Processed list: {args.resume_file.resolve()}")
    print(f"Output folder: {out_dir.resolve()}")

if __name__ == "__main__":
    main()
