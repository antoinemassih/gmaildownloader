#!/usr/bin/env python3
"""
Export Gmail subjects to CSV with robust pagination + rate-limit backoff.
Parses each subject via subject_parser.parse_trade_subject.

Usage:
  python gmail_subject_exporter.py --query "from:alerts@thinkorswim.com is:anywhere" --csv "./trades.csv"
"""

import argparse
import csv
import random
import sys
import time
from pathlib import Path
from typing import Dict, Generator, Optional, Tuple
from email.utils import parsedate_to_datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from subject_parser import parse_trade_subject

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
RATE_LIMIT_HTTP_STATUSES = {429, 500, 503}
RATE_LIMIT_REASONS = {"rateLimitExceeded", "userRateLimitExceeded", "backendError", "quotaExceeded"}


def is_rate_limited_error(e: HttpError) -> bool:
    status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
    if status in RATE_LIMIT_HTTP_STATUSES:
        return True
    try:
        body = e.content.decode("utf-8") if getattr(e, "content", None) else ""
        return any(r in body for r in RATE_LIMIT_REASONS)
    except Exception:
        return False


def with_backoff(callable_fn, *args, max_tries: int = 12, base_sleep: float = 1.0, **kwargs):
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
        except (TimeoutError, OSError, ConnectionError):
            attempt += 1
            if attempt >= max_tries:
                raise
            sleep_s = random.uniform(0, base_sleep * (2 ** (attempt - 1)))
            time.sleep(sleep_s)


def load_service(credentials_path: Path, token_path: Path):
    creds: Optional[Credentials] = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def list_message_ids(service, query: str, max_results: int = 500) -> Generator[str, None, None]:
    next_page = None
    while True:
        resp = with_backoff(
            service.users().messages().list(
                userId="me", q=query, maxResults=max_results, pageToken=next_page
            ).execute
        )
        for m in resp.get("messages", []):
            yield m["id"]
        next_page = resp.get("nextPageToken")
        if not next_page:
            break


def _parse_date_header(date_value: str) -> Tuple[str, Optional[str], Optional[int]]:
    """Returns (date_raw, date_iso, date_unix_ts)."""
    date_raw = date_value or ""
    if not date_raw:
        return "", None, None
    try:
        dt = parsedate_to_datetime(date_raw)
        if dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        return date_raw, dt.isoformat(), int(dt.timestamp())
    except Exception:
        return date_raw, None, None


def fetch_subject_headers(service, msg_id: str) -> Dict[str, Optional[str]]:
    resp = with_backoff(
        service.users().messages().get(
            userId="me",
            id=msg_id,
            format="metadata",
            metadataHeaders=["Subject", "Date", "From", "To"],
        ).execute
    )
    headers = {h["name"].lower(): h["value"] for h in resp.get("payload", {}).get("headers", [])}
    date_raw, date_iso, date_unix_ts = _parse_date_header(headers.get("date", ""))
    return {
        "id": resp.get("id"),
        "threadId": resp.get("threadId"),
        "subject": headers.get("subject", ""),
        "date_raw": date_raw,
        "date_iso": date_iso,
        "date_unix_ts": date_unix_ts,
        "from_email": headers.get("from", ""),
        "to_email": headers.get("to", ""),
    }


def main():
    ap = argparse.ArgumentParser(description="Export parsed Thinkorswim subjects to CSV.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--query", type=str, help="Raw Gmail search query (e.g., 'from:alerts@thinkorswim.com is:anywhere').")
    g.add_argument("--sender", type=str, help="Convenience: sender email; we'll build 'from:SENDER is:anywhere'.")
    ap.add_argument("--csv", type=Path, required=True, help="Output CSV path.")
    ap.add_argument("--resume-file", type=Path, default=Path("processed_ids.txt"))
    ap.add_argument("--max-results", type=int, default=500, help="Gmail page size (max 500).")
    ap.add_argument("--base-sleep", type=float, default=1.0, help="Base sleep for backoff.")
    ap.add_argument("--limit", type=int, default=0, help="Process only N messages (0 = no limit).")
    args = ap.parse_args()

    query = args.query or f"from:{args.sender} is:anywhere"

    credentials_path = Path("credentials.json")
    token_path = Path("token.json")
    service = load_service(credentials_path, token_path)

    profile = with_backoff(service.users().getProfile(userId="me").execute)
    print("Using Gmail account:", profile.get("emailAddress"))
    print("Gmail query:", query)

    processed = set()
    if args.resume_file.exists():
        processed.update(x.strip() for x in args.resume_file.read_text().splitlines() if x.strip())

    # Clean, DB-friendly header names
    header = [
        "message_id",
        "thread_id",
        "date_raw",
        "date_iso",
        "date_unix_ts",
        "from_email",
        "to_email",
        "subject_raw",

        "parse_ok",
        "trade_id",
        "side",                # BUY / SELL
        "quantity_signed",
        "quantity_abs",
        "instrument",
        "contract_code",
        "option_expiry",
        "strike",
        "option_type",
        "fill_price",
        "underlying_mark",
        "implied_vol",
        "account",
    ]
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    csv_exists = args.csv.exists()
    csv_fp = open(args.csv, "a", newline="", encoding="utf-8")
    writer = csv.writer(csv_fp)
    if not csv_exists:
        writer.writerow(header)

    count_msgs = 0
    try:
        for msg_id in list_message_ids(service, query, max_results=args.max_results):
            if args.limit and count_msgs >= args.limit:
                break
            if msg_id in processed:
                continue

            hdr = fetch_subject_headers(service, msg_id)
            parsed = parse_trade_subject(hdr["subject"])

            row = [
                hdr["id"],
                hdr["threadId"],
                hdr["date_raw"],
                hdr["date_iso"],
                hdr["date_unix_ts"],
                hdr["from_email"],
                hdr["to_email"],
                hdr["subject"],

                parsed["parse_ok"],
                parsed["trade_id"],
                parsed["side"],
                parsed["quantity_signed"],
                parsed["quantity_abs"],
                parsed["instrument"],
                parsed["contract_code"],
                parsed["option_expiry"],
                parsed["strike"],
                parsed["option_type"],
                parsed["fill_price"],
                parsed["underlying_mark"],
                parsed["implied_vol"],
                parsed["account"],
            ]
            writer.writerow(row)
            csv_fp.flush()

            with open(args.resume_file, "a", encoding="utf-8") as rf:
                rf.write(msg_id + "\n")
            processed.add(msg_id)

            count_msgs += 1
            if count_msgs % 500 == 0:
                print(f"Processed {count_msgs} messages...")

    except KeyboardInterrupt:
        print("\nInterrupted. Progress saved.", file=sys.stderr)
    finally:
        csv_fp.close()

    print(f"Done. Messages processed: {count_msgs}")
    print(f"CSV: {args.csv.resolve()}")
    print(f"Processed list: {args.resume_file.resolve()}")


if __name__ == "__main__":
    main()
