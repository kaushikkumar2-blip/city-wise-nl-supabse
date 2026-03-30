"""
One-time migration: upload existing CSV data to Supabase.

Usage:
    py -u migrate_to_supabase.py          (full upload)
    py -u migrate_to_supabase.py --skip N  (resume from row N)
"""

import os
import sys
import time
import json
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CSV_PATH = "362c62a8adb9d17ecb5a6c9d33385822.csv"

try:
    import tomllib
except ImportError:
    import tomli as tomllib

_secrets_path = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
with open(_secrets_path, "rb") as f:
    _secrets = tomllib.load(f)

SUPABASE_URL = _secrets["supabase"]["url"]
SUPABASE_KEY = _secrets["supabase"]["key"]

DB_COLS = [
    "reporting_date", "destination_city", "seller_type", "payment_type",
    "phin", "conv_num", "zero_attempt_num", "fm_created", "fm_picked",
    "fm_d0_picked", "dhin", "d0_ofd", "first_attempt_delivered", "fac_deno",
    "total_delivered_attempts", "total_attempts", "rfr_num", "rfr_deno",
    "breach_num", "breach_den", "breach_plus1_num",
]

BATCH_SIZE = 3000


def main():
    skip = 0
    if "--skip" in sys.argv:
        skip = int(sys.argv[sys.argv.index("--skip") + 1])

    print(f"Reading {CSV_PATH} ...", flush=True)
    df = pd.read_csv(CSV_PATH, low_memory=False)
    print(f"  {len(df):,} rows, {len(df.columns)} columns", flush=True)

    df.columns = df.columns.str.lower()

    for col in DB_COLS:
        if col not in df.columns:
            print(f"  WARNING: column '{col}' missing, filling with 0", flush=True)
            df[col] = 0

    df = df[DB_COLS]

    numeric_cols = [c for c in DB_COLS if c not in ("reporting_date", "destination_city", "seller_type", "payment_type")]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df["reporting_date"] = df["reporting_date"].astype(str)

    # Fill nulls in NOT NULL text columns
    df["destination_city"] = df["destination_city"].fillna("UNKNOWN")
    df["seller_type"] = df["seller_type"].fillna("UNKNOWN")
    df["payment_type"] = df["payment_type"].fillna("UNKNOWN")

    # Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    if before != after:
        print(f"  Removed {before - after:,} duplicate rows ({before:,} -> {after:,})", flush=True)

    if skip > 0:
        print(f"  Skipping first {skip:,} rows (already uploaded)", flush=True)
        df = df.iloc[skip:]

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    total = len(df)
    inserted = 0
    failed_batches = []
    start = time.time()

    print(f"\nUploading {total:,} rows in batches of {BATCH_SIZE} ...", flush=True)

    for i in range(0, total, BATCH_SIZE):
        batch_df = df.iloc[i : i + BATCH_SIZE]
        records = batch_df.to_dict(orient="records")

        for attempt in range(3):
            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/shipments",
                    headers=headers,
                    data=json.dumps(records, default=str),
                    timeout=120,
                    verify=False,
                )
                if r.status_code in (200, 201):
                    inserted += len(records)
                    break
                else:
                    err_msg = r.text[:200] if attempt == 0 else ""
                    print(f"  Batch {i//BATCH_SIZE}: HTTP {r.status_code} (attempt {attempt+1}) {err_msg}", flush=True)
                    if attempt == 2:
                        failed_batches.append((i, r.status_code, r.text[:200]))
            except Exception as e:
                print(f"  Batch {i//BATCH_SIZE}: error (attempt {attempt+1}): {e}", flush=True)
                if attempt == 2:
                    failed_batches.append((i, "error", str(e)[:200]))
            time.sleep(2)

        elapsed = time.time() - start
        pct = inserted / total * 100
        rate = inserted / elapsed if elapsed > 0 else 0
        eta = (total - inserted) / rate / 60 if rate > 0 else 0
        sys.stdout.write(f"\r  {inserted:>10,} / {total:,}  ({pct:5.1f}%)  {rate:.0f} rows/s  ETA {eta:.1f} min  ")
        sys.stdout.flush()

    elapsed = time.time() - start
    print(f"\n\nDone in {elapsed/60:.1f} minutes.", flush=True)
    print(f"  Inserted: {inserted:,}", flush=True)
    print(f"  Failed batches: {len(failed_batches)}", flush=True)

    if failed_batches:
        print("\nFailed batches:", flush=True)
        for offset, status, text in failed_batches:
            print(f"  offset={offset}: {status} — {text}", flush=True)


if __name__ == "__main__":
    main()
