"""
Supabase database helper — provides read/write access to the shipments table.

On Streamlit Cloud:  uses psycopg2 (direct PostgreSQL, fastest).
On local / corporate: falls back to REST API over HTTPS.
"""

import os
import ssl
import pandas as pd
import streamlit as st

# Column mapping: DB (lowercase) <-> DataFrame (mixed-case used by dashboard)
DB_TO_DF = {
    "reporting_date": "reporting_date",
    "destination_city": "destination_city",
    "seller_type": "seller_type",
    "payment_type": "payment_type",
    "phin": "PHin",
    "conv_num": "conv_num",
    "zero_attempt_num": "zero_attempt_num",
    "fm_created": "fm_created",
    "fm_picked": "fm_picked",
    "fm_d0_picked": "fm_d0_picked",
    "dhin": "DHin",
    "d0_ofd": "D0_OFD",
    "first_attempt_delivered": "First_attempt_delivered",
    "fac_deno": "fac_deno",
    "total_delivered_attempts": "total_delivered_attempts",
    "total_attempts": "total_attempts",
    "rfr_num": "rfr_num",
    "rfr_deno": "rfr_deno",
    "breach_num": "Breach_Num",
    "breach_den": "Breach_Den",
    "breach_plus1_num": "breach_plus1_num",
}
DF_TO_DB = {v: k for k, v in DB_TO_DF.items()}

NUMERIC_DB_COLS = [
    "phin", "conv_num", "zero_attempt_num", "fm_created", "fm_picked",
    "fm_d0_picked", "dhin", "d0_ofd", "first_attempt_delivered", "fac_deno",
    "total_delivered_attempts", "total_attempts", "rfr_num", "rfr_deno",
    "breach_num", "breach_den", "breach_plus1_num",
]

DATA_DB_COLS = [
    "reporting_date", "destination_city", "seller_type", "payment_type",
] + NUMERIC_DB_COLS


def _get_secrets():
    """Read Supabase secrets from Streamlit secrets or environment."""
    try:
        return dict(st.secrets["postgres"]), dict(st.secrets["supabase"])
    except Exception:
        return None, None


def _try_psycopg2(pg_conf: dict) -> pd.DataFrame | None:
    """Attempt a direct PostgreSQL read (fastest, works on Streamlit Cloud)."""
    try:
        import psycopg2
        conf = {**pg_conf, "sslmode": "require", "connect_timeout": 15}
        conf["port"] = int(conf.get("port", 5432))
        conn = psycopg2.connect(**conf)
        df = pd.read_sql_query(
            f"SELECT {', '.join(DATA_DB_COLS)} FROM shipments", conn
        )
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Direct DB connection failed: {e}")
        return None


def _rest_fetch_all(supa_conf: dict) -> pd.DataFrame | None:
    """Fetch all rows via Supabase REST API with pagination."""
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    url = supa_conf["url"]
    key = supa_conf["key"]
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "text/csv",
        "Prefer": "count=exact",
    }

    all_frames = []
    page_size = 1000
    offset = 0

    while True:
        ep = f"{url}/rest/v1/shipments?select={','.join(DATA_DB_COLS)}&limit={page_size}&offset={offset}"
        try:
            r = requests.get(ep, headers=headers, timeout=120)
        except Exception as e:
            st.error(f"REST request error: {e}")
            return None

        if r.status_code != 200:
            st.error(f"REST API returned {r.status_code}: {r.text[:300]}")
            return None
        from io import StringIO
        chunk = pd.read_csv(StringIO(r.text))
        if chunk.empty:
            break
        all_frames.append(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    if not all_frames:
        st.warning("REST API returned no data rows.")
        return None
    return pd.concat(all_frames, ignore_index=True)


def load_from_supabase() -> pd.DataFrame | None:
    """Load all shipment rows. Tries psycopg2 first, then REST API."""
    pg_conf, supa_conf = _get_secrets()

    if not pg_conf and not supa_conf:
        st.error("No database credentials found in Streamlit secrets.")
        return None

    if pg_conf:
        df = _try_psycopg2(pg_conf)
        if df is not None:
            df.rename(columns=DB_TO_DF, inplace=True)
            return df

    if supa_conf:
        df = _rest_fetch_all(supa_conf)
        if df is not None:
            df.rename(columns=DB_TO_DF, inplace=True)
            return df
        else:
            st.error("REST API fallback also failed.")

    return None


def insert_rows(df: pd.DataFrame) -> tuple[bool, str]:
    """Insert DataFrame rows into the shipments table.
    Returns (success, message)."""
    renamed = df.rename(columns=DF_TO_DB)
    cols_to_use = [c for c in DATA_DB_COLS if c in renamed.columns]
    renamed = renamed[cols_to_use]

    pg_conf, supa_conf = _get_secrets()

    if pg_conf:
        ok, msg = _insert_psycopg2(renamed, pg_conf)
        if ok:
            return True, msg

    if supa_conf:
        return _insert_rest(renamed, supa_conf)

    return False, "No database credentials configured."


def _insert_psycopg2(df: pd.DataFrame, pg_conf: dict) -> tuple[bool, str]:
    """Bulk insert via psycopg2 COPY (fastest)."""
    try:
        import psycopg2
        from io import StringIO
        conn = psycopg2.connect(**pg_conf, connect_timeout=10)
        cur = conn.cursor()
        buf = StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert(
            f"COPY shipments ({', '.join(df.columns)}) FROM STDIN WITH CSV",
            buf,
        )
        conn.commit()
        count = cur.rowcount
        cur.close()
        conn.close()
        return True, f"Inserted {count:,} rows via direct connection."
    except Exception as e:
        return False, str(e)


def _insert_rest(df: pd.DataFrame, supa_conf: dict) -> tuple[bool, str]:
    """Insert via Supabase REST API in batches."""
    import requests
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    url = supa_conf["url"]
    key = supa_conf["key"]
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    verify = not os.environ.get("SUPABASE_NO_SSL_VERIFY")
    records = df.to_dict(orient="records")
    batch_size = 5000
    inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        try:
            r = requests.post(
                f"{url}/rest/v1/shipments",
                headers=headers,
                json=batch,
                timeout=120,
                verify=verify,
            )
        except requests.exceptions.SSLError:
            r = requests.post(
                f"{url}/rest/v1/shipments",
                headers=headers,
                json=batch,
                timeout=120,
                verify=False,
            )
        if r.status_code not in (200, 201):
            return False, f"REST insert failed at row {i}: {r.status_code} {r.text[:300]}"
        inserted += len(batch)

    return True, f"Inserted {inserted:,} rows via REST API."
