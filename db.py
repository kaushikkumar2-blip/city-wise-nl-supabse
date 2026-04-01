"""
Supabase database helper — provides read/write access to the shipments table.

Uses psycopg2 with COPY for fast bulk data transfer.
Automatically falls back to Supavisor connection pooler when direct
IPv6 connections are unavailable (e.g. Streamlit Cloud).
"""

import os
from io import StringIO
from urllib.parse import unquote, urlparse

import pandas as pd
import streamlit as st

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

DEFAULT_DAYS = 0

POOLER_REGIONS = [
    "ap-south-1", "ap-southeast-1", "ap-southeast-2",
    "ap-northeast-1", "ap-northeast-2",
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-central-2",
    "ca-central-1", "sa-east-1", "me-south-1", "af-south-1",
]


def _pg_from_database_url() -> dict | None:
    """Parse postgres:// or postgresql:// URL (Railway, Supabase connection strings)."""
    raw = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DB_URL")
        or os.environ.get("POSTGRES_URL")
        or ""
    ).strip()
    if not raw:
        return None
    try:
        if raw.startswith("postgres://"):
            raw = "postgresql://" + raw[len("postgres://") :]
        p = urlparse(raw)
        if not p.hostname:
            return None
        path = (p.path or "").lstrip("/") or "postgres"
        return {
            "host": p.hostname,
            "port": str(p.port or 5432),
            "dbname": path,
            "user": unquote(p.username or ""),
            "password": unquote(p.password or ""),
        }
    except Exception:
        return None


def _get_secrets():
    """Streamlit Cloud / local: st.secrets. Railway / Docker: env vars."""
    pg_conf = None
    supa_conf = None
    try:
        sec = st.secrets
        pg_conf = dict(sec["postgres"])
        try:
            supa_conf = dict(sec["supabase"])
        except Exception:
            supa_conf = {}
    except Exception:
        pass

    if not pg_conf or not pg_conf.get("host"):
        host = os.environ.get("POSTGRES_HOST") or os.environ.get("PGHOST")
        if host:
            pg_conf = {
                "host": host,
                "port": os.environ.get("POSTGRES_PORT") or os.environ.get("PGPORT") or "5432",
                "dbname": os.environ.get("POSTGRES_DB") or os.environ.get("PGDATABASE") or "postgres",
                "user": os.environ.get("POSTGRES_USER") or os.environ.get("PGUSER"),
                "password": os.environ.get("POSTGRES_PASSWORD")
                or os.environ.get("PGPASSWORD")
                or "",
            }

    if not pg_conf or not pg_conf.get("host"):
        pg_conf = _pg_from_database_url()

    if not supa_conf or not supa_conf.get("url"):
        url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
        key = (
            os.environ.get("SUPABASE_KEY")
            or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            or ""
        ).strip()
        if url and key:
            supa_conf = {"url": url, "key": key}

    return pg_conf, supa_conf


def _build_pooler_conf(pg_conf: dict) -> dict | None:
    host = pg_conf.get("host", "")
    parts = host.split(".")
    if len(parts) >= 3 and parts[0] == "db" and "supabase" in host:
        project_ref = parts[1]
        return {**pg_conf, "user": f"postgres.{project_ref}"}
    return None


def _connect(pg_conf: dict):
    """Connect via direct host, falling back to Supavisor pooler."""
    import psycopg2

    conf = {**pg_conf, "sslmode": "require", "connect_timeout": 15}
    conf["port"] = int(conf.get("port", 5432))

    try:
        return psycopg2.connect(**conf)
    except Exception:
        pass

    pooler_base = _build_pooler_conf(conf)
    if not pooler_base:
        raise ConnectionError("Cannot derive pooler config from host.")

    last_err = None
    for prefix in ("aws-0", "aws-1"):
        for region in POOLER_REGIONS:
            pooler_conf = {
                **pooler_base,
                "host": f"{prefix}-{region}.pooler.supabase.com",
                "connect_timeout": 5,
            }
            try:
                return psycopg2.connect(**pooler_conf)
            except Exception as e:
                last_err = e
    raise ConnectionError(f"All pooler regions failed. Last: {last_err}")


def load_from_supabase(days: int = DEFAULT_DAYS) -> pd.DataFrame | None:
    """Load shipment rows using COPY (fast CSV streaming).

    Args:
        days: only fetch the last N days of data. 0 = all data.
    """
    pg_conf, _ = _get_secrets()
    if not pg_conf or not pg_conf.get("host"):
        st.error(
            "No database credentials. On **Railway**, set either `DATABASE_URL` "
            "(paste your Supabase pooler URI) or `POSTGRES_HOST`, `POSTGRES_USER`, "
            "`POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`. "
            "On **Streamlit Cloud**, use Secrets with a `[postgres]` section."
        )
        return None

    cols = ", ".join(DATA_DB_COLS)
    where = ""
    if days > 0:
        where = (
            f" WHERE reporting_date >= "
            f"to_char(current_date - interval '{days} days', 'YYYYMMDD')"
        )

    sql = f"COPY (SELECT {cols} FROM shipments{where}) TO STDOUT WITH CSV HEADER"

    try:
        conn = _connect(pg_conf)
        cur = conn.cursor()
        buf = StringIO()
        cur.copy_expert(sql, buf)
        buf.seek(0)
        df = pd.read_csv(buf)
        cur.close()
        conn.close()
        df.rename(columns=DB_TO_DF, inplace=True)
        return df
    except Exception as e:
        st.error(f"Database load failed: {e}")
        return None


def insert_rows(df: pd.DataFrame) -> tuple[bool, str]:
    """Insert DataFrame rows into the shipments table via COPY."""
    renamed = df.rename(columns=DF_TO_DB)
    cols_to_use = [c for c in DATA_DB_COLS if c in renamed.columns]
    renamed = renamed[cols_to_use]

    pg_conf, supa_conf = _get_secrets()

    if pg_conf:
        ok, msg = _insert_psycopg2(renamed, pg_conf)
        if ok:
            return True, msg

    if supa_conf and supa_conf.get("url") and supa_conf.get("key"):
        return _insert_rest(renamed, supa_conf)

    return False, "No database credentials configured."


def _insert_psycopg2(df: pd.DataFrame, pg_conf: dict) -> tuple[bool, str]:
    try:
        conn = _connect(pg_conf)
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
        return True, f"Inserted {count:,} rows."
    except Exception as e:
        return False, str(e)


def _insert_rest(df: pd.DataFrame, supa_conf: dict) -> tuple[bool, str]:
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
                f"{url}/rest/v1/shipments", headers=headers,
                json=batch, timeout=120, verify=verify,
            )
        except requests.exceptions.SSLError:
            r = requests.post(
                f"{url}/rest/v1/shipments", headers=headers,
                json=batch, timeout=120, verify=False,
            )
        if r.status_code not in (200, 201):
            return False, f"REST insert failed at row {i}: {r.status_code} {r.text[:300]}"
        inserted += len(batch)

    return True, f"Inserted {inserted:,} rows via REST API."
