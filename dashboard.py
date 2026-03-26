"""
Seller × City Performance Dashboard
====================================
Optimized for large datasets (~1 M+ rows) with:
  • @st.cache_data    — one-time CSV parse & aggregation caching
  • @st.fragment       — isolated seller drill-down (no full-page re-render)
  • Categorical dtypes — 40-60 % RAM reduction on repeated strings
  • Vectorised NumPy   — fast metric computation across columns
  • st.dataframe       — native virtualised scrolling (only visible rows rendered)

Run:
    streamlit run dashboard.py

Requirements:
    pip install streamlit pandas numpy plotly
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime

# ═════════════════════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Seller × City Performance",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═════════════════════════════════════════════════════════════════════════════
#  CUSTOM CSS
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif}
.block-container{padding:1.5rem 2rem 2rem 2rem}

.kpi-card{
    background:#fff;border-radius:12px;padding:0.8rem 1rem;
    border:1px solid #E2E8F0;border-left:4px solid #3B82F6;
    box-shadow:0 1px 3px rgba(0,0,0,0.04);
    transition:transform .15s,box-shadow .15s;
}
.kpi-card:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.08)}
.kpi-card.green{border-left-color:#10B981}
.kpi-card.red{border-left-color:#EF4444}
.kpi-card.orange{border-left-color:#F59E0B}
.kpi-card.purple{border-left-color:#8B5CF6}
.kpi-card.cyan{border-left-color:#06B6D4}
.kpi-label{font-size:.66rem;font-weight:600;color:#94A3B8;
            letter-spacing:.06em;text-transform:uppercase;margin-bottom:3px}
.kpi-value{font-size:1.35rem;font-weight:700;color:#0F172A;
            font-family:'JetBrains Mono',monospace;line-height:1.1}
.kpi-sub{font-size:.66rem;color:#94A3B8;margin-top:3px}

.section-hdr{
    background:linear-gradient(135deg,#1E293B 0%,#334155 100%);
    border-radius:12px;padding:.85rem 1.2rem;margin:.6rem 0;
    color:#fff;display:flex;align-items:center;gap:.75rem;
    box-shadow:0 2px 8px rgba(30,41,59,.18);
}
.section-hdr .ico{font-size:1.2rem;background:rgba(255,255,255,.12);
    border-radius:8px;padding:.35rem .45rem}
.section-hdr .ttl{font-size:.95rem;font-weight:700}
.section-hdr .sub{font-size:.72rem;color:rgba(255,255,255,.7)}

.drill-hdr{
    background:linear-gradient(135deg,#1D4ED8 0%,#3B82F6 100%);
    border-radius:12px;padding:1rem 1.4rem;margin:1rem 0 .5rem 0;
    color:#fff;box-shadow:0 4px 16px rgba(29,78,216,.22);
}
.drill-hdr .ttl{font-size:1.05rem;font-weight:700}
.drill-hdr .sub{font-size:.75rem;color:rgba(255,255,255,.7);margin-top:2px}
</style>""", unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ═════════════════════════════════════════════════════════════════════════════
NUMERIC_COLS = [
    "PHin", "conv_num", "zero_attempt_num", "fm_created", "fm_picked",
    "fm_d0_picked", "DHin", "D0_OFD", "First_attempt_delivered", "fac_deno",
    "total_delivered_attempts", "total_attempts", "rfr_num", "rfr_deno",
    "Breach_Num", "Breach_Den", "breach_plus1_num",
]

AGG_COLS = [
    "PHin", "conv_num", "zero_attempt_num",
    "First_attempt_delivered", "fac_deno",
    "Breach_Num", "Breach_Den",
]

# ═════════════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ═════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=600, show_spinner="Loading CSV data …")
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("float32")
    df["reporting_date"] = df["reporting_date"].astype(str)
    df["destination_city"] = df["destination_city"].astype("category")
    df["seller_type"] = df["seller_type"].astype("category")
    pt = df["payment_type"].str.strip().str.upper()
    df["payment_norm"] = pt.map({"COD": "COD", "PREPAID": "Prepaid"}).astype("category")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  AGGREGATION HELPERS
# ═════════════════════════════════════════════════════════════════════════════
def _safe_pct(num, den):
    """Vectorised safe percentage: returns 0 where denominator is 0."""
    den_safe = np.where(den > 0, den, 1)
    return np.where(den > 0, num / den_safe * 100, 0.0)


def _add_pct_cols(df):
    """Add all percentage metric columns from raw aggregated sums (vectorised)."""
    p = df["PHin"].values.astype("float64")
    df["Conv %"] = np.round(_safe_pct(df["conv_num"].values, p), 2)
    df["ZRTO %"] = np.round(_safe_pct(df["zero_attempt_num"].values, p), 2)
    fd = df["fac_deno"].values.astype("float64")
    df["FAC %"] = np.round(_safe_pct(df["First_attempt_delivered"].values, fd), 2)
    bd = df["Breach_Den"].values.astype("float64")
    df["Breach %"] = np.round(_safe_pct(df["Breach_Num"].values, bd), 2)
    if "cod_vol" in df.columns:
        cv = df["cod_vol"].values.astype("float64")
        pv = df["pp_vol"].values.astype("float64")
        df["COD Share %"] = np.round(_safe_pct(cv, p), 2)
        df["Prepaid Share %"] = np.round(_safe_pct(pv, p), 2)
        df["COD Conv %"] = np.round(_safe_pct(df["cod_conv"].values, cv), 2)
        df["Prepaid Conv %"] = np.round(_safe_pct(df["pp_conv"].values, pv), 2)
    return df


def aggregate_by(df: pd.DataFrame, group_cols, with_payment_split: bool = True):
    """Generic aggregation by one or more grouping columns."""
    if isinstance(group_cols, str):
        group_cols = [group_cols]
    present = [c for c in AGG_COLS if c in df.columns]
    base = df.groupby(group_cols, observed=True)[present].sum().reset_index()
    if with_payment_split:
        cod = (
            df[df["payment_norm"] == "COD"]
            .groupby(group_cols, observed=True)
            .agg(cod_vol=("PHin", "sum"), cod_conv=("conv_num", "sum"))
            .reset_index()
        )
        pp = (
            df[df["payment_norm"] == "Prepaid"]
            .groupby(group_cols, observed=True)
            .agg(pp_vol=("PHin", "sum"), pp_conv=("conv_num", "sum"))
            .reset_index()
        )
        base = (
            base
            .merge(cod, on=group_cols, how="left")
            .merge(pp, on=group_cols, how="left")
        )
    num_cols = base.select_dtypes(include="number").columns
    base[num_cols] = base[num_cols].fillna(0)
    return _add_pct_cols(base).sort_values("PHin", ascending=False)


def overall_kpis(df: pd.DataFrame) -> dict:
    """Compute scalar KPI values from a (possibly filtered) DataFrame."""
    if df.empty:
        return {k: 0 for k in ["Volume", "Conv %", "ZRTO %", "FAC %", "Breach %"]}
    tv = float(df["PHin"].sum())
    td = float(df["conv_num"].sum())
    zn = float(df["zero_attempt_num"].sum())
    fn = float(df["First_attempt_delivered"].sum())
    fd = float(df["fac_deno"].sum())
    bn = float(df["Breach_Num"].sum())
    bd = float(df["Breach_Den"].sum())
    pct = lambda n, d: round(n / d * 100, 2) if d > 0 else 0.0
    return {
        "Volume": int(tv),
        "Conv %": pct(td, tv),
        "ZRTO %": pct(zn, tv),
        "FAC %": pct(fn, fd),
        "Breach %": pct(bn, bd),
    }


# ═════════════════════════════════════════════════════════════════════════════
#  COLOUR / STYLE HELPERS
# ═════════════════════════════════════════════════════════════════════════════
def _clr_breach(v):
    if pd.isna(v) or v == 0:
        return "background-color:#F8FAFC;color:#64748B;"
    if v <= 5:
        return "background-color:#DCFCE7;color:#166534;font-weight:600;"
    if v <= 10:
        return "background-color:#FEF9C3;color:#854D0E;font-weight:600;"
    return "background-color:#FEE2E2;color:#991B1B;font-weight:700;"


def _clr_zrto(v):
    if pd.isna(v) or v == 0:
        return "background-color:#F8FAFC;color:#64748B;"
    if v <= 1.5:
        return "background-color:#DCFCE7;color:#166534;font-weight:600;"
    if v <= 3:
        return "background-color:#FEF9C3;color:#854D0E;font-weight:600;"
    return "background-color:#FEE2E2;color:#991B1B;font-weight:700;"


def _clr_high_good(v):
    if pd.isna(v) or v == 0:
        return "background-color:#F8FAFC;color:#64748B;"
    if v >= 70:
        return "background-color:#DCFCE7;color:#166534;font-weight:600;"
    if v >= 50:
        return "background-color:#FEF9C3;color:#854D0E;font-weight:600;"
    return "background-color:#FEE2E2;color:#991B1B;font-weight:600;"


def _clr_vol(v):
    if pd.isna(v) or v == 0:
        return "background-color:#F8FAFC;color:#64748B;"
    return "background-color:#EFF6FF;color:#1E40AF;font-weight:500;"


PCT_FMT = {
    "Breach %": "{:.1f}%", "FAC %": "{:.1f}%", "ZRTO %": "{:.2f}%",
    "Conv %": "{:.1f}%", "COD Conv %": "{:.1f}%", "Prepaid Conv %": "{:.1f}%",
    "COD Share %": "{:.1f}%", "Prepaid Share %": "{:.1f}%",
}


def style_overview(df, extra_fmt=None):
    """Apply colour-coding to an overview DataFrame."""
    fmt = {"Volume": "{:,.0f}", **PCT_FMT}
    if extra_fmt:
        fmt.update(extra_fmt)
    cols = set(df.columns)
    styler = df.style
    if "Breach %" in cols:
        styler = styler.map(_clr_breach, subset=["Breach %"])
    if "ZRTO %" in cols:
        styler = styler.map(_clr_zrto, subset=["ZRTO %"])
    high_cols = [c for c in ("FAC %", "Conv %", "COD Conv %", "Prepaid Conv %") if c in cols]
    if high_cols:
        styler = styler.map(_clr_high_good, subset=high_cols)
    if "Volume" in cols:
        styler = styler.map(_clr_vol, subset=["Volume"])
    active_fmt = {k: v for k, v in fmt.items() if k in cols}
    styler = styler.format(active_fmt)
    return styler


# ═════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════
data_path = "362c62a8adb9d17ecb5a6c9d33385822.csv"

with st.sidebar:
    st.markdown("## 📦 Seller × City Dashboard")
    st.divider()
    page = st.radio("Navigation", ["Upload Data", "Dashboard View"], index=1,
                     label_visibility="collapsed")

# ── Load data ────────────────────────────────────────────────────────────────
try:
    raw_df = load_data(data_path)
except FileNotFoundError:
    st.error(f"File not found: `{data_path}`. Update the path in the sidebar.")
    st.stop()

all_sellers = sorted(raw_df["seller_type"].cat.categories.tolist())
all_cities = sorted(raw_df["destination_city"].cat.categories.tolist())

# ═════════════════════════════════════════════════════════════════════════════
#  UPLOAD DATA PAGE
# ═════════════════════════════════════════════════════════════════════════════
if page == "Upload Data":
    st.markdown(
        '<div class="section-hdr">'
        '<span class="ico">📤</span>'
        '<div><div class="ttl">Upload & Append Data</div>'
        '<div class="sub">Upload a CSV file to append new records to the raw dataset</div></div></div>',
        unsafe_allow_html=True,
    )

    EXPECTED_COLS = [
        "reporting_date", "destination_city", "seller_type", "payment_type",
        "PHin", "conv_num", "zero_attempt_num", "fm_created", "fm_picked",
        "fm_d0_picked", "DHin", "D0_OFD", "First_attempt_delivered", "fac_deno",
        "total_delivered_attempts", "total_attempts", "rfr_num", "rfr_deno",
        "Breach_Num", "Breach_Den", "breach_plus1_num",
    ]

    with st.expander("📋 Expected columns & current data summary", expanded=False):
        st.code(", ".join(EXPECTED_COLS))
        ec1, ec2, ec3 = st.columns(3)
        ec1.metric("Current rows", f"{len(raw_df):,}")
        ec2.metric("Sellers", f"{len(all_sellers):,}")
        ec3.metric("Cities", f"{len(all_cities):,}")

    uploaded = st.file_uploader(
        "Choose a CSV file", type=["csv"], key="upload_csv",
    )

    if uploaded is not None:
        try:
            new_df = pd.read_csv(uploaded)
        except Exception as exc:
            st.error(f"Failed to read CSV: {exc}")
            st.stop()

        missing = sorted(set(EXPECTED_COLS) - set(new_df.columns))
        extra = sorted(set(new_df.columns) - set(EXPECTED_COLS))

        if missing:
            st.error(f"**Missing columns:** {', '.join(missing)}")
            st.info("Upload a file that contains all expected columns listed above.")
            st.stop()

        if extra:
            st.warning(f"Extra columns will be dropped: {', '.join(extra)}")

        new_df = new_df[EXPECTED_COLS]

        st.markdown(f"**Preview** — showing first 100 of **{len(new_df):,}** rows")
        st.dataframe(new_df.head(100), width="stretch", hide_index=True)

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Rows to append", f"{len(new_df):,}")
        mc2.metric("Current rows", f"{len(raw_df):,}")
        mc3.metric("Total after append", f"{len(raw_df) + len(new_df):,}")

        if st.button("✅ Append to raw data", type="primary", width="stretch"):
            try:
                new_df.to_csv(data_path, mode="a", header=False, index=False)
                st.cache_data.clear()
                st.success(
                    f"Successfully appended **{len(new_df):,}** rows to `{data_path}`. "
                    "Switch to the Dashboard page or hit Refresh to see updated data."
                )
                st.balloons()
            except Exception as exc:
                st.error(f"Failed to write to `{data_path}`: {exc}")
    else:
        st.info("Upload a CSV file with the expected columns to append data to the raw dataset.")

    st.divider()
    st.markdown(
        "<div style='text-align:center;color:#94A3B8;font-size:.7rem'>"
        "Seller × City Performance Dashboard · Upload Data</div>",
        unsafe_allow_html=True,
    )
    st.stop()

# ═════════════════════════════════════════════════════════════════════════════
#  DASHBOARD PAGE — use all data (no sidebar filters)
# ═════════════════════════════════════════════════════════════════════════════
filtered_df = raw_df

# ═════════════════════════════════════════════════════════════════════════════
#  PRE-COMPUTE AGGREGATIONS (these feed the overview tables)
# ═════════════════════════════════════════════════════════════════════════════
kpis = overall_kpis(filtered_df)
city_table = aggregate_by(filtered_df, "destination_city")
seller_table = aggregate_by(filtered_df, "seller_type")

date_strs = sorted(raw_df["reporting_date"].unique())
try:
    min_d = datetime.strptime(min(date_strs), "%Y%m%d").date()
    max_d = datetime.strptime(max(date_strs), "%Y%m%d").date()
except (ValueError, TypeError):
    min_d = max_d = datetime.now().date()

# ═════════════════════════════════════════════════════════════════════════════
#  KPI CARDS
# ═════════════════════════════════════════════════════════════════════════════
st.markdown(
    f"<div style='font-size:.8rem;color:#64748B;margin-bottom:8px'>"
    f"<b>{len(seller_table)}</b> sellers · <b>{len(city_table)}</b> cities · "
    f"{min_d} to {max_d}</div>",
    unsafe_allow_html=True,
)

kc = st.columns(5)
cards = [
    ("Total Volume", f"{kpis['Volume']:,}",       "Total shipments",  ""),
    ("Conv %",       f"{kpis['Conv %']:.1f}%",     "Conversion rate",  "green"),
    ("Breach %",     f"{kpis['Breach %']:.1f}%",   "SLA breach rate",  "red"),
    ("ZRTO %",       f"{kpis['ZRTO %']:.2f}%",    "Zero-attempt RTO", "orange"),
    ("FAC %",        f"{kpis['FAC %']:.1f}%",      "1st attempt conv", "purple"),
]
for col, (lbl, val, sub, cls) in zip(kc, cards):
    with col:
        st.markdown(
            f'<div class="kpi-card {cls}">'
            f'<div class="kpi-label">{lbl}</div>'
            f'<div class="kpi-value">{val}</div>'
            f'<div class="kpi-sub">{sub}</div></div>',
            unsafe_allow_html=True,
        )

st.markdown("")

# ═════════════════════════════════════════════════════════════════════════════
#  OVERVIEW TABLES — City & Seller tabs
# ═════════════════════════════════════════════════════════════════════════════
tab_city, tab_seller = st.tabs(["🏙  City Overview", "🏪  Seller Overview"])

DISPLAY_COLS = [
    "Volume", "Breach %", "FAC %", "ZRTO %", "Conv %",
    "COD Conv %", "Prepaid Conv %", "COD Share %", "Prepaid Share %",
]


def _display_table(agg_df, label_col, label_name, tab_key):
    """Render a styled overview table with search."""
    search = st.text_input(
        f"Search {label_name.lower()}",
        placeholder=f"🔍  Filter by {label_name.lower()} name …",
        label_visibility="collapsed",
        key=f"search_{tab_key}",
    )
    disp = agg_df.rename(columns={label_col: label_name, "PHin": "Volume"})
    show = [label_name] + [c for c in DISPLAY_COLS if c in disp.columns]
    disp = disp[show]
    if search:
        disp = disp[disp[label_name].astype(str).str.upper().str.contains(search.strip().upper())]
    st.dataframe(
        style_overview(disp),
        width="stretch",
        height=min(460, 38 + 35 * len(disp)),
        hide_index=True,
    )
    st.download_button(
        f"⬇ Download {label_name} table",
        disp.to_csv(index=False).encode(),
        file_name=f"{label_name.lower()}_performance.csv",
        mime="text/csv",
        key=f"dl_{tab_key}",
    )


with tab_city:
    st.markdown(
        '<div class="section-hdr">'
        '<span class="ico">🏙</span>'
        '<div><div class="ttl">City-wise Performance</div>'
        '<div class="sub">Aggregated metrics per destination city</div></div></div>',
        unsafe_allow_html=True,
    )
    _display_table(city_table, "destination_city", "City", "city")

with tab_seller:
    st.markdown(
        '<div class="section-hdr">'
        '<span class="ico">🏪</span>'
        '<div><div class="ttl">Seller-wise Performance</div>'
        '<div class="sub">Aggregated metrics per seller type</div></div></div>',
        unsafe_allow_html=True,
    )
    _display_table(seller_table, "seller_type", "Seller", "seller")

# ═════════════════════════════════════════════════════════════════════════════
#  SELLER DRILL-DOWN — rendered inside @st.fragment so that changing the
#  seller selectbox does NOT re-run the entire page (only this block).
# ═════════════════════════════════════════════════════════════════════════════
st.divider()
st.markdown(
    '<div class="drill-hdr">'
    '<div class="ttl">🔎  Seller Drill-Down</div>'
    '<div class="sub">Select a seller to view detailed city-wise and date-wise performance. '
    'This section re-renders independently for a lag-free experience.</div></div>',
    unsafe_allow_html=True,
)


@st.fragment
def seller_drilldown():
    # ── Step 1: Date range picker ─────────────────────────────────────────
    st.markdown(
        "<div style='font-size:.8rem;color:#64748B;margin-bottom:4px'>"
        "<b>Step 1</b> — Choose a date range for the drill-down</div>",
        unsafe_allow_html=True,
    )
    avail_dates = sorted(filtered_df["reporting_date"].unique())
    try:
        drill_min = datetime.strptime(min(avail_dates), "%Y%m%d").date()
        drill_max = datetime.strptime(max(avail_dates), "%Y%m%d").date()
    except (ValueError, TypeError):
        drill_min = drill_max = datetime.now().date()

    dc1, dc2, dc3 = st.columns([1, 1, 2])
    with dc1:
        drill_start = st.date_input(
            "From", value=drill_min, min_value=drill_min, max_value=drill_max,
            key="drill_from",
        )
    with dc2:
        drill_end = st.date_input(
            "To", value=drill_max, min_value=drill_min, max_value=drill_max,
            key="drill_to",
        )
    if drill_start > drill_end:
        drill_start, drill_end = drill_end, drill_start

    ds = drill_start.strftime("%Y%m%d")
    de = drill_end.strftime("%Y%m%d")
    date_scoped = filtered_df[
        (filtered_df["reporting_date"] >= ds)
        & (filtered_df["reporting_date"] <= de)
    ]

    # ── Step 2: Seller selector ───────────────────────────────────────────
    sellers_in_range = sorted(date_scoped["seller_type"].unique())

    with dc3:
        st.markdown(
            "<div style='font-size:.8rem;color:#64748B;margin-bottom:4px'>"
            "<b>Step 2</b> — Pick a seller</div>",
            unsafe_allow_html=True,
        )
        chosen = st.selectbox(
            "Choose seller",
            options=["— Select a seller —"] + sellers_in_range,
            key="drill_sel",
            label_visibility="collapsed",
        )

    if chosen == "— Select a seller —":
        return

    sdf = date_scoped[date_scoped["seller_type"] == chosen]
    if sdf.empty:
        st.warning("No data for the selected seller in this date range.")
        return

    # ── Seller KPI row ────────────────────────────────────────────────────
    sk = overall_kpis(sdf)
    sc = st.columns(5)
    s_cards = [
        ("Volume",   f"{sk['Volume']:,}",       ""),
        ("Conv %",   f"{sk['Conv %']:.1f}%",     "green"),
        ("Breach %", f"{sk['Breach %']:.1f}%",   "red"),
        ("ZRTO %",   f"{sk['ZRTO %']:.2f}%",    "orange"),
        ("FAC %",    f"{sk['FAC %']:.1f}%",      "purple"),
    ]
    for col, (lbl, val, cls) in zip(sc, s_cards):
        with col:
            st.markdown(
                f'<div class="kpi-card {cls}">'
                f'<div class="kpi-label">{lbl}</div>'
                f'<div class="kpi-value">{val}</div></div>',
                unsafe_allow_html=True,
            )

    # ── City-wise breakdown ─────────────────────────────────────────────
    city_detail = aggregate_by(sdf, "destination_city")
    city_detail = city_detail[city_detail["PHin"] > 0]
    disp_city = city_detail.rename(columns={"destination_city": "City", "PHin": "Volume"})
    show_cols = ["City"] + [c for c in DISPLAY_COLS if c in disp_city.columns]
    disp_city = disp_city[show_cols]

    city_search = st.text_input(
        "Search city", placeholder="🔍  Filter cities …",
        label_visibility="collapsed", key="drill_city_search",
    )
    if city_search:
        disp_city = disp_city[
            disp_city["City"].astype(str).str.upper().str.contains(city_search.strip().upper())
        ]

    st.dataframe(
        style_overview(disp_city),
        width="stretch",
        height=min(420, 38 + 35 * len(disp_city)),
        hide_index=True,
    )
    st.download_button(
        "⬇ Download city breakdown",
        disp_city.to_csv(index=False).encode(),
        file_name=f"{chosen}_city_breakdown.csv",
        mime="text/csv",
        key="dl_drill_city",
    )

    # ── City drill-down: day-wise trend for seller × city ─────────────
    st.divider()
    cities_available = sorted(city_detail["destination_city"].unique()) if "destination_city" in city_detail.columns else sorted(disp_city["City"].unique())
    st.markdown(
        "<div style='font-size:.8rem;color:#64748B;margin-bottom:4px'>"
        "<b>Step 3</b> — Select a city to view day-wise trend for this seller</div>",
        unsafe_allow_html=True,
    )
    chosen_city = st.selectbox(
        "Choose city",
        options=["— Select a city —"] + cities_available,
        key="drill_city_sel",
        label_visibility="collapsed",
    )

    if chosen_city != "— Select a city —":
        city_df = sdf[sdf["destination_city"] == chosen_city]
        if city_df.empty:
            st.warning(f"No data for **{chosen}** in **{chosen_city}**.")
        else:
            day_agg = aggregate_by(city_df, "reporting_date", with_payment_split=False)
            day_agg = day_agg.sort_values("reporting_date")

            def _fmt_d(s):
                try:
                    return datetime.strptime(str(s), "%Y%m%d").strftime("%d %b")
                except ValueError:
                    return str(s)

            day_agg["Date"] = day_agg["reporting_date"].apply(_fmt_d)
            disp_day = day_agg.rename(columns={"PHin": "Volume"})
            day_cols = ["Date", "Volume", "Breach %", "FAC %", "ZRTO %", "Conv %"]
            disp_day = disp_day[[c for c in day_cols if c in disp_day.columns]]

            st.markdown(
                f"<div style='font-size:.82rem;color:#64748B;margin-bottom:6px'>"
                f"Day-wise trend for <b>{chosen}</b> in <b>{chosen_city}</b> "
                f"({drill_start} → {drill_end})</div>",
                unsafe_allow_html=True,
            )
            st.dataframe(
                style_overview(disp_day),
                width="stretch",
                height=min(400, 38 + 35 * len(disp_day)),
                hide_index=True,
            )
            st.download_button(
                "⬇ Download day-wise city trend",
                disp_day.to_csv(index=False).encode(),
                file_name=f"{chosen}_{chosen_city}_daily.csv",
                mime="text/csv",
                key="dl_drill_city_day",
            )


seller_drilldown()

# ═════════════════════════════════════════════════════════════════════════════
#  FOOTER
# ═════════════════════════════════════════════════════════════════════════════
st.divider()
st.markdown(
    "<div style='text-align:center;color:#94A3B8;font-size:.7rem'>"
    "Seller × City Performance Dashboard · Data refreshed on load</div>",
    unsafe_allow_html=True,
)
