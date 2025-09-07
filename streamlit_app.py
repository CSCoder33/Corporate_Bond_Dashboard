from __future__ import annotations

import io
from datetime import timedelta
from pathlib import Path

import pandas as pd
import streamlit as st


PROC_CSV = Path("data/processed/series.csv")


@st.cache_data(show_spinner=False)
def load_processed() -> pd.DataFrame | None:
    if PROC_CSV.exists():
        df = pd.read_csv(PROC_CSV, parse_dates=["DATE"], index_col="DATE")
        return df
    return None


def ensure_data() -> pd.DataFrame:
    df = load_processed()
    if df is not None and not df.empty:
        return df
    # Fallback: attempt to compute on the fly if raw data present
    try:
        from compute_series import merge_all, add_derived
        merged = merge_all()
        enriched = add_derived(merged)
        return enriched
    except Exception as e:
        st.error("No processed data found. Click 'Update Data' to fetch from FRED, or run the pipeline locally.")
        raise


def fetch_and_process() -> pd.DataFrame | None:
    try:
        from fetch_fred import SERIES_MAP, fetch_series
        from compute_series import merge_all, add_derived
    except Exception as e:
        st.error(f"Unable to import pipeline modules: {e}")
        return None

    with st.spinner("Fetching FRED series..."):
        ok = 0
        for sid in SERIES_MAP.keys():
            try:
                fetch_series(sid)
                ok += 1
            except Exception as e:
                st.warning(f"Fetch failed for {sid}: {e}")
        st.success(f"Fetched {ok} series")
    with st.spinner("Processing and computing derived series..."):
        df = add_derived(merge_all())
    # cache to disk for reuse
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    df.to_csv(PROC_CSV)
    return df


def subset_by_range(df: pd.DataFrame, range_label: str) -> pd.DataFrame:
    end = df.index.max()
    years = {"1Y": 1, "3Y": 3, "5Y": 5, "20Y": 20}
    start = end - pd.DateOffset(years=years[range_label])
    return df.loc[df.index >= start]


def convert_units(df: pd.DataFrame, unit: str, cols: list[str] | None = None) -> pd.DataFrame:
    """Convert selected columns between Percent (default) and bps.
    FRED OAS/yields are in percent (e.g., 0.60 -> 60 bps)."""
    if unit == "Percent":
        return df
    if cols is None:
        cols = [
            c for c in df.columns if any(k in c for k in ["OAS", "SPREAD", "DGS", "BAA_YIELD"]) and not c.endswith("USREC")
        ]
    scaled = df.copy()
    for c in cols:
        if c in scaled.columns:
            scaled[c] = scaled[c] * 100.0
    return scaled


def build_current_table(df: pd.DataFrame) -> pd.DataFrame:
    end = df.index.max()
    sub1 = df.loc[df.index >= end - pd.DateOffset(years=1)]
    metrics = [
        ("HY_OAS", "HY OAS"),
        ("IG_OAS", "IG OAS"),
        ("HY_IG_SPREAD", "HY – IG"),
        ("BAA_10Y_SPREAD", "Baa – 10y"),
        ("TWOS_TENS", "2s10s"),
        ("AAA_OAS", "AAA OAS"),
        ("AA_OAS", "AA OAS"),
        ("A_OAS", "A OAS"),
        ("BBB_OAS", "BBB OAS"),
    ]
    rows = []
    for col, label in metrics:
        if col not in df.columns:
            continue
        cur = df[col].iloc[-1]
        yr = sub1[col]
        rows.append({
            "Metric": label,
            "Current": round(cur, 2) if pd.notna(cur) else None,
            "1y High": round(yr.max(), 2) if pd.notna(yr.max()) else None,
            "1y Low": round(yr.min(), 2) if pd.notna(yr.min()) else None,
        })
    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(page_title="Corporate Bond Spread Dashboard", layout="wide")
    st.title("Corporate Bond Spread Dashboard")

    col_l, col_r = st.columns([1, 1])
    with col_l:
        if st.button("Update Data (Fetch from FRED)"):
            df = fetch_and_process()
            if df is not None:
                # Streamlit >=1.25 uses st.rerun; keep fallback for older versions
                try:
                    st.rerun()
                except AttributeError:
                    st.experimental_rerun()
    with col_r:
        df0 = load_processed()
        if df0 is not None and not df0.empty:
            st.caption(f"Last updated data: {df0.index.max().date().isoformat()}")

    df = ensure_data()

    with st.sidebar:
        st.header("View Options")
        range_label = st.selectbox("Time window", ["1Y", "3Y", "5Y", "20Y"], index=2)
        unit = st.selectbox("Units", ["Percent", "bps"], index=1)
        show_buckets = st.multiselect(
            "Quality buckets",
            ["AAA_OAS", "AA_OAS", "A_OAS", "BBB_OAS"],
            default=[],  # no buckets selected by default
        )
        shade_recessions = "USREC" in df.columns and st.checkbox("Shade recessions (USREC)", value=True)
        include_ig_hy_in_ladder = st.checkbox("Include IG/HY in ladder", value=True)

    sub = subset_by_range(df, range_label)
    sub_units = convert_units(sub, unit)

    # Main chart: IG vs HY, optional buckets
    st.subheader("IG vs HY OAS")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4))
    if "IG_OAS" in sub_units.columns:
        ax.plot(sub_units.index, sub_units["IG_OAS"], label="IG OAS", lw=2, color="red")
    if "HY_OAS" in sub_units.columns:
        ax.plot(sub_units.index, sub_units["HY_OAS"], label="HY OAS", lw=2, color="blue")
    colors = {
        "AAA_OAS": "green",
        "AA_OAS": "orange",
        "A_OAS": "purple",
        "BBB_OAS": "black",
    }
    for col in show_buckets:
        if col in sub_units.columns:
            ax.plot(sub_units.index, sub_units[col], label=col.replace("_OAS", " OAS"), lw=1.5, alpha=0.8, color=colors.get(col))
    if shade_recessions:
        rec = sub.get("USREC")
        if rec is not None:
            rec = (rec > 0.5).astype(int)
            in_rec = False
            start = None
            for dt, val in rec.items():
                if val == 1 and not in_rec:
                    in_rec = True
                    start = dt
                elif val == 0 and in_rec:
                    in_rec = False
                    ax.axvspan(start, dt, color="gray", alpha=0.2, lw=0)
            if in_rec and start is not None:
                ax.axvspan(start, rec.index[-1], color="gray", alpha=0.2, lw=0)
    ax.set_ylabel("Spread ({} )".format("bps" if unit == "bps" else "%"))
    ax.set_xlabel("")
    ax.legend(ncol=3)
    ax.grid(True, alpha=0.3)
    st.pyplot(fig, use_container_width=True)

    # Secondary chart: HY – IG
    if {"HY_IG_SPREAD"}.issubset(sub_units.columns):
        st.subheader("HY – IG Spread")
        fig2, ax2 = plt.subplots(figsize=(10, 3))
        ax2.plot(sub_units.index, sub_units["HY_IG_SPREAD"], color="#d62728", lw=2)
        ax2.set_ylabel("Spread ({} )".format("bps" if unit == "bps" else "pp"))
        ax2.grid(True, alpha=0.3)
        st.pyplot(fig2, use_container_width=True)

    # Helper: last common date and values across quality buckets
    def last_common_values(frame: pd.DataFrame, cols: list[str]):
        dfc = frame[cols].dropna()
        if dfc.empty:
            return None, None
        last_dt = dfc.index.max()
        return last_dt, dfc.loc[last_dt]

    # Sanity check: AAA <= AA <= A <= BBB ordering at last common date
    order_cols = ["AAA_OAS", "AA_OAS", "A_OAS", "BBB_OAS"]
    present_cols = [c for c in order_cols if c in sub.columns]
    if len(present_cols) >= 2:
        last_dt, last_vals = last_common_values(sub, present_cols)
        if last_vals is not None:
            # Allow tiny tolerance for near-ties
            tol = 1e-6
            is_sorted = all(last_vals.values[i] <= last_vals.values[i+1] + tol for i in range(len(last_vals)-1))
            if not is_sorted:
                disp_vals = last_vals.to_dict()
                st.warning(
                    f"Quality OAS not in typical order at {last_dt.date()}: "
                    + ", ".join(f"{k.replace('_OAS','')}: {v:.2f}{'bps' if unit=='bps' else '%'}" for k, v in disp_vals.items())
                    + ". Try Update Data and ensure consistent units.")

    # Show latest common values for selected buckets in the sidebar for clarity
    if any(c in sub.columns for c in ["AAA_OAS", "AA_OAS", "A_OAS", "BBB_OAS"]):
        show_cols = [c for c in ["AAA_OAS", "AA_OAS", "A_OAS", "BBB_OAS"] if c in sub.columns]
        last_dt_u, last_vals_u = last_common_values(sub_units, show_cols)
        if last_vals_u is not None:
            st.caption(
                "Latest quality OAS (" + ("bps" if unit=="bps" else "%") + f") as of {last_dt_u.date()}: "
                + ", ".join(f"{c.replace('_OAS','')}: {last_vals_u[c]:.2f}" for c in show_cols)
            )

    # Quality ladder chart
    ladder_cols = [c for c in ["AAA_OAS", "AA_OAS", "A_OAS", "BBB_OAS"] if c in sub_units.columns]
    if ladder_cols:
        st.subheader("Quality Ladder: AAA → BBB")
        fig3, ax3 = plt.subplots(figsize=(10, 4))
        ladder_colors = {"AAA_OAS": "green", "AA_OAS": "orange", "A_OAS": "purple", "BBB_OAS": "black"}
        for col in ladder_cols:
            ax3.plot(sub_units.index, sub_units[col], label=col.replace("_OAS", " OAS"), lw=1.8, color=ladder_colors.get(col))
        if include_ig_hy_in_ladder:
            if "IG_OAS" in sub_units.columns:
                ax3.plot(sub_units.index, sub_units["IG_OAS"], label="IG OAS", lw=1.2, color="red", alpha=0.7)
            if "HY_OAS" in sub_units.columns:
                ax3.plot(sub_units.index, sub_units["HY_OAS"], label="HY OAS", lw=1.2, color="blue", alpha=0.7)
        if shade_recessions:
            rec = sub.get("USREC")
            if rec is not None:
                rec = (rec > 0.5).astype(int)
                in_rec = False
                start = None
                for dt, val in rec.items():
                    if val == 1 and not in_rec:
                        in_rec = True
                        start = dt
                    elif val == 0 and in_rec:
                        in_rec = False
                        ax3.axvspan(start, dt, color="gray", alpha=0.2, lw=0)
                if in_rec and start is not None:
                    ax3.axvspan(start, rec.index[-1], color="gray", alpha=0.2, lw=0)
        ax3.set_ylabel("Spread ({} )".format("bps" if unit == "bps" else "%"))
        ax3.set_xlabel("")
        ax3.legend(ncol=3)
        ax3.grid(True, alpha=0.3)
        st.pyplot(fig3, use_container_width=True)

    # Current table
    st.subheader("Current Spreads (with 1y High/Low)")
    table = build_current_table(df)
    # Convert table units if needed
    if unit == "bps":
        for col in ["Current", "1y High", "1y Low"]:
            if col in table.columns:
                table[col] = table[col] * 100.0
    st.dataframe(table, use_container_width=True)


if __name__ == "__main__":
    main()
