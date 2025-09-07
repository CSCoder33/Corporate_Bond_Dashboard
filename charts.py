"""
Generate charts and current spread table from processed data.

Usage:
    python charts.py

Outputs:
    reports/figures/ig_hy_oas_5y.png
    reports/figures/current_spreads.png
    reports/figures/current_spreads.csv (numeric table)
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import List, Tuple

import matplotlib
# Use a non-interactive backend for headless environments
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


PROC = Path("data/processed/series.csv")
FIG_DIR = Path("reports/figures")
FIG_DIR.mkdir(parents=True, exist_ok=True)


def load() -> pd.DataFrame:
    if not PROC.exists():
        raise FileNotFoundError("data/processed/series.csv not found. Run compute_series.py first.")
    df = pd.read_csv(PROC, parse_dates=["DATE"], index_col="DATE")
    return df


def recession_spans(df: pd.DataFrame) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    spans: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    if "USREC" not in df.columns:
        return spans
    rec = df["USREC"].dropna()
    rec = (rec > 0.5).astype(int)
    in_rec = False
    start = None
    for dt, val in rec.items():
        if val == 1 and not in_rec:
            in_rec = True
            start = dt
        elif val == 0 and in_rec:
            in_rec = False
            spans.append((start, dt))
    if in_rec and start is not None:
        spans.append((start, rec.index[-1]))
    return spans


def plot_oas_window(df: pd.DataFrame, years: int, out_name: str, title: str) -> Path:
    end = df.index.max()
    start = end - pd.DateOffset(years=years)
    sub = df.loc[df.index >= start]

    plt.figure(figsize=(10, 5.5))
    sns.set_style("whitegrid")

    if "IG_OAS" in sub.columns:
        plt.plot(sub.index, sub["IG_OAS"], label="IG OAS", color="#1f77b4", linewidth=2)
    if "HY_OAS" in sub.columns:
        plt.plot(sub.index, sub["HY_OAS"], label="HY OAS", color="#d62728", linewidth=2)

    # Recession shading if available
    for s, e in recession_spans(sub):
        plt.axvspan(s, e, color="gray", alpha=0.2, linewidth=0)

    # Removed dotted 1-year high/low markers for cleaner presentation

    plt.title(title)
    plt.ylabel("Spread (%)")
    plt.xlabel("")
    plt.legend()
    plt.tight_layout()

    out = FIG_DIR / out_name
    plt.savefig(out, dpi=150)
    plt.close()
    return out

def plot_oas_5y(df: pd.DataFrame) -> Path:
    return plot_oas_window(df, 5, "ig_hy_oas_5y.png", "IG vs HY OAS (last 5 years)")


def build_current_table(df: pd.DataFrame) -> pd.DataFrame:
    end = df.index.max()
    one_year_ago = end - pd.DateOffset(years=1)
    sub = df.loc[df.index >= one_year_ago]

    metrics = [
        ("HY_OAS", "HY OAS"),
        ("IG_OAS", "IG OAS"),
        ("HY_IG_SPREAD", "HY – IG"),
        ("BAA_10Y_SPREAD", "Baa – 10y"),
        ("TWOS_TENS", "2s10s"),
    ]

    rows = []
    for col, label in metrics:
        if col not in df.columns:
            continue
        cur = df[col].iloc[-1]
        yr = sub[col]
        hi = yr.max()
        lo = yr.min()
        rows.append({
            "Metric": label,
            "Current": round(cur, 2) if pd.notna(cur) else None,
            "1y High": round(hi, 2) if pd.notna(hi) else None,
            "1y Low": round(lo, 2) if pd.notna(lo) else None,
        })
    table = pd.DataFrame(rows)
    return table


def render_table_image(table: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(7, 1 + 0.45 * max(1, len(table))))
    ax.axis("off")
    tbl = ax.table(
        cellText=table.values,
        colLabels=table.columns,
        cellLoc="center",
        colLoc="center",
        loc="center",
    )
    tbl.scale(1, 1.2)
    for (row, col), cell in tbl.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
    fig.tight_layout()
    out = FIG_DIR / "current_spreads.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out


def main() -> None:
    df = load()
    chart_path = plot_oas_5y(df)
    chart_20y = plot_oas_window(df, 20, "ig_hy_oas_20y.png", "IG vs HY OAS (last 20 years)")
    table = build_current_table(df)
    csv_path = FIG_DIR / "current_spreads.csv"
    table.to_csv(csv_path, index=False)
    table_img = render_table_image(table)
    print(f"Saved chart -> {chart_path}")
    print(f"Saved chart -> {chart_20y}")
    print(f"Saved table -> {table_img}\nCSV -> {csv_path}")


if __name__ == "__main__":
    main()
