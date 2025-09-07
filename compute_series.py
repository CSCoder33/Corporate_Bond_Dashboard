"""
Merge raw FRED CSVs into a clean daily DataFrame and compute derived series.

Usage:
    python compute_series.py

Outputs:
    data/processed/series.csv
    data/processed/series.parquet
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class SeriesDef:
    sid: str
    col: str


SERIES: List[SeriesDef] = [
    SeriesDef("BAMLC0A0CM", "IG_OAS"),
    SeriesDef("BAMLH0A0HYM2", "HY_OAS"),
    SeriesDef("BAA", "BAA_YIELD"),
    SeriesDef("DGS10", "DGS10"),
    SeriesDef("DGS2", "DGS2"),
    SeriesDef("DGS5", "DGS5"),
    SeriesDef("USREC", "USREC"),  # optional, if available
    # Optional OAS quality buckets (true OAS, not effective yield)
    SeriesDef("BAMLC0A1CAAA", "AAA_OAS"),
    SeriesDef("BAMLC0A2CAA", "AA_OAS"),
    SeriesDef("BAMLC0A3CA", "A_OAS"),
    SeriesDef("BAMLC0A4CBBB", "BBB_OAS"),
]


def latest_csv_for(series_id: str) -> Optional[Path]:
    series_dir = RAW_DIR / series_id
    if not series_dir.exists():
        return None
    # Expect files like <sid>_YYYYMMDD.csv
    files = sorted(series_dir.glob(f"{series_id}_*.csv"))
    if not files:
        # Fall back to latest.csv if present
        if (series_dir / "latest.csv").exists():
            return series_dir / "latest.csv"
        return None
    return files[-1]


def load_series(series: SeriesDef) -> Optional[pd.DataFrame]:
    path = latest_csv_for(series.sid)
    if path is None:
        return None
    df = pd.read_csv(path)
    # FRED CSV columns are typically observation_date, <sid>
    date_col_candidates = ["DATE", "date", "observation_date"]
    date_col = next((c for c in date_col_candidates if c in df.columns), df.columns[0])
    value_col_candidates = [c for c in df.columns if c != date_col]
    if not value_col_candidates:
        return None
    value_col = value_col_candidates[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.rename(columns={date_col: "DATE", value_col: series.col}).set_index("DATE").sort_index()
    # Convert to numeric, coerce '.' to NaN, etc.
    df[series.col] = pd.to_numeric(df[series.col], errors="coerce")
    return df[[series.col]]


def merge_all() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for s in SERIES:
        df = load_series(s)
        if df is None:
            continue
        frames.append(df)
    if not frames:
        raise RuntimeError("No raw series found. Run fetch_fred.py first.")

    merged = pd.concat(frames, axis=1).sort_index()
    # Forward-fill across business days to align daily panel
    merged = merged.ffill()
    return merged


def add_derived(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # HY–IG OAS spread
    if {"HY_OAS", "IG_OAS"}.issubset(out.columns):
        out["HY_IG_SPREAD"] = out["HY_OAS"] - out["IG_OAS"]
    # Baa – 10y Treasury spread
    if {"BAA_YIELD", "DGS10"}.issubset(out.columns):
        out["BAA_10Y_SPREAD"] = out["BAA_YIELD"] - out["DGS10"]
    # 2s10s curve
    if {"DGS10", "DGS2"}.issubset(out.columns):
        out["TWOS_TENS"] = out["DGS10"] - out["DGS2"]

    # Optional: 90-day rolling z-scores for selected columns
    cols_for_z = [c for c in [
        "IG_OAS", "HY_OAS", "HY_IG_SPREAD", "BAA_10Y_SPREAD", "TWOS_TENS"
    ] if c in out.columns]
    window = 90
    for c in cols_for_z:
        rolling = out[c].rolling(window, min_periods=window//3)
        z = (out[c] - rolling.mean()) / rolling.std(ddof=0)
        out[f"{c}_Z{window}"] = z
    return out


def save_outputs(df: pd.DataFrame) -> None:
    csv_path = PROC_DIR / "series.csv"
    parquet_path = PROC_DIR / "series.parquet"
    df.to_csv(csv_path, index=True)
    try:
        df.to_parquet(parquet_path, index=True)
    except Exception:
        # Parquet may fail if pyarrow is unavailable
        pass
    print(f"Saved processed panel -> {csv_path}")


def main() -> None:
    merged = merge_all()
    enriched = add_derived(merged)
    save_outputs(enriched)


if __name__ == "__main__":
    main()
