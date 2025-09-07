"""
Download CSVs from FRED for required series and cache locally.

Usage:
    python fetch_fred.py [--series ALL|ID,ID,...]

Saves:
    data/raw/<SERIES_ID>/<SERIES_ID>_YYYYMMDD.csv
    data/raw/<SERIES_ID>/latest.csv (copy of most recent)
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests


FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"


# Core series per spec
SERIES_MAP: Dict[str, str] = {
    "BAMLC0A0CM": "IG_OAS",     # ICE BofA U.S. Corporate OAS (Investment Grade proxy)
    "BAMLH0A0HYM2": "HY_OAS",   # ICE BofA U.S. High Yield OAS
    "BAA": "BAA_YIELD",         # Moody's Baa Corporate Yield
    "DGS10": "DGS10",           # 10-year Treasury
    "DGS2": "DGS2",             # 2-year Treasury
    "DGS5": "DGS5",             # 5-year Treasury
    # Optional: U.S. recession indicator for shading
    "USREC": "USREC",
    # Optional: OAS quality buckets (true OAS, not effective yield)
    "BAMLC0A1CAAA": "AAA_OAS",  # ICE BofA AAA US Corporate OAS
    "BAMLC0A2CAA": "AA_OAS",    # ICE BofA AA US Corporate OAS
    "BAMLC0A3CA": "A_OAS",      # ICE BofA A US Corporate OAS
    "BAMLC0A4CBBB": "BBB_OAS",  # ICE BofA BBB US Corporate OAS
}


def ensure_dirs(series_id: str) -> Path:
    base = Path("data/raw") / series_id
    base.mkdir(parents=True, exist_ok=True)
    return base


def fetch_series(series_id: str) -> Path:
    out_dir = ensure_dirs(series_id)
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_path = out_dir / f"{series_id}_{date_str}.csv"

    url = FRED_CSV_URL.format(series_id=series_id)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    out_path.write_bytes(resp.content)

    # Maintain a latest.csv convenience copy
    latest = out_dir / "latest.csv"
    latest.write_bytes(resp.content)
    return out_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch FRED CSV series and cache locally")
    p.add_argument(
        "--series",
        default="ALL",
        help="Comma-separated series IDs or 'ALL'",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.series == "ALL":
        series_to_fetch: List[str] = list(SERIES_MAP.keys())
    else:
        requested = [s.strip() for s in args.series.split(",") if s.strip()]
        # Validate against allowed or allow arbitrary?
        # Allow arbitrary but still save under its ID folder.
        series_to_fetch = requested

    downloaded: List[Path] = []
    for sid in series_to_fetch:
        try:
            path = fetch_series(sid)
            downloaded.append(path)
            print(f"Fetched {sid} -> {path}")
        except Exception as e:
            print(f"Failed to fetch {sid}: {e}")

    if downloaded:
        print(f"Downloaded {len(downloaded)} file(s)")
    else:
        print("No files downloaded")


if __name__ == "__main__":
    main()
