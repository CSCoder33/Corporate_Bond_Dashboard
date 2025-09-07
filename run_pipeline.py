"""
Run the full pipeline: fetch -> process -> visualize -> update README.

Usage:
    python run_pipeline.py
"""
from __future__ import annotations

import subprocess
import sys


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}")
    proc = subprocess.run([sys.executable, *cmd], check=True)


def main() -> None:
    run(["fetch_fred.py"])  # ALL by default
    run(["compute_series.py"]) 
    run(["charts.py"]) 
    run(["update_readme.py"]) 


if __name__ == "__main__":
    main()

