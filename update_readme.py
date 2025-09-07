"""
Update README 'Last updated' stamp to today's UTC date.

Usage:
    python update_readme.py
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


README = Path("README.md")
MARKER_PREFIX = "Last updated: "


def main() -> None:
    if not README.exists():
        return
    lines = README.read_text().splitlines()
    new_lines = []
    updated = False
    today = datetime.now(timezone.utc).date().isoformat()
    for line in lines:
        if line.startswith(MARKER_PREFIX):
            new_lines.append(f"{MARKER_PREFIX}{today}")
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f"{MARKER_PREFIX}{today}")
    README.write_text("\n".join(new_lines) + "\n")
    print(f"README updated to {today}")


if __name__ == "__main__":
    main()

