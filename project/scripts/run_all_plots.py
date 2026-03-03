"""Run all plot scripts. Skips AQS plots if credentials are missing."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
import argparse

ROOT = Path(__file__).resolve().parents[1]
PLOTS_DIR = ROOT / "scripts" / "plots"

SCRIPTS = [
    "rq1/rq1_histogram_ces_change.py",
    "rq1/rq1_map_ces_change_points.py",
    "rq1/rq1_top_bottom_tables.py",
    "rq1/rq1_top_changes_slopegraph.py",
    "rq2/rq2_county_slopegraph.py",
    "rq2/rq2_county_heatmap.py",
    "rq2/rq2_county_distributions_boxplots.py",
    "rq2/rq2_county_consistency_summary.py",
    "rq2/rq2_county_trends_linechart.py",
    "rq5/rq5_high_burden_counts_2021.py",
    "rq5/rq5_high_burden_counts_multi.py",
    "rq5/rq5_high_burden_map_share.py",
    "ancillary/ancillary_scatter_traffic_vs_asthma.py",
    "ancillary/ancillary_hidden_hotspots_indicator.py",
    "ancillary/ancillary_pollution_vs_poverty_education.py",
    "ancillary/ancillary_linguistic_isolation_vs_metrics.py",
]


def run_script(path: Path) -> None:
    print(f"[RUN] {path.name}")
    subprocess.run([sys.executable, str(path)], check=True)

def load_env_file() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value and key not in os.environ:
            os.environ[key] = value

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--aqs",
        action="store_true",
        help="Include AQS API plots (requires AQS_USER/AQS_PW).",
    )
    args = parser.parse_args()

    # Ensure standardized data exists
    standardize = ROOT / "scripts" / "standardize_ces.py"
    if not (ROOT / "data" / "ces_standardized.csv").exists():
        print("[RUN] standardize_ces.py")
        subprocess.run([sys.executable, str(standardize)], check=True)

    for name in SCRIPTS:
        run_script(PLOTS_DIR / name)

    # AQS plots require credentials; default is skip unless --aqs is passed
    load_env_file()
    if args.aqs:
        if os.getenv("AQS_USER") and os.getenv("AQS_PW"):
            run_script(PLOTS_DIR / "rq3/rq3_aqs_vs_ces_scatter.py")
        else:
            print("[SKIP] qx_aqs_vs_ces_scatter.py (missing AQS_USER/AQS_PW)")
    else:
        print("[SKIP] qx_aqs_vs_ces_scatter.py (default skip; pass --aqs to include)")


if __name__ == "__main__":
    main()
