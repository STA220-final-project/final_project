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
    "rq1/rq1_map_ces_change_tracts.py",
    "rq1/rq1_top_bottom_tables.py",
    "rq1/rq1_top_changes_slopegraph.py",
    "rq1/rq1_top_changes_slopegraph_legend_inset.py",
    "rq2/rq2_county_slopegraph.py",
    "rq2/rq2_county_heatmap.py",
    "rq2/rq2_county_heatmap_topbottom.py",
    "rq2/rq2_county_distributions_boxplots.py",
    "rq2/rq2_county_distributions_boxplots_combined.py",
    "rq2/rq2_county_consistency_summary.py",
    "rq2/rq2_county_trends_linechart.py",
    "rq3/rq3_aqs_vs_ces_scatter.py",
    "rq5/rq5_high_burden_counts_2021.py",
    "rq5/rq5_high_burden_counts_multi.py",
    "rq5/rq5_high_burden_map_share.py",
    "rq5/rq5_high_burden_bivariate_map.py",
    "rq5/rq5_high_burden_stacked_bars.py",
    "rq4/rq4_populous_counties_aqs_comparison.py",
    "ancillary/ancillary_scatter_traffic_vs_asthma.py",
    "ancillary/ancillary_hidden_hotspots_indicator.py",
    "ancillary/ancillary_pollution_vs_poverty_education.py",
    "ancillary/ancillary_linguistic_isolation_vs_metrics.py",
    "ancillary/ancillary_lmer_models.py",
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
        help="Include AQS plots (uses cached CSVs).",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch AQS data before plotting (requires AQS_USER/AQS_PW).",
    )
    args = parser.parse_args()

    # Ensure standardized data exists
    standardize = ROOT / "scripts" / "standardize_ces.py"
    if not (ROOT / "data" / "ces_standardized.csv").exists():
        print("[RUN] standardize_ces.py")
        subprocess.run([sys.executable, str(standardize)], check=True)

    # Only fetch when explicitly requested
    if args.fetch:
        if os.getenv("AQS_USER") and os.getenv("AQS_PW"):
            fetch_script = ROOT / "scripts" / "fetch" / "fetch_aqs_all.py"
            run_script(fetch_script)
        else:
            print("[SKIP] AQS fetch (missing AQS_USER/AQS_PW)")

    for name in SCRIPTS:
        # AQS-dependent plots; skip unless --aqs is set
        if (name.startswith("rq3/") or name.startswith("rq4/")) and not args.aqs:
            continue
        run_script(PLOTS_DIR / name)

    # AQS plots require --aqs
    load_env_file()
    if not args.aqs:
        print("[SKIP] AQS plots (pass --aqs to include)")
    elif args.fetch and not (os.getenv("AQS_USER") and os.getenv("AQS_PW")):
        print("[SKIP] AQS fetch (missing AQS_USER/AQS_PW)")


if __name__ == "__main__":
    main()
