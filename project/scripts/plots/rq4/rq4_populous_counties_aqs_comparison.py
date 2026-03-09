"""RQ4: Compare AQS PM2.5 and Ozone in the 10 most populous counties.

Reads pre-fetched data from project/data/rq4_aqs_populous.csv.
Use scripts/fetch/rq4_fetch_aqs_populous.py to download.
"""

from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import ensure_out_dir

PARAM_PM25 = "88101"
PARAM_OZONE = "44201"
YEARS = [2014, 2019, 2021]
DATA_PATH = ROOT / "data" / "rq4_aqs_populous.csv"


def load_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Missing {DATA_PATH}. Run scripts/fetch/rq4_fetch_aqs_populous.py first."
        )
    return pd.read_csv(DATA_PATH)


def main() -> None:
    fig_dir = ensure_out_dir("rq4_populous_comparison", "figures")
    tbl_dir = ensure_out_dir("rq4_populous_comparison", "tables")

    out_df = load_data()
    if out_df.empty:
        raise ValueError("AQS data file is empty. Re-run the fetch script.")

    out_csv = tbl_dir / "rq4_populous_counties_aqs.csv"
    out_df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")

    # Plot: 2x3 grid (PM2.5/Ozone x 2014/2019/2021)
    fig, axes = plt.subplots(2, 3, figsize=(15, 9), sharex=False)
    params = [PARAM_PM25, PARAM_OZONE]
    for i, param in enumerate(params):
        for j, year in enumerate(YEARS):
            ax = axes[i, j]
            sub = out_df[(out_df["param"] == param) & (out_df["year"] == year)].copy()
            if sub.empty:
                ax.set_visible(False)
                continue
            sub = sub.sort_values("aqs_mean", ascending=False)
            colors = sub["is_ca"].map(lambda x: "#1f77b4" if x else "#ff7f0e")
            ax.barh(sub["county"], sub["aqs_mean"], color=colors)
            label = "PM2.5" if param == PARAM_PM25 else "Ozone"
            ax.set_title(f"{label} ({year})")
            ax.invert_yaxis()
            ax.set_xlabel("AQS Annual Mean")
            if not sub["aqs_mean"].empty:
                xmax = sub["aqs_mean"].max() * 1.15
                ax.set_xlim(0, xmax)

    fig.suptitle("AQS Air Quality in the 10 Most Populous Counties", y=1.02)
    fig.tight_layout()
    out_fig = fig_dir / "rq4_populous_counties_aqs.png"
    fig.savefig(out_fig, dpi=150, bbox_inches="tight")
    print(f"Wrote {out_fig}")


if __name__ == "__main__":
    main()
