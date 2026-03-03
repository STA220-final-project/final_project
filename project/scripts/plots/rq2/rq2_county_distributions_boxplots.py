"""Q2: Distribution comparison by county and year (boxplots)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR

TOP_N = 8
YEARS = [2014, 2019, 2021]


def main() -> None:
    fig_dir = ensure_out_dir("rq2_county_trends", "figures")
    tbl_dir = ensure_out_dir("rq2_county_trends", "tables")
    df = load_data()

    # Choose top counties by number of tracts for readability.
    county_counts = df.groupby("county").size().sort_values(ascending=False)
    counties = county_counts.head(TOP_N).index.tolist()

    subset = df[df["county"].isin(counties)].copy()
    subset = subset[subset["year"].isin(YEARS)]

    fig, axes = plt.subplots(TOP_N, 1, figsize=(8, max(8, TOP_N * 1.1)), sharex=True)
    if TOP_N == 1:
        axes = [axes]

    colors = {2014: "#1f77b4", 2019: "#2ca02c", 2021: "#d62728"}
    positions = [1, 2, 3]

    for ax, county in zip(axes, counties):
        data = []
        for year in YEARS:
            vals = subset[(subset["county"] == county) & (subset["year"] == year)][
                "ces_score"
            ].dropna()
            data.append(vals.astype(float).values)

        bp = ax.boxplot(
            data,
            positions=positions,
            widths=0.6,
            patch_artist=True,
            showfliers=False,
        )
        for patch, year in zip(bp["boxes"], YEARS):
            patch.set_facecolor(colors[year])
            patch.set_alpha(0.35)
        ax.set_ylabel(county, rotation=0, labelpad=35, fontsize=8, va="center")
        ax.set_yticks([])
        ax.grid(axis="x", linestyle="--", alpha=0.2)

    axes[-1].set_xticks(positions)
    axes[-1].set_xticklabels([str(y) for y in YEARS])
    axes[0].set_title("County CES Distributions (Boxplots, Top Counties by Tract Count)")
    axes[-1].set_xlabel("CES Score")
    fig.tight_layout()

    out = fig_dir / "rq2_county_distributions_boxplots.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Wrote {out}")

    # Export raw data used for boxplots.
    data_out = tbl_dir / "rq2_county_distributions_boxplots.csv"
    subset[["county", "year", "ces_score"]].to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
