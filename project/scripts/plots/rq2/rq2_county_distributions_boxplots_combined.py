"""Q2: Combined boxplots (all counties in one chart)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir

TOP_N = 8
YEARS = [2014, 2019, 2021]


def main() -> None:
    fig_dir = ensure_out_dir("rq2_county_trends", "figures")
    tbl_dir = ensure_out_dir("rq2_county_trends", "tables")
    df = load_data()

    county_counts = df.groupby("county").size().sort_values(ascending=False)
    counties = county_counts.head(TOP_N).index.tolist()
    subset = df[df["county"].isin(counties)].copy()
    subset = subset[subset["year"].isin(YEARS)]

    fig, ax = plt.subplots(figsize=(12, max(6, TOP_N * 0.6)))
    colors = {2014: "#1f77b4", 2019: "#2ca02c", 2021: "#d62728"}

    positions = []
    data = []
    labels = []
    pos = 1
    for county in counties:
        for year in YEARS:
            vals = subset[(subset["county"] == county) & (subset["year"] == year)][
                "ces_score"
            ].dropna()
            data.append(vals.astype(float).values)
            positions.append(pos)
            labels.append(f"{county}\n{year}")
            pos += 1
        pos += 1

    bp = ax.boxplot(
        data,
        positions=positions,
        widths=0.6,
        patch_artist=True,
        showfliers=False,
    )
    for i, patch in enumerate(bp["boxes"]):
        year = YEARS[i % len(YEARS)]
        patch.set_facecolor(colors[year])
        patch.set_alpha(0.35)

    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=90, fontsize=7)
    ax.set_title("County CES Distributions (Combined Boxplots)")
    ax.set_ylabel("CES Score")
    fig.tight_layout()

    out = fig_dir / "rq2_county_distributions_boxplots_combined.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq2_county_distributions_boxplots_combined.csv"
    subset[["county", "year", "ces_score"]].to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
