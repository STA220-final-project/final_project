"""Q3: County mean CES score trends (top/bottom change)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR

TOP_N = 8


def main() -> None:
    fig_dir = ensure_out_dir("rq2_county_trends", "figures")
    tbl_dir = ensure_out_dir("rq2_county_trends", "tables")
    df = load_data()

    # Average CES score by county and year.
    county_year = (
        df.groupby(["county", "year"], as_index=False)["ces_score"].mean()
    )
    pivot = county_year.pivot(index="county", columns="year", values="ces_score")
    pivot["delta_2014_2021"] = pivot[2021] - pivot[2014]
    slope = pivot["delta_2014_2021"].sort_values(ascending=False)

    top = slope.head(TOP_N).index
    bottom = slope.tail(TOP_N).index
    sel = list(top) + list(bottom)
    sel_df = pivot.loc[sel, [2014, 2019, 2021]]

    # Plot top (largest increases) and bottom (largest decreases) in two panels.
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    for county in top:
        ax1.plot([2014, 2019, 2021], sel_df.loc[county], marker="o", linewidth=1.5, alpha=0.9, label=county)
    ax1.set_title(f"Plot: Line Chart (Top {TOP_N} Increases)")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Mean CES Score")
    ax1.legend(fontsize=7, ncol=1, frameon=False)

    for county in bottom:
        ax2.plot([2014, 2019, 2021], sel_df.loc[county], marker="o", linewidth=1.5, alpha=0.9, label=county)
    ax2.set_title(f"Plot: Line Chart (Top {TOP_N} Decreases)")
    ax2.set_xlabel("Year")
    ax2.legend(fontsize=7, ncol=1, frameon=False)

    fig.suptitle("County Mean CES Trends (Separated by Increase vs Decrease)", y=1.02)
    fig.tight_layout()

    out = fig_dir / "rq2_county_trends_linechart.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq2_county_trends_linechart.csv"
    export = pivot.copy()
    export.insert(0, "county", export.index)
    export.reset_index(drop=True).to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
