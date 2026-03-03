"""Q1: Top CES score changes (2014→2019→2021) slopegraph."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR

TOP_N = 20


def main() -> None:
    fig_dir = ensure_out_dir("rq1_tract_change", "figures")
    tbl_dir = ensure_out_dir("rq1_tract_change", "tables")
    df = load_data()

    # Pivot to wide format so each tract has a score for each year.
    wide = df.pivot_table(
        index="census_tract",
        columns="year",
        values="ces_score",
        aggfunc="first",
    )
    wide = wide.dropna(subset=[2014, 2019, 2021])
    wide["delta_2014_2021"] = wide[2021] - wide[2014]
    top = wide.sort_values("delta_2014_2021", ascending=False).head(TOP_N)

    # Map each tract to a county (use first non-null).
    county_map = (
        df.dropna(subset=["county"])
        .sort_values(["census_tract", "year"])
        .groupby("census_tract")["county"]
        .first()
        .to_dict()
    )

    # Color by county so patterns are easier to see (tract labels are skipped).
    counties = [county_map.get(idx, "Unknown") for idx in top.index]
    unique_counties = sorted(set(counties))
    cmap = plt.get_cmap("tab20")
    color_map = {c: cmap(i % 20) for i, c in enumerate(unique_counties)}

    fig, ax = plt.subplots(figsize=(12, 7))
    for idx, row in top.iterrows():
        county = county_map.get(idx, "Unknown")
        ax.plot([0, 1, 2], [row[2014], row[2019], row[2021]], color=color_map[county], linewidth=1.6, alpha=0.85)
        ax.scatter([0, 1, 2], [row[2014], row[2019], row[2021]], s=18, color=color_map[county], alpha=0.9)

    ax.set_yticks([])
    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels([2014, 2019, 2021])
    ax.set_ylabel("CES Score")
    ax.set_title(f"Plot: Slopegraph (Top {TOP_N} CES Increases, Colored by County)")
    # Legend for counties present in this plot.
    handles = [
        plt.Line2D([0], [0], color=color_map[c], lw=2, label=c)
        for c in unique_counties
    ]
    ax.legend(handles=handles, fontsize=7, ncol=2, frameon=False, loc="upper left", bbox_to_anchor=(1.02, 1))

    fig.tight_layout()
    out = fig_dir / "rq1_plot_slopegraph_top_changes.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq1_plot_slopegraph_top_changes.csv"
    export = top[[2014, 2019, 2021, "delta_2014_2021"]].copy()
    export["county"] = [county_map.get(idx, "Unknown") for idx in export.index]
    export.insert(0, "census_tract", export.index)
    export.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
