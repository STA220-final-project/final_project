"""Q1: Dumbbell chart for top CES score increases (2014→2021)."""

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

    wide = df.pivot_table(
        index="census_tract",
        columns="year",
        values="ces_score",
        aggfunc="first",
    )
    wide = wide.dropna(subset=[2014, 2021])
    wide["delta_2014_2021"] = wide[2021] - wide[2014]
    top = wide.sort_values("delta_2014_2021", ascending=False).head(TOP_N)

    county_map = (
        df.dropna(subset=["county"])
        .sort_values(["census_tract", "year"])
        .groupby("census_tract")["county"]
        .first()
        .to_dict()
    )
    labels = [f"{idx} ({county_map.get(idx, 'Unknown')})" for idx in top.index]

    fig, ax = plt.subplots(figsize=(9, max(6, TOP_N * 0.18)))
    y = list(range(len(top)))
    ax.hlines(y, top[2014], top[2021], color="#bbbbbb", linewidth=1)
    ax.scatter(top[2014], y, color="#1f77b4", label="2014", s=18)
    ax.scatter(top[2021], y, color="#d62728", label="2021", s=18)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=6)
    ax.set_xlabel("CES Score")
    ax.set_title(f"Dumbbell: Top {TOP_N} Tracts by CES Increase (2014→2021)")
    ax.legend(frameon=False)
    fig.tight_layout()

    out = fig_dir / "rq1_dumbbell_top_increases.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq1_dumbbell_top_increases.csv"
    export = top[[2014, 2021, "delta_2014_2021"]].copy()
    export["county"] = [county_map.get(idx, "Unknown") for idx in export.index]
    export.insert(0, "census_tract", export.index)
    export.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
