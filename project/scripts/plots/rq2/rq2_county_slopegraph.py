"""Q2: County-level slopegraph for mean CES (2014→2019→2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR

TOP_N = 10


def main() -> None:
    fig_dir = ensure_out_dir("rq2_county_trends", "figures")
    tbl_dir = ensure_out_dir("rq2_county_trends", "tables")
    df = load_data()

    county_year = df.groupby(["county", "year"], as_index=False)["ces_score"].mean()
    pivot = county_year.pivot(index="county", columns="year", values="ces_score")
    pivot = pivot.dropna(subset=[2014, 2019, 2021])
    pivot["delta_2014_2021"] = pivot[2021] - pivot[2014]
    slope = pivot["delta_2014_2021"].sort_values(ascending=False)

    top = slope.head(TOP_N).index.tolist()
    bottom = slope.tail(TOP_N).index.tolist()
    sel = top + bottom

    fig, ax = plt.subplots(figsize=(10, 6))
    for county in sel:
        row = pivot.loc[county]
        color = "#1f77b4" if county in top else "#d62728"
        ax.plot([0, 1, 2], [row[2014], row[2019], row[2021]], color=color, alpha=0.85)
        ax.scatter([0, 1, 2], [row[2014], row[2019], row[2021]], s=18, color=color)
        ax.text(2.02, row[2021], county, fontsize=7, va="center")

    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels([2014, 2019, 2021])
    ax.set_ylabel("Mean CES Score")
    ax.set_title(f"Slopegraph: County Mean CES (Top/Bottom {TOP_N})")
    ax.set_xlim(-0.1, 2.4)
    fig.tight_layout()

    out = fig_dir / "rq2_county_slopegraph.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq2_county_slopegraph.csv"
    export = pivot.loc[sel, [2014, 2019, 2021, "delta_2014_2021"]].copy()
    export.insert(0, "county", export.index)
    export.reset_index(drop=True).to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
