"""Q2: Heatmap of county mean CES score by year (top/bottom by change)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir


def main() -> None:
    fig_dir = ensure_out_dir("rq2_county_trends", "figures")
    tbl_dir = ensure_out_dir("rq2_county_trends", "tables")
    df = load_data()

    county_year = df.groupby(["county", "year"], as_index=False)["ces_score"].mean()
    pivot = county_year.pivot(index="county", columns="year", values="ces_score")
    pivot = pivot.dropna(subset=[2014, 2019, 2021])
    pivot["delta_2014_2021"] = pivot[2021] - pivot[2014]
    pivot = pivot.sort_values("delta_2014_2021", ascending=False)

    top = pivot.head(10)
    bottom = pivot.tail(10)
    pivot = pd.concat([top, bottom])[[2014, 2019, 2021]]

    data = pivot.values
    fig, ax = plt.subplots(figsize=(7, max(6, len(pivot) * 0.3)))
    im = ax.imshow(data, aspect="auto", cmap="viridis")
    ax.set_yticks(np.arange(len(pivot)))
    ax.set_yticklabels(pivot.index, fontsize=6)
    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels([2014, 2019, 2021])
    ax.set_title("Heatmap: County Mean CES (Top/Bottom by Change)")
    plt.colorbar(im, ax=ax, label="Mean CES Score")
    fig.tight_layout()

    out = fig_dir / "rq2_county_heatmap_topbottom.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq2_county_heatmap_topbottom.csv"
    export = pivot.copy()
    export.insert(0, "county", export.index)
    export.reset_index(drop=True).to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
