"""Q1: Histogram of CES score change (2014→2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR


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

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(wide["delta_2014_2021"], bins=40, color="#4c78a8", alpha=0.85)
    ax.axvline(0, color="#333333", linewidth=1)
    ax.set_xlabel("Δ CES Score (2021 − 2014)")
    ax.set_ylabel("Tract Count")
    ax.set_title("Histogram: CES Score Change Across Tracts (2014→2021)")
    fig.tight_layout()

    out = fig_dir / "rq1_histogram_ces_change.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq1_histogram_ces_change.csv"
    wide[["delta_2014_2021"]].reset_index().to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
