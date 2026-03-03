"""Q6: Counties with most high-burden tracts (2014, 2019, 2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR, add_missing_ces_percentile

TOP_N = 20
YEARS = [2014, 2019, 2021]


def plot_for_year(df, year: int) -> None:
    d = df[df["year"] == year].copy()
    high = d[d["ces_percentile"] >= 80]
    counts = high.groupby("county").size().sort_values(ascending=False)
    total = d.groupby("county").size()
    share = (counts / total).sort_values(ascending=False)

    top = counts.head(TOP_N)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top.index[::-1], top.values[::-1], color="#2ca02c")
    ax.set_xlabel("High-burden tract count (CES >= 80th percentile)")
    ax.set_title(f"Plot: Bar Chart (Top {TOP_N} Counties by Count, {year})")
    fig.tight_layout()
    fig_dir = ensure_out_dir("rq5_high_burden", "figures")
    tbl_dir = ensure_out_dir("rq5_high_burden", "tables")

    out = fig_dir / f"rq5_high_burden_counts_{year}.png"
    fig.savefig(out, dpi=150)

    top_share = share.head(TOP_N)
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.barh(top_share.index[::-1], (top_share.values[::-1] * 100), color="#ff7f0e")
    ax2.set_xlabel("High-burden tracts (% of county tracts)")
    ax2.set_title(f"Plot: Bar Chart (Top {TOP_N} Counties by Share, {year})")
    fig2.tight_layout()
    out2 = fig_dir / f"rq5_high_burden_share_{year}.png"
    fig2.savefig(out2, dpi=150)

    print(f"Wrote {out}")
    print(f"Wrote {out2}")

    data_out = tbl_dir / f"rq5_high_burden_{year}.csv"
    export = (
        counts.rename("high_burden_count")
        .to_frame()
        .join(share.rename("high_burden_share"))
        .reset_index()
    )
    export.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


def main() -> None:
    ensure_out_dir("rq5_high_burden", "figures")
    ensure_out_dir("rq5_high_burden", "tables")
    df = add_missing_ces_percentile(load_data())
    for year in YEARS:
        plot_for_year(df, year)


if __name__ == "__main__":
    main()
