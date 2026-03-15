"""RQ5: Stacked bars of high-burden share by county (2014/2019/2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, add_missing_ces_percentile


def main() -> None:
    fig_dir = ensure_out_dir("rq5_high_burden", "figures")
    tbl_dir = ensure_out_dir("rq5_high_burden", "tables")

    df = add_missing_ces_percentile(load_data())

    def share_for_year(year: int) -> pd.DataFrame:
        d = df[df["year"] == year].copy()
        high = d[d["ces_percentile"] >= 80]
        counts = high.groupby("county").size().rename("high_burden_count").reset_index()
        totals = d.groupby("county").size().rename("total_tracts").reset_index()
        counts = counts.merge(totals, on="county", how="right").fillna(0)
        counts["high_burden_share"] = counts["high_burden_count"] / counts["total_tracts"].replace(0, 1)
        counts["year"] = year
        return counts[["county", "high_burden_share", "year"]]

    s2014 = share_for_year(2014)
    s2019 = share_for_year(2019)
    s2021 = share_for_year(2021)
    all_df = pd.concat([s2014, s2019, s2021], ignore_index=True)

    # Use top 10 by 2021 share for readability
    top = (
        s2021.sort_values("high_burden_share", ascending=False)
        .head(10)["county"]
        .tolist()
    )
    sub = all_df[all_df["county"].isin(top)]

    pivot = sub.pivot(index="county", columns="year", values="high_burden_share").fillna(0)
    pivot = pivot.loc[top]

    ax = pivot.plot(kind="barh", stacked=True, figsize=(9, 6), color=["#8da0cb", "#fc8d62", "#66c2a5"])
    ax.set_xlabel("High‑burden share (stacked by year)")
    ax.set_ylabel("")
    ax.set_title("High‑Burden Share by County (Top 10 in 2021)")
    ax.legend(title="Year", loc="lower right")
    # Add value labels on each stacked segment
    for container in ax.containers:
        ax.bar_label(container, fmt="%.2f", label_type="center", fontsize=7, color="#222222")
    plt.tight_layout()

    out_fig = fig_dir / "rq5_high_burden_stacked_bars.png"
    plt.savefig(out_fig, dpi=150)

    out_csv = tbl_dir / "rq5_high_burden_stacked_bars.csv"
    pivot.reset_index().to_csv(out_csv, index=False)


if __name__ == "__main__":
    main()
