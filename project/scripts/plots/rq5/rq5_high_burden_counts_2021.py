"""Q6: Counties with most high-burden tracts (2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR, add_missing_ces_percentile

TOP_N = 20


def main() -> None:
    fig_dir = ensure_out_dir("rq5_high_burden", "figures")
    tbl_dir = ensure_out_dir("rq5_high_burden", "tables")
    df = add_missing_ces_percentile(load_data())

    d2021 = df[df["year"] == 2021].copy()
    # High-burden = CES percentile >= 80 (top 20% most burdened)
    high = d2021[d2021["ces_percentile"] >= 80]
    counts = high.groupby("county").size().sort_values(ascending=False)
    total = d2021.groupby("county").size()
    share = (counts / total).sort_values(ascending=False)

    top = counts.head(TOP_N)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top.index[::-1], top.values[::-1], color="#2ca02c")
    ax.set_xlabel("High-burden tract count (CES >= 80th percentile)")
    ax.set_title(f"Plot: Bar Chart (Top {TOP_N} Counties by Count, 2021)")
    fig.tight_layout()

    out = fig_dir / "rq5_high_burden_counts_2021.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    # Also show the percentage of tracts that are high-burden (size-adjusted).
    top_share = share.head(TOP_N)
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.barh(top_share.index[::-1], (top_share.values[::-1] * 100), color="#ff7f0e")
    ax2.set_xlabel("High-burden tracts (% of county tracts)")
    ax2.set_title(f"Plot: Bar Chart (Top {TOP_N} Counties by Share, 2021)")
    fig2.tight_layout()

    out2 = fig_dir / "rq5_high_burden_share_2021.png"
    fig2.savefig(out2, dpi=150)
    print(f"Wrote {out2}")

    data_out = tbl_dir / "rq5_high_burden_2021.csv"
    export = (
        counts.rename("high_burden_count")
        .to_frame()
        .join(share.rename("high_burden_share"))
        .reset_index()
    )
    export.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
