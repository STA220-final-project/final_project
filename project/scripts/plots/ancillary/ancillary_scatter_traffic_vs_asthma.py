"""Q5: Scatter of ΔTraffic vs ΔAsthma (2014→2021)."""

import numpy as np
import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR


def main() -> None:
    fig_dir = ensure_out_dir("ancillary", "figures")
    tbl_dir = ensure_out_dir("ancillary", "tables")
    df = load_data()

    # Make one table per metric so we can compute changes between years.
    wide_t = df.pivot_table(
        index="census_tract",
        columns="year",
        values="traffic_pctl",
        aggfunc="first",
    )
    wide_a = df.pivot_table(
        index="census_tract",
        columns="year",
        values="asthma_pctl",
        aggfunc="first",
    )
    joined = wide_t.join(wide_a, lsuffix="_traffic", rsuffix="_asthma")
    joined = joined.dropna(subset=["2014_traffic", "2021_traffic", "2014_asthma", "2021_asthma"])
    joined["delta_traffic"] = joined["2021_traffic"] - joined["2014_traffic"]
    joined["delta_asthma"] = joined["2021_asthma"] - joined["2014_asthma"]

    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(joined["delta_traffic"], joined["delta_asthma"], s=8, alpha=0.25, color="#1f77b4")
    x = joined["delta_traffic"].values
    y = joined["delta_asthma"].values
    if len(x) > 1:
        # Simple trend line to show overall direction.
        m, b = np.polyfit(x, y, 1)
        xs = np.linspace(x.min(), x.max(), 100)
        ax.plot(xs, m * xs + b, color="#d62728", linewidth=2)
    ax.axhline(0, color="#999999", linewidth=1)
    ax.axvline(0, color="#999999", linewidth=1)
    ax.set_xlabel("Δ Traffic Percentile (2021−2014)")
    ax.set_ylabel("Δ Asthma Percentile (2021−2014)")
    ax.set_title("Plot: Scatter (Traffic vs Asthma Change)")
    fig.tight_layout()

    out = fig_dir / "ancillary_scatter_traffic_vs_asthma.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "ancillary_scatter_traffic_vs_asthma.csv"
    joined.reset_index().to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
