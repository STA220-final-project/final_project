"""Scatter + correlation: Linguistic isolation vs selected metrics (2021)."""

import numpy as np
import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR


def scatter_with_corr(ax, x, y, x_label, y_label, color):
    mask = x.notna() & y.notna()
    ax.scatter(x[mask], y[mask], s=8, alpha=0.25, color=color)
    if mask.sum() > 1:
        m, b = np.polyfit(x[mask], y[mask], 1)
        xs = np.linspace(x[mask].min(), x[mask].max(), 100)
        ax.plot(xs, m * xs + b, color="#d62728", linewidth=2)
        corr = np.corrcoef(x[mask], y[mask])[0, 1]
    else:
        corr = float("nan")
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(f"Plot: Scatter (corr={corr:.3f})")
    return corr


def main() -> None:
    ensure_out_dir()
    df = load_data()

    # [PRR] Starting with simple scatter + correlation so we can see
    # if there is any relationship before doing more complex plots.

    d2021 = df[df["year"] == 2021].copy()
    x = d2021["linguistic_isolation_pctl"].astype(float)

    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    corr1 = scatter_with_corr(
        axes[0],
        x,
        d2021["traffic_pctl"].astype(float),
        "Linguistic Isolation Percentile",
        "Traffic Percentile",
        "#1f77b4",
    )
    corr2 = scatter_with_corr(
        axes[1],
        x,
        d2021["solid_waste_pctl"].astype(float),
        "Linguistic Isolation Percentile",
        "Solid Waste Percentile",
        "#2ca02c",
    )
    corr3 = scatter_with_corr(
        axes[2],
        x,
        d2021["cardiovascular_disease_pctl"].astype(float),
        "Linguistic Isolation Percentile",
        "Cardiovascular Disease Percentile",
        "#9467bd",
    )

    fig.suptitle("Linguistic Isolation vs Selected Metrics (2021)", y=1.05)
    fig.tight_layout()

    out = OUT_DIR / "qx_plot_scatter_linguistic_isolation_vs_metrics.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Wrote {out}")
    print(f"Correlation (Linguistic Isolation vs Traffic): {corr1:.3f}")
    print(f"Correlation (Linguistic Isolation vs Solid Waste): {corr2:.3f}")
    print(f"Correlation (Linguistic Isolation vs Cardio Disease): {corr3:.3f}")


if __name__ == "__main__":
    main()
