"""Scatter + correlation: Pollution Burden vs Poverty/Education (2021)."""

import numpy as np
import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR


def main() -> None:
    ensure_out_dir()
    df = load_data()

    # [PRR] I went with scatter + correlation first because it directly answers
    # whether there is a relationship, and is simpler to explain than a heatmap.

    d2021 = df[df["year"] == 2021].copy()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Pollution burden vs poverty
    x1 = d2021["pollution_burden_pctl"].astype(float)
    y1 = d2021["poverty_pctl"].astype(float)
    mask1 = x1.notna() & y1.notna()
    ax1.scatter(x1[mask1], y1[mask1], s=8, alpha=0.25, color="#1f77b4")
    if mask1.sum() > 1:
        m, b = np.polyfit(x1[mask1], y1[mask1], 1)
        xs = np.linspace(x1[mask1].min(), x1[mask1].max(), 100)
        ax1.plot(xs, m * xs + b, color="#d62728", linewidth=2)
    ax1.set_xlabel("Pollution Burden Percentile")
    ax1.set_ylabel("Poverty Percentile")
    ax1.set_title("Plot: Scatter (Pollution vs Poverty, 2021)")

    # Pollution burden vs education
    x2 = d2021["pollution_burden_pctl"].astype(float)
    y2 = d2021["education_pctl"].astype(float)
    mask2 = x2.notna() & y2.notna()
    ax2.scatter(x2[mask2], y2[mask2], s=8, alpha=0.25, color="#2ca02c")
    if mask2.sum() > 1:
        m, b = np.polyfit(x2[mask2], y2[mask2], 1)
        xs = np.linspace(x2[mask2].min(), x2[mask2].max(), 100)
        ax2.plot(xs, m * xs + b, color="#d62728", linewidth=2)
    ax2.set_xlabel("Pollution Burden Percentile")
    ax2.set_ylabel("Education Percentile")
    ax2.set_title("Plot: Scatter (Pollution vs Education, 2021)")

    fig.tight_layout()
    out = OUT_DIR / "qx_plot_scatter_pollution_vs_poverty_education.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    # Print correlations for quick reference
    if mask1.sum() > 1:
        corr1 = np.corrcoef(x1[mask1], y1[mask1])[0, 1]
        print(f"Correlation (Pollution vs Poverty): {corr1:.3f}")
    if mask2.sum() > 1:
        corr2 = np.corrcoef(x2[mask2], y2[mask2])[0, 1]
        print(f"Correlation (Pollution vs Education): {corr2:.3f}")


if __name__ == "__main__":
    main()
