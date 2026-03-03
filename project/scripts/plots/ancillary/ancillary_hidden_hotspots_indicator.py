"""Q7: Hidden hotspots in 2021 (low CES, extreme pollution indicator)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR, POLLUTION_PCTL_COLS


def main() -> None:
    fig_dir = ensure_out_dir("ancillary", "figures")
    tbl_dir = ensure_out_dir("ancillary", "tables")
    df = load_data()

    d2021 = df[df["year"] == 2021].copy()
    # Hidden hotspot = low overall CES but one indicator is extreme
    d2021["max_pollution_pctl"] = d2021[POLLUTION_PCTL_COLS].max(axis=1, skipna=True)
    d2021["max_pollution_indicator"] = d2021[POLLUTION_PCTL_COLS].idxmax(axis=1)

    hidden = d2021[(d2021["ces_percentile"] < 40) & (d2021["max_pollution_pctl"] >= 90)]

    indicator_counts = hidden["max_pollution_indicator"].value_counts()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(range(len(indicator_counts)), indicator_counts.values, color="#9467bd")
    ax.set_xticks(range(len(indicator_counts)))
    ax.set_xticklabels(indicator_counts.index, rotation=45, ha="right")
    ax.set_ylabel("Count")
    ax.set_title("Plot: Bar Chart (Top Indicator Among Hidden Hotspots, 2021)")
    fig.tight_layout()

    out = fig_dir / "ancillary_hidden_hotspots_indicator.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "ancillary_hidden_hotspots.csv"
    hidden.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")

    summary_out = tbl_dir / "ancillary_hidden_hotspots_indicator_counts.csv"
    indicator_counts.rename_axis("indicator").rename("count").to_csv(summary_out)
    print(f"Wrote {summary_out}")


if __name__ == "__main__":
    main()
