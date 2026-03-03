"""Q2: Identify counties with consistent improvement or decline in CES scores."""

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR, county_centroids, north_south_split


def trend_label(row: pd.Series) -> str:
    if row[2014] <= row[2019] <= row[2021]:
        return "Consistent Improvement"
    if row[2014] >= row[2019] >= row[2021]:
        return "Consistent Decline"
    return "Mixed"


def main() -> None:
    tbl_dir = ensure_out_dir("rq2_county_trends", "tables")
    notes_dir = ensure_out_dir("rq2_county_trends", "notes")
    df = load_data()

    county_year = df.groupby(["county", "year"], as_index=False)["ces_score"].mean()
    pivot = county_year.pivot(index="county", columns="year", values="ces_score")
    pivot = pivot.dropna(subset=[2014, 2019, 2021])
    pivot["trend"] = pivot.apply(trend_label, axis=1)
    pivot["delta_2014_2021"] = pivot[2021] - pivot[2014]

    # Add North/South label based on centroid latitude.
    centroids = county_centroids(df[df["year"] == 2021])
    centroids["region"] = centroids["latitude"].apply(north_south_split)
    merged = pivot.reset_index().merge(centroids[["county", "region"]], on="county", how="left")

    out_csv = tbl_dir / "rq2_county_consistency.csv"
    merged.to_csv(out_csv, index=False)

    # Summaries for presentation notes.
    summary_path = notes_dir / "rq2_county_consistency_summary.txt"
    with summary_path.open("w") as f:
        f.write("Counts by trend:\n")
        f.write(merged["trend"].value_counts().to_string())
        f.write("\n\nCounts by region and trend:\n")
        f.write(merged.groupby(["region", "trend"]).size().to_string())

    print(f"Wrote {out_csv}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
