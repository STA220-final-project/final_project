"""Q1: Output top 10 improving and worsening tracts (2014→2021)."""

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, OUT_DIR, ensure_out_dir

TOP_N = 10


def main() -> None:
    tbl_dir = ensure_out_dir("rq1_tract_change", "tables")
    notes_dir = ensure_out_dir("rq1_tract_change", "notes")
    df = load_data()

    wide = df.pivot_table(
        index="census_tract",
        columns="year",
        values="ces_score",
        aggfunc="first",
    )
    wide = wide.dropna(subset=[2014, 2021])
    wide["delta_2014_2021"] = wide[2021] - wide[2014]

    county_map = (
        df.dropna(subset=["county"])
        .sort_values(["census_tract", "year"])
        .groupby("census_tract")["county"]
        .first()
        .to_dict()
    )

    top = wide.sort_values("delta_2014_2021", ascending=False).head(TOP_N).copy()
    bottom = wide.sort_values("delta_2014_2021", ascending=True).head(TOP_N).copy()

    def format_table(sub: pd.DataFrame) -> pd.DataFrame:
        out = sub[[2014, 2021, "delta_2014_2021"]].copy()
        out["county"] = [county_map.get(idx, "Unknown") for idx in out.index]
        out.insert(0, "census_tract", out.index)
        return out.reset_index(drop=True)

    top_out = format_table(top)
    bottom_out = format_table(bottom)

    top_path = tbl_dir / "rq1_top10_improving.csv"
    bottom_path = tbl_dir / "rq1_top10_worsening.csv"
    top_out.to_csv(top_path, index=False)
    bottom_out.to_csv(bottom_path, index=False)

    summary_path = notes_dir / "rq1_top10_summary.txt"
    with summary_path.open("w") as f:
        f.write("Top 10 improving tracts (2014→2021):\n")
        f.write(top_out.to_string(index=False))
        f.write("\n\nTop 10 worsening tracts (2014→2021):\n")
        f.write(bottom_out.to_string(index=False))

    print(f"Wrote {top_path}")
    print(f"Wrote {bottom_path}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
