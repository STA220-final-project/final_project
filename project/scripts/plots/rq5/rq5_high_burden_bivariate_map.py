"""RQ5: Bivariate map (2014 vs 2021 high-burden share)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, add_missing_ces_percentile, try_import_basemap


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
        return counts[["county", "high_burden_share"]]

    s2014 = share_for_year(2014).rename(columns={"high_burden_share": "share_2014"})
    s2021 = share_for_year(2021).rename(columns={"high_burden_share": "share_2021"})
    merged = s2014.merge(s2021, on="county", how="outer").fillna(0)

    out_csv = tbl_dir / "rq5_high_burden_bivariate_2014_2021.csv"
    merged.to_csv(out_csv, index=False)

    gpd, ctx = try_import_basemap()
    counties_shp = ROOT / "data" / "shapefile" / "CA_Counties.shp"
    if not gpd or not counties_shp.exists():
        raise FileNotFoundError("Missing geopandas or CA_Counties.shp for bivariate map.")

    counties = gpd.read_file(counties_shp)
    name_col = None
    for cand in ["name", "NAME", "county", "COUNTY", "CountyName", "COUNTY_NAME"]:
        if cand in counties.columns:
            name_col = cand
            break
    if name_col is None:
        name_col = counties.columns[0]

    counties["county_norm"] = counties[name_col].astype(str).str.lower().str.replace("county", "").str.strip()
    merged["county_norm"] = merged["county"].astype(str).str.lower().str.replace("county", "").str.strip()
    chor = counties.merge(merged, on="county_norm", how="left")
    chor = chor.fillna(0)

    # Bivariate color bins
    # Quantile bins can fail if many zeros; allow duplicates and fall back to cut
    try:
        chor["biv_x"] = pd.qcut(chor["share_2014"], 3, labels=[0, 1, 2], duplicates="drop")
        chor["biv_y"] = pd.qcut(chor["share_2021"], 3, labels=[0, 1, 2], duplicates="drop")
        # If fewer than 3 bins survived, fall back to equal-width bins
        if chor["biv_x"].isna().any() or chor["biv_y"].isna().any():
            raise ValueError("qcut produced NaNs")
    except Exception:
        chor["biv_x"] = pd.cut(chor["share_2014"], 3, labels=[0, 1, 2], include_lowest=True)
        chor["biv_y"] = pd.cut(chor["share_2021"], 3, labels=[0, 1, 2], include_lowest=True)
    chor["biv_key"] = chor["biv_x"].astype(int) * 3 + chor["biv_y"].astype(int)

    palette = [
        "#e8e8e8", "#b5c0da", "#6c83b5",
        "#b8d6be", "#90b2b3", "#567994",
        "#73ae80", "#5a9178", "#2a5a5b",
    ]
    chor["color"] = chor["biv_key"].apply(lambda k: palette[k])

    chor = chor.to_crs(epsg=3857)
    fig, ax = plt.subplots(figsize=(8, 10))
    chor.plot(ax=ax, color=chor["color"], linewidth=0.6, edgecolor="#333333")
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, alpha=0.6)
    ax.set_axis_off()
    ax.set_title("High‑Burden Share (2014 vs 2021, Bivariate)")
    fig.tight_layout()

    out_fig = fig_dir / "rq5_high_burden_bivariate_2014_2021.png"
    fig.savefig(out_fig, dpi=150, bbox_inches="tight")


if __name__ == "__main__":
    main()
