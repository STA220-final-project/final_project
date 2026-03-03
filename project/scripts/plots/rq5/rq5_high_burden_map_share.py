"""Q6: County-level map of high-burden tract counts (2014, 2019, 2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import (
    load_data,
    ensure_out_dir,
    OUT_DIR,
    county_centroids,
    add_missing_ces_percentile,
    plot_geojson_outline,
    try_import_basemap,
)

YEARS = [2014, 2019, 2021]


def plot_for_year(df, year: int) -> None:
    d = df[df["year"] == year].copy()
    high = d[d["ces_percentile"] >= 80]
    counts = high.groupby("county").size().rename("high_burden_count").reset_index()
    totals = d.groupby("county").size().rename("total_tracts").reset_index()
    counts = counts.merge(totals, on="county", how="right").fillna(0)
    counts["high_burden_share"] = counts["high_burden_count"] / counts["total_tracts"].replace(0, 1)

    centroids = county_centroids(d)
    merged = counts.merge(centroids, on="county", how="left").dropna(subset=["latitude", "longitude"])

    fig, ax = plt.subplots(figsize=(9, 11))

    gpd, ctx = try_import_basemap()
    counties_path = ROOT / "data" / "ca_counties.geojson"

    if gpd and ctx and counties_path.exists():
        counties = gpd.read_file(counties_path)
        # Try common county name fields
        name_col = None
        for cand in ["name", "NAME", "county", "COUNTY", "CountyName", "COUNTY_NAME"]:
            if cand in counties.columns:
                name_col = cand
                break
        if name_col is None:
            name_col = counties.columns[0]

        counties["county_norm"] = counties[name_col].astype(str).str.lower().str.replace("county", "").str.strip()
        merged["county_norm"] = merged["county"].astype(str).str.lower().str.replace("county", "").str.strip()
        chor = counties.merge(
            merged[["county_norm", "high_burden_share"]],
            on="county_norm",
            how="left",
        )
        chor["high_burden_share"] = chor["high_burden_share"].fillna(0)

        chor = chor.to_crs(epsg=3857)
        chor.plot(
            ax=ax,
            column="high_burden_share",
            cmap="YlOrRd",
            linewidth=0.6,
            edgecolor="#333333",
            alpha=0.65,
            legend=True,
            legend_kwds={"label": "High-burden tract share"},
        )
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, alpha=0.85)
        ax.set_axis_off()
        ax.set_title(f"County High-Burden Tract Share ({year})")
        # Pad bounds a bit so the map isn't cropped
        minx, miny, maxx, maxy = chor.total_bounds
        pad_x = (maxx - minx) * 0.03
        pad_y = (maxy - miny) * 0.03
        ax.set_xlim(minx - pad_x, maxx + pad_x)
        ax.set_ylim(miny - pad_y, maxy + pad_y)
    elif gpd and ctx:
        gdf = gpd.GeoDataFrame(
            merged,
            geometry=gpd.points_from_xy(merged["longitude"], merged["latitude"]),
            crs="EPSG:4326",
        ).to_crs(epsg=3857)
        # Scale marker size by share for visibility
        sizes = (gdf["high_burden_share"].clip(lower=0.001) ** 0.5) * 900
        gdf.plot(
            ax=ax,
            column="high_burden_share",
            cmap="YlOrRd",
            markersize=sizes,
            alpha=0.7,
            legend=True,
            legend_kwds={"label": "High-burden tract share"},
        )
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, alpha=0.85)
        ax.set_axis_off()
        ax.set_title(f"County High-Burden Tract Share ({year})")
        minx, miny, maxx, maxy = gdf.total_bounds
        pad_x = (maxx - minx) * 0.03
        pad_y = (maxy - miny) * 0.03
        ax.set_xlim(minx - pad_x, maxx + pad_x)
        ax.set_ylim(miny - pad_y, maxy + pad_y)
    else:
        geojson_path = ROOT / "data" / "ca_state.geojson"
        plot_geojson_outline(ax, geojson_path)

        sizes = (merged["high_burden_share"].clip(lower=0.001) ** 0.5) * 900
        sc = ax.scatter(
            merged["longitude"],
            merged["latitude"],
            c=merged["high_burden_share"],
            cmap="YlOrRd",
            s=sizes,
            alpha=0.7,
            edgecolor="#222222",
            linewidths=0.4,
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title(f"County High-Burden Tract Share ({year})")
        plt.colorbar(sc, ax=ax, label="High-burden tract share", fraction=0.035, pad=0.02)
        ax.set_aspect("auto")
        # Pad bounds
        minx, maxx = merged["longitude"].min(), merged["longitude"].max()
        miny, maxy = merged["latitude"].min(), merged["latitude"].max()
        pad_x = (maxx - minx) * 0.03
        pad_y = (maxy - miny) * 0.03
        ax.set_xlim(minx - pad_x, maxx + pad_x)
        ax.set_ylim(miny - pad_y, maxy + pad_y)

    fig.tight_layout()

    fig_dir = ensure_out_dir("rq5_high_burden", "figures")
    tbl_dir = ensure_out_dir("rq5_high_burden", "tables")

    out = fig_dir / f"rq5_map_high_burden_share_{year}.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / f"rq5_map_high_burden_share_{year}.csv"
    merged.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


def main() -> None:
    ensure_out_dir("rq5_high_burden", "figures")
    ensure_out_dir("rq5_high_burden", "tables")
    df = add_missing_ces_percentile(load_data())
    for year in YEARS:
        plot_for_year(df, year)


if __name__ == "__main__":
    main()
