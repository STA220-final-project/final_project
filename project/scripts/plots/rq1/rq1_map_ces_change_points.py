"""RQ1: Spatial point map of CES score change (2014→2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, plot_geojson_outline, try_import_basemap


def main() -> None:
    fig_dir = ensure_out_dir("rq1_tract_change", "figures")
    tbl_dir = ensure_out_dir("rq1_tract_change", "tables")
    df = load_data()

    wide = df.pivot_table(
        index="census_tract",
        columns="year",
        values="ces_score",
        aggfunc="first",
    )
    wide = wide.dropna(subset=[2014, 2021])
    wide["delta_2014_2021"] = wide[2021] - wide[2014]

    coords = (
        df[df["year"] == 2021][["census_tract", "latitude", "longitude"]]
        .dropna()
        .drop_duplicates(subset=["census_tract"])
    )
    merged = wide.merge(coords, left_index=True, right_on="census_tract", how="inner")
    merged["latitude"] = merged["latitude"].astype(float)
    merged["longitude"] = merged["longitude"].astype(float)

    fig, ax = plt.subplots(figsize=(8.5, 10))

    gpd, ctx = try_import_basemap()
    if gpd and ctx:
        gdf = gpd.GeoDataFrame(
            merged,
            geometry=gpd.points_from_xy(merged["longitude"], merged["latitude"]),
            crs="EPSG:4326",
        ).to_crs(epsg=3857)
        gdf.plot(
            ax=ax,
            column="delta_2014_2021",
            cmap="RdBu_r",
            markersize=12,
            alpha=0.75,
            legend=True,
            legend_kwds={"label": "Δ CES Score (2021 − 2014)"},
        )
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
        ax.set_axis_off()
        ax.set_title("Tract-Level CES Change (2014→2021)")
    else:
        geojson_path = ROOT / "data" / "ca_state.geojson"
        plot_geojson_outline(ax, geojson_path)

        sc = ax.scatter(
            merged["longitude"],
            merged["latitude"],
            c=merged["delta_2014_2021"],
            cmap="RdBu_r",
            s=12,
            alpha=0.75,
            linewidths=0,
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title("Tract-Level CES Change (2014→2021)")
        plt.colorbar(sc, ax=ax, label="Δ CES Score (2021 − 2014)", fraction=0.035, pad=0.02)
        ax.set_aspect("auto")

    fig.tight_layout()

    out = fig_dir / "rq1_map_ces_change_points.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq1_map_ces_change_points.csv"
    merged[["census_tract", "latitude", "longitude", "delta_2014_2021"]].to_csv(
        data_out, index=False
    )
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
