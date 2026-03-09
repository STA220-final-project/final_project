"""RQ1: Tract polygon map of CES score change (2014→2021)."""

import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, try_import_basemap


def normalize_geoid(s: str) -> str:
    s = str(s).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(11)


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

    merged = wide.reset_index().copy()
    merged["geoid"] = merged["census_tract"].apply(normalize_geoid)

    gpd, ctx = try_import_basemap()
    tract_shp = ROOT / "data" / "tracts" / "tl_2021_06_tract.shp"
    if not gpd or not tract_shp.exists():
        raise FileNotFoundError("Missing geopandas or tract shapefile.")

    tracts = gpd.read_file(tract_shp)
    tracts["geoid"] = tracts["GEOID"].astype(str).apply(normalize_geoid)

    chor = tracts.merge(merged[["geoid", "delta_2014_2021"]], on="geoid", how="left")
    chor = chor.to_crs(epsg=3857)

    fig, ax = plt.subplots(figsize=(9, 10))
    chor.plot(
        ax=ax,
        column="delta_2014_2021",
        cmap="RdBu_r",
        linewidth=0,
        alpha=0.85,
        legend=True,
        legend_kwds={"label": "Δ CES Score (2021 − 2014)"},
    )
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, alpha=0.5)
    ax.set_axis_off()
    ax.set_title("Tract-Level CES Change (2014→2021)")
    fig.tight_layout()

    out = fig_dir / "rq1_map_ces_change_tracts.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Wrote {out}")

    data_out = tbl_dir / "rq1_map_ces_change_tracts.csv"
    merged[["geoid", "delta_2014_2021"]].to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
