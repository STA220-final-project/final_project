"""Shared helpers for CES plots."""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import json
import math

ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "ces_standardized.csv"
OUT_DIR = ROOT / "output"

POLLUTION_PCTL_COLS = [
    "ozone_pctl",
    "pm25_pctl",
    "diesel_pm_pctl",
    "drinking_water_pctl",
    "pesticides_pctl",
    "tox_release_pctl",
    "traffic_pctl",
    "cleanup_sites_pctl",
    "groundwater_threats_pctl",
    "haz_waste_pctl",
    "imp_water_bodies_pctl",
    "solid_waste_pctl",
]


def load_data() -> pd.DataFrame:
    # Keep this simple so each question script can just call load_data().
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Missing {DATA_PATH}. Run scripts/standardize_ces.py first."
        )
    return pd.read_csv(DATA_PATH)


def ensure_out_dir(*parts: str) -> Path:
    """Ensure an output subdirectory exists and return its Path."""
    path = OUT_DIR.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def county_centroids(df: pd.DataFrame) -> pd.DataFrame:
    """Compute county centroids from tract lat/lon (population-weighted if available)."""
    cols = ["county", "latitude", "longitude", "total_population"]
    for c in ["county", "latitude", "longitude"]:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")

    work = df[[c for c in cols if c in df.columns]].copy()
    work = work.dropna(subset=["county", "latitude", "longitude"])
    work["latitude"] = pd.to_numeric(work["latitude"], errors="coerce")
    work["longitude"] = pd.to_numeric(work["longitude"], errors="coerce")
    work = work.dropna(subset=["latitude", "longitude"])

    if "total_population" in work.columns:
        work["total_population"] = pd.to_numeric(work["total_population"], errors="coerce").fillna(0)
        work["lat_w"] = work["latitude"] * work["total_population"]
        work["lon_w"] = work["longitude"] * work["total_population"]
        grouped = work.groupby("county", as_index=False).agg(
            pop_sum=("total_population", "sum"),
            lat_sum=("lat_w", "sum"),
            lon_sum=("lon_w", "sum"),
        )
        grouped["latitude"] = grouped["lat_sum"] / grouped["pop_sum"].replace(0, 1)
        grouped["longitude"] = grouped["lon_sum"] / grouped["pop_sum"].replace(0, 1)
        return grouped[["county", "latitude", "longitude"]]

    return (
        work.groupby("county", as_index=False)
        .agg(latitude=("latitude", "mean"), longitude=("longitude", "mean"))
    )


def north_south_split(lat: float) -> str:
    """Rough split of California into North/South by latitude."""
    try:
        return "North" if float(lat) >= 37.0 else "South"
    except Exception:
        return "Unknown"


def add_missing_ces_percentile(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing CES percentiles by computing within-year ranks from CES score."""
    if "ces_percentile" not in df.columns or "ces_score" not in df.columns:
        return df
    out = df.copy()
    for year, sub in out.groupby("year"):
        if sub["ces_percentile"].notna().sum() == 0:
            scores = pd.to_numeric(sub["ces_score"], errors="coerce")
            pct = scores.rank(pct=True) * 100
            out.loc[sub.index, "ces_percentile"] = pct
    return out


def plot_geojson_outline(ax, geojson_path: Path, color: str = "#333333", linewidth: float = 0.6) -> None:
    """Plot GeoJSON polygon/multipolygon outlines on a matplotlib axis."""
    if not geojson_path.exists():
        return
    data = json.loads(geojson_path.read_text())
    features = data.get("features", [])

    # Sanity check coordinates are lon/lat (EPSG:4326). If not, skip with warning.
    sample_vals = []
    for feat in features[:5]:
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [])
        if geom.get("type") == "Polygon" and coords:
            sample_vals.extend(coords[0][:3])
        elif geom.get("type") == "MultiPolygon" and coords:
            sample_vals.extend(coords[0][0][:3])
    if sample_vals:
        xs = [pt[0] for pt in sample_vals if isinstance(pt, (list, tuple)) and len(pt) >= 2]
        ys = [pt[1] for pt in sample_vals if isinstance(pt, (list, tuple)) and len(pt) >= 2]
        if xs and ys:
            if max(map(abs, xs)) > 200 or max(map(abs, ys)) > 100:
                print("[WARN] GeoJSON appears projected (not lon/lat). Skipping outline.")
                return

    def plot_coords(coords):
        xs = [pt[0] for pt in coords]
        ys = [pt[1] for pt in coords]
        ax.plot(xs, ys, color=color, linewidth=linewidth, alpha=0.8)

    for feat in features:
        geom = feat.get("geometry", {})
        gtype = geom.get("type")
        coords = geom.get("coordinates", [])
        if gtype == "Polygon":
            # coords: [ring1, ring2, ...]
            for ring in coords:
                plot_coords(ring)
        elif gtype == "MultiPolygon":
            # coords: [[ring1, ring2], [ring1, ring2], ...]
            for poly in coords:
                for ring in poly:
                    plot_coords(ring)


def try_import_basemap():
    try:
        import geopandas as gpd  # type: ignore
        import contextily as ctx  # type: ignore
        return gpd, ctx
    except Exception:
        return None, None
