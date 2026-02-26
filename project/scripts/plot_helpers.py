"""Shared helpers for CES plots."""

from __future__ import annotations

from pathlib import Path
import pandas as pd

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


def ensure_out_dir() -> None:
    # Make sure output folder exists before saving plots.
    OUT_DIR.mkdir(parents=True, exist_ok=True)
