"""AQS API + CES: county-level scatter of CES vs measured PM2.5 / Ozone.

Requires AQS API credentials in env vars:
  AQS_USER and AQS_PW

Outputs:
  project/output/qx_plot_aqs_vs_ces_pm25.png
  project/output/qx_plot_aqs_vs_ces_ozone.png
"""

import os
import time
import json
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import load_data, ensure_out_dir, OUT_DIR

BASE_URL = "https://aqs.epa.gov/data/api"
STATE_FIPS = "06"  # California
PARAM_PM25 = "88101"
PARAM_OZONE = "44201"
YEARS = [2019, 2021]
SLEEP_SEC = 5.0
RETRIES = 3
CACHE_PATH = ROOT / "data" / "aqs_cache.json"

# Fallback CA county codes (FIPS, 3-digit) in case list/countiesByState is slow.
CA_COUNTY_CODES = [
    "001","003","005","007","009","011","013","015","017","019","021","023",
    "025","027","029","031","033","035","037","039","041","043","045","047",
    "049","051","053","055","057","059","061","063","065","067","069","071",
    "073","075","077","079","081","083","085","087","089","091","093","095",
    "097","099","101","103","105","107","109","111","113","115"
]


def load_env_file() -> None:
    # Simple .env loader to avoid extra dependencies.
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value and key not in os.environ:
            os.environ[key] = value


def require_env(name: str) -> str:
    load_env_file()
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}. Add it to project/.env or export it.")
    return val


def fetch_counties(user: str, pw: str) -> pd.DataFrame:
    url = f"{BASE_URL}/list/countiesByState"
    params = {
        "email": user,
        "key": pw,
        "state": STATE_FIPS,
    }
    import requests
    last_err = None
    for attempt in range(RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=180)
            resp.raise_for_status()
            data = resp.json()
            header = data.get("Header", [])
            body = (
                data.get("Body")
                or data.get("body")
                or data.get("Data")
                or data.get("data")
                or []
            )
            if not body:
                for v in data.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        body = v
                        break
            if not body:
                raise ValueError(f"AQS counties response empty. Header: {header}")
            return pd.DataFrame(body)
        except Exception as e:
            last_err = e
            time.sleep(5 + attempt * 5)
    # If we still fail, use fallback county list.
    print(f"[WARN] countiesByState failed, using fallback CA county codes. Error: {last_err}")
    return pd.DataFrame({"code": CA_COUNTY_CODES})


def fetch_annual_by_county(user: str, pw: str, param: str, year: int, county: str) -> pd.DataFrame:
    # Smaller queries to avoid timeouts.
    url = f"{BASE_URL}/annualData/byCounty"
    params = {
        "email": user,
        "key": pw,
        "param": param,
        "bdate": f"{year}0101",
        "edate": f"{year}1231",
        "state": STATE_FIPS,
        "county": county,
    }
    import requests
    last_err = None
    for attempt in range(RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=180)
            resp.raise_for_status()
            data = resp.json()
            body = (
                data.get("Body")
                or data.get("body")
                or data.get("Data")
                or data.get("data")
                or []
            )
            if not body:
                for v in data.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        body = v
                        break
            return pd.DataFrame(body)
        except Exception as e:
            last_err = e
            time.sleep(5 + attempt * 5)
    raise last_err


def pick_mean_column(df: pd.DataFrame) -> str:
    # Try common column names for annual summaries.
    candidates = [
        "arithmetic_mean",
        "arithmetic mean",
        "arithmetic_mean_1",
        "mean",
    ]
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand in lower_map:
            return lower_map[cand]
    raise ValueError(f"Could not find mean column in AQS data. Columns: {list(df.columns)[:20]}")


def normalize_county(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.lower().strip()
    s = s.replace("county", "").strip()
    s = " ".join(s.split())
    return s


def main() -> None:
    fig_dir = ensure_out_dir("rq3_aqs_validation", "figures")
    tbl_dir = ensure_out_dir("rq3_aqs_validation", "tables")
    user = require_env("AQS_USER")
    pw = require_env("AQS_PW")

    ces = load_data()
    ces = ces[ces["year"].isin(YEARS)].copy()
    ces["county_norm"] = ces["county"].astype(str).apply(normalize_county)

    ces_mean = (
        ces.groupby(["county_norm", "year"], as_index=False)["ces_score"].mean()
    )

    # Fetch AQS data
    counties_df = fetch_counties(user, pw)
    # AQS list service usually returns 'code' + 'name' for counties.
    county_code_col = None
    for c in ["county_code", "code", "county", "county_cd"]:
        if c in counties_df.columns:
            county_code_col = c
            break
    if county_code_col is None:
        # fallback: any column containing 'code'
        for c in counties_df.columns:
            if "code" in c.lower():
                county_code_col = c
                break
    if county_code_col is None:
        raise ValueError(f"AQS counties list missing code column. Columns: {list(counties_df.columns)}")

    county_codes = counties_df[county_code_col].astype(str).str.zfill(3).tolist()

    # Cache to avoid losing progress if the API drops the connection.
    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text())

    aqs_frames = []
    for year in YEARS:
        for param in [PARAM_PM25, PARAM_OZONE]:
            for county in county_codes:
                print(f"Fetching year={year} param={param} county={county}...")
                cache_key = f"{year}|{param}|{county}"
                if cache_key in cache:
                    rows = cache[cache_key]
                    if rows:
                        df = pd.DataFrame(rows)
                        df["year"] = year
                        df["param"] = param
                        aqs_frames.append(df)
                    time.sleep(SLEEP_SEC)
                    continue

                df = fetch_annual_by_county(user, pw, param, year, county)
                rows = df.to_dict(orient="records") if not df.empty else []
                cache[cache_key] = rows
                CACHE_PATH.write_text(json.dumps(cache))
                if rows:
                    df["year"] = year
                    df["param"] = param
                    aqs_frames.append(df)
                time.sleep(SLEEP_SEC)

    if not aqs_frames:
        raise ValueError("No AQS data returned. Check credentials or parameters.")

    aqs = pd.concat(aqs_frames, ignore_index=True)

    # Normalize county name in AQS (try common columns)
    county_col = None
    for c in ["county_name", "county", "county name"]:
        if c in aqs.columns:
            county_col = c
            break
    if county_col is None:
        # fallback: try any column containing 'county'
        for c in aqs.columns:
            if "county" in c.lower():
                county_col = c
                break
    if county_col is None:
        raise ValueError("Could not find county column in AQS data.")

    aqs["county_norm"] = aqs[county_col].astype(str).apply(normalize_county)

    mean_col = pick_mean_column(aqs)

    # Aggregate to county-level mean (average across sites in county)
    aqs_mean = (
        aqs.groupby(["county_norm", "year", "param"], as_index=False)[mean_col]
        .mean()
        .rename(columns={mean_col: "aqs_mean"})
    )

    merged = ces_mean.merge(aqs_mean, on=["county_norm", "year"], how="inner")

    # Plot PM2.5
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for i, year in enumerate(YEARS):
        sub = merged[(merged["year"] == year) & (merged["param"] == PARAM_PM25)]
        axes[i].scatter(sub["ces_score"], sub["aqs_mean"], s=18, alpha=0.7)
        if len(sub) > 1:
            m, b = np.polyfit(sub["ces_score"], sub["aqs_mean"], 1)
            xs = np.linspace(sub["ces_score"].min(), sub["ces_score"].max(), 100)
            axes[i].plot(xs, m * xs + b, color="#d62728", linewidth=2)
        axes[i].set_title(f"PM2.5 vs CES (Year {year})")
        axes[i].set_xlabel("County Mean CES Score")
        axes[i].set_ylabel("AQS Annual Mean PM2.5")
    fig.tight_layout()
    out_pm = fig_dir / "rq3_aqs_pm25_vs_ces.png"
    fig.savefig(out_pm, dpi=150)

    # Plot Ozone
    fig2, axes2 = plt.subplots(1, 2, figsize=(12, 5))
    for i, year in enumerate(YEARS):
        sub = merged[(merged["year"] == year) & (merged["param"] == PARAM_OZONE)]
        axes2[i].scatter(sub["ces_score"], sub["aqs_mean"], s=18, alpha=0.7)
        if len(sub) > 1:
            m, b = np.polyfit(sub["ces_score"], sub["aqs_mean"], 1)
            xs = np.linspace(sub["ces_score"].min(), sub["ces_score"].max(), 100)
            axes2[i].plot(xs, m * xs + b, color="#d62728", linewidth=2)
        axes2[i].set_title(f"Ozone vs CES (Year {year})")
        axes2[i].set_xlabel("County Mean CES Score")
        axes2[i].set_ylabel("AQS Annual Mean Ozone")
    fig2.tight_layout()
    out_oz = fig_dir / "rq3_aqs_ozone_vs_ces.png"
    fig2.savefig(out_oz, dpi=150)

    print(f"Wrote {out_pm}")
    print(f"Wrote {out_oz}")

    data_out = tbl_dir / "rq3_aqs_vs_ces_scatter.csv"
    merged.to_csv(data_out, index=False)
    print(f"Wrote {data_out}")


if __name__ == "__main__":
    main()
