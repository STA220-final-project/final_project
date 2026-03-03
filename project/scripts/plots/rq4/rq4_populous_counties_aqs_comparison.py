"""RQ4: Compare AQS PM2.5 and Ozone in the 10 most populous counties.

Requires AQS API credentials in env vars:
  AQS_USER and AQS_PW
"""

import os
import json
import time
from pathlib import Path
import sys

import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT / "scripts"))
from plot_helpers import ensure_out_dir

BASE_URL = "https://aqs.epa.gov/data/api"
PARAM_PM25 = "88101"
PARAM_OZONE = "44201"
YEARS = [2019, 2021]
SLEEP_SEC = 3.0
RETRIES = 3
CACHE_PATH = ROOT / "data" / "aqs_cache_populous.json"

# 10 most populous counties (approx list provided)
COUNTIES = [
    {"name": "Los Angeles, CA", "state": "06", "county": "037", "is_ca": True},
    {"name": "Cook, IL", "state": "17", "county": "031", "is_ca": False},
    {"name": "Harris, TX", "state": "48", "county": "201", "is_ca": False},
    {"name": "Maricopa, AZ", "state": "04", "county": "013", "is_ca": False},
    {"name": "San Diego, CA", "state": "06", "county": "073", "is_ca": True},
    {"name": "Orange, CA", "state": "06", "county": "059", "is_ca": True},
    {"name": "Miami-Dade, FL", "state": "12", "county": "086", "is_ca": False},
    {"name": "Dallas, TX", "state": "48", "county": "113", "is_ca": False},
    {"name": "Riverside, CA", "state": "06", "county": "065", "is_ca": True},
    {"name": "Kings, NY", "state": "36", "county": "047", "is_ca": False},
]


def load_env_file() -> None:
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


def fetch_annual_by_county(user: str, pw: str, param: str, year: int, state: str, county: str) -> pd.DataFrame:
    url = f"{BASE_URL}/annualData/byCounty"
    params = {
        "email": user,
        "key": pw,
        "param": param,
        "bdate": f"{year}0101",
        "edate": f"{year}1231",
        "state": state,
        "county": county,
    }
    import requests
    last_err = None
    for attempt in range(RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=180)
            resp.raise_for_status()
            data = resp.json()
            # AQS sometimes returns the payload under 'Data' or 'Body' or 'rows'
            body = (
                data.get("rows")
                or data.get("Rows")
                or data.get("Data")
                or data.get("data")
                or data.get("Body")
                or data.get("body")
                or []
            )
            # If we got the full response dict (status/request_time/rows), unwrap rows
            if isinstance(body, dict) and body:
                if "rows" in body and isinstance(body["rows"], list):
                    body = body["rows"]
                else:
                    body = [body]
            if not body:
                for v in data.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        body = v
                        break
            df = pd.DataFrame(body)
            # Some AQS responses use lowercase keys or wrap rows under 'rows'
            if "rows" in df.columns and isinstance(df.iloc[0]["rows"], list):
                df = pd.DataFrame(df.iloc[0]["rows"])
            # If response is metadata-only (status/request_time/rows), treat as empty
            if "status" in df.columns and "rows" in df.columns and len(df.columns) <= 4:
                return pd.DataFrame()
            return df
        except Exception as e:
            last_err = e
            time.sleep(3 + attempt * 3)
    raise last_err


def pick_mean_column(df: pd.DataFrame) -> str:
    candidates = [
        "arithmetic_mean",
        "arithmetic mean",
        "arithmetic_mean_1",
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


def main() -> None:
    fig_dir = ensure_out_dir("rq4_populous_comparison", "figures")
    tbl_dir = ensure_out_dir("rq4_populous_comparison", "tables")

    user = require_env("AQS_USER")
    pw = require_env("AQS_PW")

    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text())

    # If cache contains non-data entries, reset it
    if cache:
        sample = next(iter(cache.values()))
        if isinstance(sample, list) and sample and isinstance(sample[0], dict):
            if "status" in sample[0]:
                cache = {}
        elif isinstance(sample, dict) and "status" in sample:
            cache = {}

    rows = []
    for year in YEARS:
        for param in [PARAM_PM25, PARAM_OZONE]:
            for c in COUNTIES:
                cache_key = f"{year}|{param}|{c['state']}|{c['county']}"
                if cache_key in cache:
                    data = cache[cache_key]
                    df = pd.DataFrame(data)
                else:
                    df = fetch_annual_by_county(user, pw, param, year, c["state"], c["county"])
                    # Only cache if we got actual data columns
                    if not df.empty and "status" not in df.columns:
                        cache[cache_key] = df.to_dict(orient="records")
                        CACHE_PATH.write_text(json.dumps(cache))
                    time.sleep(SLEEP_SEC)

                if df.empty:
                    continue
                mean_col = pick_mean_column(df)
                val = pd.to_numeric(df[mean_col], errors="coerce").dropna()
                if val.empty:
                    continue
                rows.append(
                    {
                        "county": c["name"],
                        "state": c["state"],
                        "county_fips": c["county"],
                        "is_ca": c["is_ca"],
                        "year": year,
                        "param": param,
                        "aqs_mean": float(val.mean()),
                    }
                )

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        raise ValueError("No AQS data returned for the populous counties list. Check FIPS codes or API credentials.")
    out_csv = tbl_dir / "rq4_populous_counties_aqs.csv"
    out_df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")

    # Plot: 2x2 grid (PM2.5/Ozone x 2019/2021)
    fig, axes = plt.subplots(2, 2, figsize=(12, 9), sharex=False)
    params = [PARAM_PM25, PARAM_OZONE]
    for i, param in enumerate(params):
        for j, year in enumerate(YEARS):
            ax = axes[i, j]
            sub = out_df[(out_df["param"] == param) & (out_df["year"] == year)].copy()
            if sub.empty:
                ax.set_visible(False)
                continue
            # Order by value for readability
            sub = sub.sort_values("aqs_mean", ascending=False)
            colors = sub["is_ca"].map(lambda x: "#1f77b4" if x else "#ff7f0e")
            ax.barh(sub["county"], sub["aqs_mean"], color=colors)
            label = "PM2.5" if param == PARAM_PM25 else "Ozone"
            ax.set_title(f"{label} ({year})")
            ax.invert_yaxis()
            ax.set_xlabel("AQS Annual Mean")
            # Set x-limits per panel so ozone is visible on its own scale
            if not sub["aqs_mean"].empty:
                xmax = sub["aqs_mean"].max() * 1.15
                ax.set_xlim(0, xmax)

    fig.suptitle("AQS Air Quality in the 10 Most Populous Counties", y=1.02)
    fig.tight_layout()
    out_fig = fig_dir / "rq4_populous_counties_aqs.png"
    fig.savefig(out_fig, dpi=150, bbox_inches="tight")
    print(f"Wrote {out_fig}")


if __name__ == "__main__":
    main()
