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
YEARS = [2014, 2019, 2021]
SLEEP_SEC = 5.0
RETRIES = 3
CACHE_PATH = ROOT / "data" / "aqs_cache.json"
LOCAL_AQS_PATH = ROOT / "data" / "aqs_ca_county_annual.csv"

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

    ces = load_data()
    ces = ces[ces["year"].isin(YEARS)].copy()
    ces["county_norm"] = ces["county"].astype(str).apply(normalize_county)

    ces_mean = (
        ces.groupby(["county_norm", "year"], as_index=False)["ces_score"].mean()
    )

    # Use pre-fetched CSV only; do not hit the API during plotting.
    if not LOCAL_AQS_PATH.exists():
        raise RuntimeError(
            f"Missing {LOCAL_AQS_PATH}. Run scripts/fetch/fetch_aqs_all.py first."
        )
    local = pd.read_csv(LOCAL_AQS_PATH)
    required = {"county_norm", "year", "param", "aqs_mean"}
    if not required.issubset(set(local.columns)):
        raise RuntimeError(
            f"{LOCAL_AQS_PATH} missing required columns: {sorted(required)}"
        )
    aqs_mean = local.copy()

    merged = ces_mean.merge(aqs_mean, on=["county_norm", "year"], how="inner")

    # Plot PM2.5
    fig, axes = plt.subplots(1, len(YEARS), figsize=(14, 5))
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
    fig2, axes2 = plt.subplots(1, len(YEARS), figsize=(14, 5))
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

    # Regression summary
    rows = []
    for year in YEARS:
        for param in [PARAM_PM25, PARAM_OZONE]:
            sub = merged[(merged["year"] == year) & (merged["param"] == param)]
            if len(sub) < 2:
                continue
            x = sub["ces_score"].to_numpy()
            y = sub["aqs_mean"].to_numpy()
            m, b = np.polyfit(x, y, 1)
            y_pred = m * x + b
            ss_res = ((y - y_pred) ** 2).sum()
            ss_tot = ((y - y.mean()) ** 2).sum()
            r2 = 1 - ss_res / ss_tot if ss_tot != 0 else float("nan")
            rows.append({
                "year": year,
                "param": param,
                "slope": m,
                "intercept": b,
                "r2": r2,
                "n": len(sub),
            })
    summary_out = tbl_dir / "rq3_aqs_vs_ces_regression_summary.csv"
    pd.DataFrame(rows).to_csv(summary_out, index=False)
    print(f"Wrote {summary_out}")


if __name__ == "__main__":
    main()
