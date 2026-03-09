"""Fetch AQS PM2.5/Ozone for 10 most populous counties (2014/2019/2021).

Outputs:
  project/data/rq4_aqs_populous.csv
  project/data/rq4_aqs_populous_skipped.csv
  project/data/aqs_cache_populous.json
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

BASE_URL = "https://aqs.epa.gov/data/api"
PARAM_PM25 = "88101"
PARAM_OZONE = "44201"
YEARS = [2014, 2019, 2021]
SLEEP_SEC = 3.0
RETRIES = 4
CACHE_PATH = ROOT / "data" / "aqs_cache_populous.json"
OUT_CSV = ROOT / "data" / "rq4_aqs_populous.csv"
SKIP_CSV = ROOT / "data" / "rq4_aqs_populous_skipped.csv"

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
            if resp.status_code == 422:
                return pd.DataFrame()
            if resp.status_code == 503:
                time.sleep(5 + attempt * 5)
                continue
            resp.raise_for_status()
            data = resp.json()
            body = (
                data.get("rows")
                or data.get("Rows")
                or data.get("Data")
                or data.get("data")
                or data.get("Body")
                or data.get("body")
                or []
            )
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
            if "rows" in df.columns and isinstance(df.iloc[0]["rows"], list):
                df = pd.DataFrame(df.iloc[0]["rows"])
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
    user = require_env("AQS_USER")
    pw = require_env("AQS_PW")

    cache = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text())

    rows = []
    skipped = []

    total = len(YEARS) * 2 * len(COUNTIES)
    done = 0
    for year in YEARS:
        for param in [PARAM_PM25, PARAM_OZONE]:
            for c in COUNTIES:
                cache_key = f"{year}|{param}|{c['state']}|{c['county']}"
                if cache_key in cache:
                    data = cache[cache_key]
                    df = pd.DataFrame(data)
                else:
                    done += 1
                    label = "PM2.5" if param == PARAM_PM25 else "Ozone"
                    print(f"[{done}/{total}] Fetching {label} {year} {c['name']} (state={c['state']} county={c['county']})...")
                    df = fetch_annual_by_county(user, pw, param, year, c["state"], c["county"])
                    if not df.empty and "status" not in df.columns:
                        cache[cache_key] = df.to_dict(orient="records")
                        CACHE_PATH.write_text(json.dumps(cache))
                    time.sleep(SLEEP_SEC)
                if cache_key in cache:
                    done = max(done, done)

                if df.empty:
                    skipped.append({
                        "year": year,
                        "param": param,
                        "state": c["state"],
                        "county_fips": c["county"],
                        "county": c["name"],
                        "reason": "no_data_or_unavailable",
                    })
                    continue

                mean_col = pick_mean_column(df)
                val = pd.to_numeric(df[mean_col], errors="coerce").dropna()
                if val.empty:
                    skipped.append({
                        "year": year,
                        "param": param,
                        "state": c["state"],
                        "county_fips": c["county"],
                        "county": c["name"],
                        "reason": "missing_mean",
                    })
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
    out_df.to_csv(OUT_CSV, index=False)
    print(f"Wrote {OUT_CSV}")

    skipped_df = pd.DataFrame(skipped)
    skipped_df.to_csv(SKIP_CSV, index=False)
    print(f"Wrote {SKIP_CSV}")


if __name__ == "__main__":
    main()
