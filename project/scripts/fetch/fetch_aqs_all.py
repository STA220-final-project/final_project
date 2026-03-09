"""Fetch AQS data needed for all plots (CA counties + populous counties).

Outputs:
  project/data/aqs_ca_county_annual.csv
  project/data/aqs_ca_county_annual_skipped.csv
  project/data/rq4_aqs_populous.csv
  project/data/rq4_aqs_populous_skipped.csv
  project/data/aqs_cache_ca_county.json
  project/data/aqs_cache_populous.json
"""

from __future__ import annotations

import argparse
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

CA_CACHE = ROOT / "data" / "aqs_cache_ca_county.json"
CA_OUT = ROOT / "data" / "aqs_ca_county_annual.csv"
CA_SKIP = ROOT / "data" / "aqs_ca_county_annual_skipped.csv"

POP_CACHE = ROOT / "data" / "aqs_cache_populous.json"
POP_OUT = ROOT / "data" / "rq4_aqs_populous.csv"
POP_SKIP = ROOT / "data" / "rq4_aqs_populous_skipped.csv"

STATE_CA = "06"

# Fallback CA county codes (3-digit) in case list/countiesByState is slow.
CA_COUNTY_CODES = [
    "001","003","005","007","009","011","013","015","017","019","021","023",
    "025","027","029","031","033","035","037","039","041","043","045","047",
    "049","051","053","055","057","059","061","063","065","067","069","071",
    "073","075","077","079","081","083","085","087","089","091","093","095",
    "097","099","101","103","105","107","109","111","113","115"
]

COUNTIES_POPULOUS = [
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


def fetch_json(url: str, params: dict) -> dict:
    import requests
    last_err = None
    for attempt in range(RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=180)
            if resp.status_code == 422:
                return {}
            if resp.status_code == 503:
                time.sleep(5 + attempt * 5)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(3 + attempt * 3)
    raise last_err


def get_body(data: dict) -> list[dict]:
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
    return body if isinstance(body, list) else []


def pick_mean_column(df: pd.DataFrame) -> str:
    candidates = ["arithmetic_mean", "arithmetic mean", "arithmetic_mean_1", "mean"]
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


def fetch_ca_counties(user: str, pw: str) -> list[str]:
    url = f"{BASE_URL}/list/countiesByState"
    params = {"email": user, "key": pw, "state": STATE_CA}
    try:
        data = fetch_json(url, params)
        body = get_body(data)
        if body:
            df = pd.DataFrame(body)
            for c in ["county_code", "code", "county", "county_cd"]:
                if c in df.columns:
                    return df[c].astype(str).str.zfill(3).tolist()
            for c in df.columns:
                if "code" in c.lower():
                    return df[c].astype(str).str.zfill(3).tolist()
    except Exception:
        pass
    return CA_COUNTY_CODES


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
    data = fetch_json(url, params)
    body = get_body(data)
    df = pd.DataFrame(body)
    if "rows" in df.columns and not df.empty and isinstance(df.iloc[0]["rows"], list):
        df = pd.DataFrame(df.iloc[0]["rows"])
    if "status" in df.columns and "rows" in df.columns and len(df.columns) <= 4:
        return pd.DataFrame()
    return df


def fetch_ca_annual(user: str, pw: str, force: bool) -> None:
    if CA_OUT.exists() and not force:
        print(f"[SKIP] CA annual (exists): {CA_OUT}")
        return

    cache = {}
    if CA_CACHE.exists():
        cache = json.loads(CA_CACHE.read_text())

    rows = []
    skipped = []

    county_codes = fetch_ca_counties(user, pw)
    total = len(YEARS) * 2 * len(county_codes)
    done = 0

    for year in YEARS:
        for param in [PARAM_PM25, PARAM_OZONE]:
            for county in county_codes:
                cache_key = f"{year}|{param}|{county}"
                if cache_key in cache:
                    data = cache[cache_key]
                    df = pd.DataFrame(data)
                else:
                    done += 1
                    label = "PM2.5" if param == PARAM_PM25 else "Ozone"
                    print(f"[{done}/{total}] CA {label} {year} county={county}...")
                    df = fetch_annual_by_county(user, pw, param, year, STATE_CA, county)
                    if not df.empty and "status" not in df.columns:
                        cache[cache_key] = df.to_dict(orient="records")
                        CA_CACHE.write_text(json.dumps(cache))
                    time.sleep(SLEEP_SEC)

                if df.empty:
                    skipped.append({
                        "year": year,
                        "param": param,
                        "state": STATE_CA,
                        "county_fips": county,
                        "reason": "no_data_or_unavailable",
                    })
                    continue

                mean_col = pick_mean_column(df)
                val = pd.to_numeric(df[mean_col], errors="coerce").dropna()
                if val.empty:
                    skipped.append({
                        "year": year,
                        "param": param,
                        "state": STATE_CA,
                        "county_fips": county,
                        "reason": "missing_mean",
                    })
                    continue

                county_name = None
                for c in ["county_name", "county", "county name"]:
                    if c in df.columns:
                        county_name = df[c].iloc[0]
                        break

                rows.append(
                    {
                        "county": county_name,
                        "county_norm": normalize_county(county_name) if county_name else "",
                        "state": STATE_CA,
                        "county_fips": county,
                        "year": year,
                        "param": param,
                        "aqs_mean": float(val.mean()),
                    }
                )

    out_df = pd.DataFrame(rows)
    out_df.to_csv(CA_OUT, index=False)
    print(f"Wrote {CA_OUT}")

    skipped_df = pd.DataFrame(skipped)
    skipped_df.to_csv(CA_SKIP, index=False)
    print(f"Wrote {CA_SKIP}")


def fetch_populous(user: str, pw: str, force: bool) -> None:
    if POP_OUT.exists() and not force:
        print(f"[SKIP] Populous counties (exists): {POP_OUT}")
        return

    cache = {}
    if POP_CACHE.exists():
        cache = json.loads(POP_CACHE.read_text())

    rows = []
    skipped = []

    total = len(YEARS) * 2 * len(COUNTIES_POPULOUS)
    done = 0
    for year in YEARS:
        for param in [PARAM_PM25, PARAM_OZONE]:
            for c in COUNTIES_POPULOUS:
                cache_key = f"{year}|{param}|{c['state']}|{c['county']}"
                if cache_key in cache:
                    data = cache[cache_key]
                    df = pd.DataFrame(data)
                else:
                    done += 1
                    label = "PM2.5" if param == PARAM_PM25 else "Ozone"
                    print(f"[{done}/{total}] Pop {label} {year} {c['name']}...")
                    df = fetch_annual_by_county(user, pw, param, year, c["state"], c["county"])
                    if not df.empty and "status" not in df.columns:
                        cache[cache_key] = df.to_dict(orient="records")
                        POP_CACHE.write_text(json.dumps(cache))
                    time.sleep(SLEEP_SEC)

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
    out_df.to_csv(POP_OUT, index=False)
    print(f"Wrote {POP_OUT}")

    skipped_df = pd.DataFrame(skipped)
    skipped_df.to_csv(POP_SKIP, index=False)
    print(f"Wrote {POP_SKIP}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Refetch even if CSVs exist.")
    parser.add_argument("--only", choices=["ca", "populous"], help="Fetch only one dataset.")
    args = parser.parse_args()

    user = require_env("AQS_USER")
    pw = require_env("AQS_PW")

    if args.only == "ca":
        fetch_ca_annual(user, pw, args.force)
    elif args.only == "populous":
        fetch_populous(user, pw, args.force)
    else:
        fetch_ca_annual(user, pw, args.force)
        fetch_populous(user, pw, args.force)


if __name__ == "__main__":
    main()
