"""Quick AQS probe: test a single county/year/param request.

Usage:
  python scripts/fetch/aqs_probe.py --param 88101 --year 2014 --state 06 --county 037
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
BASE_URLS = [
    "https://aqs.epa.gov/data/api",
    "https://aqs.epa.gov/api",
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--param", default="88101", help="AQS parameter code (e.g., 88101 PM2.5, 44201 Ozone)")
    parser.add_argument("--year", type=int, default=2014)
    parser.add_argument("--state", default="06")
    parser.add_argument("--county", default="037")
    parser.add_argument("--timeout", type=int, default=60, help="Read timeout seconds")
    parser.add_argument("--retries", type=int, default=3, help="Retry attempts on timeout")
    args = parser.parse_args()

    user = require_env("AQS_USER")
    pw = require_env("AQS_PW")

    params = {
        "email": user,
        "key": pw,
        "param": args.param,
        "bdate": f"{args.year}0101",
        "edate": f"{args.year}1231",
        "state": args.state,
        "county": args.county,
    }

    last_err = None
    for base in BASE_URLS:
        print(f"[INFO] Trying base: {base}")
        # Quick health checks
        for svc in ["metaData/isAvailable", "serviceAvailable", "list/states"]:
            url = f"{base}/{svc}"
            try:
                resp = requests.get(url, params={"email": user, "key": pw}, timeout=args.timeout)
                print(f"[CHECK] {svc} -> {resp.status_code}")
            except requests.exceptions.ReadTimeout:
                print(f"[CHECK] {svc} -> timeout")

        url = f"{base}/annualData/byCounty"
        for attempt in range(1, args.retries + 1):
            try:
                print(f"[REQ] {url}")
                resp = requests.get(url, params=params, timeout=args.timeout)
                print("status", resp.status_code)
                print(resp.text[:500])
                if resp.status_code == 422:
                    # Wrong base URL or service not found; try next base.
                    break
                return
            except requests.exceptions.ReadTimeout as e:
                last_err = e
                print(f"[WARN] timeout (attempt {attempt}/{args.retries})")
    raise last_err


if __name__ == "__main__":
    main()
