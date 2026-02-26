"""Standardize CES 2.0/3.0/4.0 Excel files into a single tidy CSV.

Output: project/data/ces_standardized.csv
"""

from __future__ import annotations

import csv
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_PATH = DATA_DIR / "ces_standardized.csv"

INPUTS = {
    2014: DATA_DIR / "ces-2-2014.xlsx",
    2019: DATA_DIR / "ces-3-2019.xlsx",
    2021: DATA_DIR / "ces-4-2021.xlsx",
}


def _col_to_index(col: str) -> int:
    # Excel columns are letters (A, B, ..., AA). Convert to index.
    col = col.upper()
    idx = 0
    for c in col:
        if not ("A" <= c <= "Z"):
            break
        idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


def _read_shared_strings(z: zipfile.ZipFile) -> List[str]:
    # XLSX stores strings in a shared table, so we need to load that first.
    try:
        data = z.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(data)
    ns = {"s": root.tag.split("}")[0].strip("{")}
    strings = []
    for si in root.findall("s:si", ns):
        texts = [t.text or "" for t in si.findall(".//s:t", ns)]
        strings.append("".join(texts))
    return strings


def _first_sheet_path(z: zipfile.ZipFile) -> str:
    # We only care about the first sheet (matches how the CES files are formatted).
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    ns = {"s": wb.tag.split("}")[0].strip("{")}
    sheet = wb.find("s:sheets/s:sheet", ns)
    rid = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")

    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    rns = {"r": rels.tag.split("}")[0].strip("{")}
    for rel in rels.findall("r:Relationship", rns):
        if rel.attrib.get("Id") == rid:
            target = rel.attrib["Target"].lstrip("/")
            return f"xl/{target}"

    return "xl/worksheets/sheet1.xml"


def _parse_sheet(z: zipfile.ZipFile) -> List[List[object]]:
    # Minimal XLSX parser so we don't need extra install steps like openpyxl.
    shared = _read_shared_strings(z)
    sheet_path = _first_sheet_path(z)
    root = ET.fromstring(z.read(sheet_path))
    ns = {"s": root.tag.split("}")[0].strip("{")}

    rows: List[List[object]] = []
    max_col = 0

    for row in root.findall("s:sheetData/s:row", ns):
        row_vals: Dict[int, object] = {}
        for c in row.findall("s:c", ns):
            ref = c.attrib.get("r", "")
            col_letters = re.match(r"[A-Z]+", ref)
            if not col_letters:
                continue
            col_idx = _col_to_index(col_letters.group(0))
            max_col = max(max_col, col_idx)

            cell_type = c.attrib.get("t")
            v = c.find("s:v", ns)
            if cell_type == "inlineStr":
                is_node = c.find("s:is/s:t", ns)
                value = is_node.text if is_node is not None else ""
            elif v is None:
                value = ""
            elif cell_type == "s":
                s_idx = int(v.text)
                value = shared[s_idx] if s_idx < len(shared) else ""
            elif cell_type == "b":
                value = bool(int(v.text))
            else:
                value = v.text

            row_vals[col_idx] = value

        if not row_vals:
            continue
        row_list = ["" for _ in range(max_col + 1)]
        for idx, value in row_vals.items():
            row_list[idx] = value
        rows.append(row_list)

    return rows


def read_xlsx_first_sheet(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        rows = _parse_sheet(z)

    if not rows:
        return pd.DataFrame()

    header = rows[0]
    data = rows[1:]
    # Trim trailing empty columns that sometimes show up in Excel exports.
    while header and (header[-1] == "" or header[-1] is None):
        header = header[:-1]
    clean_data = [row[: len(header)] for row in data]
    return pd.DataFrame(clean_data, columns=header)


def _norm_col(col: str) -> str:
    # Normalize column names so mapping is consistent across CES versions.
    col = col.strip().lower()
    col = col.replace("\n", " ")
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


CANON_MAP = {
    "census_tract": "census_tract",
    "total_population": "total_population",
    "california_county": "county",
    "zip": "zip",
    "city": "approx_location",
    "nearby_city_to_help_approximate_location_only": "approx_location",
    "approximate_location": "approx_location",
    "longitude": "longitude",
    "latitude": "latitude",
    "ces_2_0_score": "ces_score",
    "ces_3_0_score": "ces_score",
    "ces_4_0_score": "ces_score",
    "ces_3_0_percentile": "ces_percentile",
    "ces_4_0_percentile": "ces_percentile",
    "ces_2_0_percentile_range": "ces_percentile_range",
    "ces_3_0_percentile_range": "ces_percentile_range",
    "ces_4_0_percentile_range": "ces_percentile_range",
    "ozone": "ozone",
    "ozone_pctl": "ozone_pctl",
    "pm2_5": "pm25",
    "pm2_5_pctl": "pm25_pctl",
    "diesel_pm": "diesel_pm",
    "diesel_pm_pctl": "diesel_pm_pctl",
    "drinking_water": "drinking_water",
    "drinking_water_pctl": "drinking_water_pctl",
    "lead": "lead",
    "lead_pctl": "lead_pctl",
    "pesticides": "pesticides",
    "pesticides_pctl": "pesticides_pctl",
    "tox_release": "tox_release",
    "tox_release_pctl": "tox_release_pctl",
    "traffic": "traffic",
    "traffic_pctl": "traffic_pctl",
    "cleanup_sites": "cleanup_sites",
    "cleanup_sites_pctl": "cleanup_sites_pctl",
    "groundwater_threats": "groundwater_threats",
    "groundwater_threats_pctl": "groundwater_threats_pctl",
    "haz_waste": "haz_waste",
    "haz_waste_pctl": "haz_waste_pctl",
    "imp_water_bodies": "imp_water_bodies",
    "imp_water_bodies_pctl": "imp_water_bodies_pctl",
    "solid_waste": "solid_waste",
    "solid_waste_pctl": "solid_waste_pctl",
    "pollution_burden": "pollution_burden",
    "pollution_burden_score": "pollution_burden_score",
    "pollution_burden_pctl": "pollution_burden_pctl",
    "asthma": "asthma",
    "asthma_pctl": "asthma_pctl",
    "low_birth_weight": "low_birth_weight",
    "low_birth_weight_pctl": "low_birth_weight_pctl",
    "cardiovascular_disease": "cardiovascular_disease",
    "cardiovascular_disease_pctl": "cardiovascular_disease_pctl",
    "education": "education",
    "education_pctl": "education_pctl",
    "linguistic_isolation": "linguistic_isolation",
    "linguistic_isolation_pctl": "linguistic_isolation_pctl",
    "poverty": "poverty",
    "poverty_pctl": "poverty_pctl",
    "unemployment": "unemployment",
    "unemployment_pctl": "unemployment_pctl",
    "housing_burden": "housing_burden",
    "housing_burden_pctl": "housing_burden_pctl",
    "pop_char": "pop_char",
    "pop_char_score": "pop_char_score",
    "pop_char_pctl": "pop_char_pctl",
}

DROP_COLS = {
    "click_for_interactive_map",
    "hyperlink",
    "sb_535_disadvantaged_community",
}


def normalize_tract(x) -> str:
    # Census tract IDs sometimes come in as floats or scientific notation.
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    try:
        if "e" in s.lower():
            s = str(int(float(s)))
    except ValueError:
        pass
    return s


def standardize_df(df: pd.DataFrame, year: int) -> pd.DataFrame:
    # Rename columns to a shared schema so we can stack all years together.
    rename_map = {}
    for col in df.columns:
        norm = _norm_col(str(col))
        if norm in DROP_COLS:
            rename_map[col] = None
            continue
        rename_map[col] = CANON_MAP.get(norm, norm)

    cols = []
    for col in df.columns:
        new = rename_map.get(col)
        if new is None:
            continue
        cols.append(new)

    df = df[[c for c in df.columns if rename_map.get(c) is not None]].copy()
    df.columns = cols

    if "census_tract" in df.columns:
        df["census_tract"] = df["census_tract"].apply(normalize_tract)

    # Add year/version for later filtering and time comparisons.
    df["year"] = year
    df["version"] = {2014: "2.0", 2019: "3.0", 2021: "4.0"}[year]

    # Try to cast to numeric where possible (e.g., scores, percentiles).
    for col in df.columns:
        if col in {"census_tract", "county", "zip", "approx_location", "version"}:
            continue
        df[col] = pd.to_numeric(df[col], errors="ignore")

    return df


def main() -> None:
    # Read each version, normalize, and stack.
    frames = []
    for year, path in INPUTS.items():
        if not path.exists():
            raise FileNotFoundError(path)
        df = read_xlsx_first_sheet(path)
        df = standardize_df(df, year)
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True, sort=False)

    preferred = [
        "census_tract",
        "county",
        "zip",
        "approx_location",
        "longitude",
        "latitude",
        "total_population",
        "ces_score",
        "ces_percentile",
        "ces_percentile_range",
        "pollution_burden",
        "pollution_burden_score",
        "pollution_burden_pctl",
        "pop_char",
        "pop_char_score",
        "pop_char_pctl",
        "ozone",
        "ozone_pctl",
        "pm25",
        "pm25_pctl",
        "diesel_pm",
        "diesel_pm_pctl",
        "drinking_water",
        "drinking_water_pctl",
        "pesticides",
        "pesticides_pctl",
        "tox_release",
        "tox_release_pctl",
        "traffic",
        "traffic_pctl",
        "cleanup_sites",
        "cleanup_sites_pctl",
        "groundwater_threats",
        "groundwater_threats_pctl",
        "haz_waste",
        "haz_waste_pctl",
        "imp_water_bodies",
        "imp_water_bodies_pctl",
        "solid_waste",
        "solid_waste_pctl",
        "lead",
        "lead_pctl",
        "asthma",
        "asthma_pctl",
        "low_birth_weight",
        "low_birth_weight_pctl",
        "cardiovascular_disease",
        "cardiovascular_disease_pctl",
        "education",
        "education_pctl",
        "linguistic_isolation",
        "linguistic_isolation_pctl",
        "poverty",
        "poverty_pctl",
        "unemployment",
        "unemployment_pctl",
        "housing_burden",
        "housing_burden_pctl",
        "year",
        "version",
    ]
    cols = [c for c in preferred if c in merged.columns] + [
        c for c in merged.columns if c not in preferred
    ]
    merged = merged[cols]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_PATH, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote {OUT_PATH} with {len(merged)} rows and {len(merged.columns)} cols")


if __name__ == "__main__":
    main()
