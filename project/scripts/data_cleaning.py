import pandas as pd
import numpy as np

na_vals = ["", "NA", " "]

ces2 = pd.read_excel("ces-2-2014.xlsx", na_values=na_vals)
ces3 = pd.read_excel("ces-3-2019.xlsx", na_values=na_vals)
ces4 = pd.read_excel("ces-4-2021.xlsx", na_values=na_vals)

ces2 = ces2.rename(columns={
    "CES 2.0 Score": "CES_score",
    "CES 2.0 Percentile Range": "CES_percentile_range"
})

ces3 = ces3.rename(columns={
    "CES 3.0 Score": "CES_score",
    "CES 3.0 Percentile": "CES_percentile",
    "CES 3.0 Percentile Range": "CES_percentile_range"
})

ces4 = ces4.rename(columns={
    "CES 4.0 Score": "CES_score",
    "CES 4.0 Percentile": "CES_percentile",
    "CES 4.0 Percentile Range": "CES_percentile_range"
})

ces2["CES_percentile"] = np.nan

common_columns = list(set(ces2.columns) & set(ces3.columns) & set(ces4.columns))

columns_needed = [
    "Census Tract",
    "Total Population",
    "California County",
    "ZIP",
    "Approximate Location",
    "Longitude",
    "Latitude",
    "CES_score",
    "CES_percentile",
    "CES_percentile_range",
    "Ozone",
    "Ozone Pctl",
    "PM2.5",
    "PM2.5 Pctl",
    "Traffic",
    "Traffic Pctl",
    "Pollution Burden",
    "Pollution Burden Score",
    "Pollution Burden Pctl",
    "Asthma",
    "Asthma Pctl"
]


ces2_small = ces2[columns_needed].copy()
ces3_small = ces3[columns_needed].copy()
ces4_small = ces4[columns_needed].copy()

ces2_small["year"] = 2014
ces3_small["year"] = 2019
ces4_small["year"] = 2021

ces_combined = pd.concat([ces2_small, ces3_small, ces4_small], ignore_index=True)


ces_combined["calc_pct"] = (
    ces_combined
    .groupby("year")["CES_score"]
    .rank(pct=True) * 100
)

ces_combined["CES_percentile"] = ces_combined["CES_percentile"].fillna(
    ces_combined["calc_pct"]
)

ces_combined = ces_combined.drop(columns=["calc_pct"])



ces_combined.to_csv("ces_final.csv", index=False)