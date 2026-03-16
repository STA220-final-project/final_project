import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import seaborn as sns

ces_combined = pd.read_csv("ces_final.csv")

ces_combined["year"] = pd.to_numeric(ces_combined["year"])

ces_combined["California County"] = ces_combined["California County"].astype("category")
ces_combined["Census Tract"] = ces_combined["Census Tract"].astype("category")

ces_combined.columns = (
    ces_combined.columns
    .str.lower()
    .str.replace(" ", "_")
)

model_data = ces_combined.dropna(
    subset = ["ces_score", "year", "census_tract"]
)

model_rq1 = smf.mixedlm(
    "ces_score ~ year",
    data=model_data,
    groups=model_data["census_tract"],
    re_formula="~year"
)

model_result = model_rq1.fit()

print(model_result.summary())

tract_changes = (
    ces_combined[ces_combined["year"].isin([2014, 2021])]
    [["census_tract", "year", "ces_score"]]
    .pivot(index="census_tract", columns="year", values="ces_score")
    .reset_index()
)

tract_changes = tract_changes.rename(
    columns={2014: "year_2014", 2021: "year_2021"}
)

tract_changes["ces_change"] = (
    tract_changes["year_2021"] - tract_changes["year_2014"]
)

most_improved = tract_changes.sort_values("ces_change").head(10)

most_worsened = tract_changes.sort_values(
    "ces_change", ascending=False
).head(10)

most_improved["direction"] = "Improved"
most_worsened["direction"] = "Worsened"

top20_tracts = pd.concat([most_improved, most_worsened])

county_lookup = ces_combined[[
    "census_tract",
    "california_county"
]].drop_duplicates()

top20_tracts = top20_tracts.merge(
    county_lookup,
    on="census_tract",
    how="left"
)

top20_tracts["label_text"] = (
    top20_tracts["census_tract"].astype(str)
    + " ("
    + top20_tracts["california_county"].astype(str)
    + ")"
)


plt.figure()

sns.barplot(
    data=top20_tracts,
    x="ces_change",
    y="label_text",
    hue="direction"
)

plt.title("Top 10 Improved vs Worsened Tracts (2014 → 2021)")
plt.xlabel("Change in CES Score")
plt.ylabel("Census Tract (County)")

plt.tight_layout()
plt.show()


plt.figure()

sns.histplot(
    tract_changes["ces_change"],
    bins=40
)

plt.axvline(0, linestyle="--")

plt.title("Distribution of CES Score Changes Across Census Tracts")
plt.xlabel("Change in CES Score (2014 → 2021)")
plt.ylabel("Number of Tracts")

plt.tight_layout()
plt.show()




