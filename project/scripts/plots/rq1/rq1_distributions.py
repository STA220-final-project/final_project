import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

ces_combined = pd.read_csv("ces_final.csv")

# mimic clean_names() from R
ces_combined.columns = (
    ces_combined.columns
    .str.lower()
    .str.replace(" ", "_")
)

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

plt.figure()

sns.histplot(
    tract_changes["ces_change"],
    bins=40,
    edgecolor="white",
    alpha=0.8
)

plt.axvline(0, color="red", linestyle="--", linewidth=2)

plt.title("Distribution of CES Score Changes Across Census Tracts from 2014 to 2021")
plt.xlabel("Annual Change in CES Score")
plt.ylabel("Number of Census Tracts")

plt.tight_layout()
plt.show()


improved = (tract_changes["ces_change"] < 0).sum()
worsened = (tract_changes["ces_change"] > 0).sum()

print("Improved tracts:", improved)
print("Worsened tracts:", worsened)