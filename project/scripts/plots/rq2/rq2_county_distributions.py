import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load and clean data
ces = pd.read_csv("ces_final.csv")

ces.columns = (
    ces.columns
    .str.lower()
    .str.replace(" ", "_")
)


county_year = (
    ces.groupby(["california_county", "year"])["ces_score"]
    .mean()          # raw mean CES score per county per year
    .reset_index()
)

# Pivot to get 2014 and 2021 side-by-side
county_changes = (
    county_year[county_year["year"].isin([2014, 2021])]
    .pivot(index="california_county", columns="year", values="ces_score")
    .reset_index()
)

county_changes = county_changes.rename(
    columns={2014: "year_2014", 2021: "year_2021"}
)

# raw CES score change
county_changes["ces_change"] = (
    county_changes["year_2021"] - county_changes["year_2014"]
)

# Histogram
plt.figure(figsize=(8, 5))

sns.histplot(
    county_changes["ces_change"],
    bins=40,
    color="#1f78b4",
    edgecolor="white",
    alpha=0.8
)

plt.axvline(0, color="red", linestyle="--", linewidth=2)

plt.title("Distribution of Raw CES Score Changes Across California Counties (2014 → 2021)")
plt.xlabel("Change in CES Score")
plt.ylabel("Number of Counties")

plt.tight_layout()
plt.show()

# Top 20 counties with largest absolute change
top_changes = (
    county_changes
    .reindex(county_changes["ces_change"].abs().sort_values(ascending=False).index)
    .head(20)
)

print("\nTop 20 counties with largest absolute CES score change:")
print(top_changes)
