# STA 220 Final Project: Tracking Environmental Burden in California, 2014-2021: A CalEnviroScreen/AQS Analysis

Link to the presentation slides: https://docs.google.com/presentation/d/1NkjC1TaEJjarT1OvqK7J55Jurxkdc8W7I0TxmR6ZelI/edit?usp=drive_link

## About the Data

Data used for this project was collected from 2 primary sources:

[1] CalEnviroScreen (CES): https://oehha.ca.gov/calenviroscreen

[2] United States Enviornmental Protection Agency Air Quality System: https://www.epa.gov/aqs

### CalEnviroScreen
Environmental justice and community vulnerability metrics were obtained from CalEnviroScreen, a statewide screening tool developed by the California Office of Environmental Health Hazard Assessment (OEHHA). CalEnviroScreen identifies communities in California that are disproportionately burdened by pollution and socioeconomic stressors.

The tool combines environmental exposure indicators (e.g., air pollution, pesticide use, drinking water contaminants) with population characteristics such as poverty, education levels, and health vulnerabilities. These indicators are calculated at the census tract level and combined into composite scores that reflect both pollution burden and population sensitivity. Higher CalEnviroScreen scores indicate communities experiencing greater cumulative environmental and social disadvantage.

### EPA Air Quality System
Air pollution data were obtained from the United States Environmental Protection Agency (EPA) Air Quality System (AQS) database. AQS is the EPA’s primary repository for ambient air pollution monitoring data collected across the United States through federal, state, and local monitoring networks. The system stores measurements from regulatory monitoring stations that track pollutants regulated under the Clean Air Act.

### Research Questions:
1. Which tracts show the largest change in CES score from 2014 to 2021?
2. Do counties show consistent improvement or worsening in CES scores between 2014–2021?
3. Is improvement in air quality indicative of improvement in overall CES?
4. Does California have better air quality in high‑population counties versus other populous U.S. counties?
5. Which counties contain the most high‑burden tracts in 2021, and how does this compare to 2014 and 2019?

## Repository Structure
```
Final_Project
│   README.md
│   .gitignore
|   License
|   project.ipynb #Includes full workflow for data retrival, cleaning and visualization for AQS data   
│
└───project
    │   .env.example
    │
    └───data
          This folder includes raw and preprocessed data from our final project 
    └───output
          Script outputs broken up by reasearch question (More info in folder)
    └───scripts
          Exploratory data retrival and plot cleaning scripts