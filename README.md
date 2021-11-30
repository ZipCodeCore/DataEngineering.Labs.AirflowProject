# Airflow Project

ingest, clean, process and report.

Data Sources that can be used:

- Tennis tournament data
- Spotify API on music
- Housing data from a few sources (gov data mixed with MLS)
- Personal history of Amazon purchases
- Wearable data
- Fantasy football
- a dataset from your assion project domain

## Point of the Project

To create an airflow pipeline to ingest data from some source and go through some type of data wrangling of their choice 

- ingest from a data source(s)
- clean (add missing data, create a feature or two)
- process and perform some transformations
- report (charts, graphs, some kind of output which informs our understanding of the datasets)

You will need to

- decide on a data source; from CSV file to online API
- decide on how to transform the data in some way to clean it using Pandas
- decide on a simple data visualization using your choice of data viz package
- do it all in a airfow DAG

## Example

Maybe you like this Kaggle DS: https://www.kaggle.com/deepcontractor/seasonal-variation-in-births

And you'd like to see if births in Australia during the summer is different from births in France during the summer? (or perhaps other countries)

You Airflow DAG might - Load the CSV, filter it, format it, account for differences in populations (births per 100,000 people?)
and generally clean any missing data. Take note of the years, perhaps consolidate by decade?
And then chart something interesting about the data.

