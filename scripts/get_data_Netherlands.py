import pandas as pd

df = pd.read_csv('https://data.rivm.nl/covid-19/COVID-19_rioolwaterdata.csv')

df.to_csv('../data/Netherlands/nl_wastewater_data_test.csv')
