import pandas as pd

df = pd.read_json('https://data.rivm.nl/covid-19/COVID-19_rioolwaterdata.json')

df.to_csv('nl_wastewater_data.csv')
