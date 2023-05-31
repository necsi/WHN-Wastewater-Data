import pandas as pd

# Altenrhein
df = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_altenrhein_v2.csv', sep=';')
df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True))
df.to_csv('data/Switzerland/altenrhein_ch_wastewater_data.csv')

# Chur
df2 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_chur_v2.csv', sep=';')
df2.rename(columns={'Unnamed: 0': 'Date'}, inplace=True))
df2.to_csv('data/Switzerland/chur_ch_wastewater_data.csv')

# Geneva
df3 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_geneve_v2.csv', sep=';')
df3.rename(columns={'Unnamed: 0': 'Date'}, inplace=True))
df3.to_csv('data/Switzerland/geneva_ch_wastewater_data.csv')

# Laupen
df4 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_laupen_v2.csv', sep=';')
df4.rename(columns={'Unnamed: 0': 'Date'}, inplace=True))
df4.to_csv('data/Switzerland/laupen_ch_wastewater_data.csv')

# Lugano
df5 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_lugano_v2.csv', sep=';')
df5.rename(columns={'Unnamed: 0': 'Date'}, inplace=True))
df5.to_csv('data/Switzerland/lugano_ch_wastewater_data.csv')

# Zurich
df6 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_zurich_v2.csv', sep=';')
df6.rename(columns={'Unnamed: 0': 'Date'}, inplace=True))
df6.to_csv('data/Switzerland/zurich_ch_wastewater_data.csv')
