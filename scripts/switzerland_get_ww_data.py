import pandas as pd

# Altenrhein
df = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_altenrhein_v2.csv')
df.to_csv('data/Switzerland/altenrhein_ch_wastewater_data.csv')

# Chur
df2 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_chur_v2.csv')
df2.to_csv('data/Switzerland/chur_ch_wastewater_data.csv')

# Geneva
df3 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_geneve_v2.csv')
df3.to_csv('data/Switzerland/geneva_ch_wastewater_data.csv')

# Laupen
df4 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_laupen_v2.csv')
df4.to_csv('data/Switzerland/laupen_ch_wastewater_data.csv')

# Lugano
df5 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_lugano_v2.csv')
df5.to_csv('data/Switzerland/lugano_ch_wastewater_data.csv')

# Zurich
df6 = pd.read_csv('https://sensors-eawag.ch/sars/__data__/processed_normed_data_zurich_v2.csv')
df6.to_csv('data/Switzerland/zurich_ch_wastewater_data.csv')
