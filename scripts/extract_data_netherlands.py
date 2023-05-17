import pandas as pd
import sqlite3 as sl


df = pd.read_csv('../data/Netherlands/nl_wastewater_data.csv')

# # Create SQLite connection object
con = sl.connect('../data/Netherlands/netherlands_data.db')

df.to_sql('NETHERLANDS_WW', con)
