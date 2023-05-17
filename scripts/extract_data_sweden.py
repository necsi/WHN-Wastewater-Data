import pandas as pd
import sqlite3 as sl


df = pd.read_csv('../data/Sweden/Sweden_ww.csv')
df = df.rename(columns={'relative_copy_number': 'rel_copy_number',
                        'infA-gene cn per PMMoV cn x 10000': 'infA',
                        'infB-gene cn per PMMoV cn x 10000': 'infB'})

# Create SQLite connection object
con = sl.connect('../data/sweden_data.db')

df.to_sql('SWEDEN_WW', con)
