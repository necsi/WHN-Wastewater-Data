import pandas as pd
import sqlite3 as sl


df = pd.read_csv('../data/Sweden/Relative-copy-number-of-SARS-CoV-2-to-PPMoV-in-Uppsala-wastewater.csv')
df = df.rename(columns={'relative_copy_number': 'rel_copy_number',
                        'infA-gene cn per PMMoV cn x 10000': 'infA',
                        'infB-gene cn per PMMoV cn x 10000': 'infB'})

# Create SQLite connection object
con = sl.connect('sweden_data.db')

df.to_sql('SWEDEN_WW', con)
