import pandas as pd
import numpy as np

# Read fecal shedding model
shedding = pd.read_csv('FecalSheddingModel.csv', index_col=0)

# Read the second row, first columns to get the column names
col_names_ww_VCH_F = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vQBi1bvkrF8c_46Ak5exKm07Nqej7Es1N-HHh9LHuR6M-tOF1H46H1ztCB5nSlPb_mJ7uGdsjA4proZ/pub?gid=1782200355&single=true&output=csv', header=None, nrows=1, usecols=range(6), skiprows=1).iloc[0]
col_names_ww_VIHA = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vQBi1bvkrF8c_46Ak5exKm07Nqej7Es1N-HHh9LHuR6M-tOF1H46H1ztCB5nSlPb_mJ7uGdsjA4proZ/pub?gid=1135929066&single=true&output=csv', header=None, nrows=1, usecols=range(4), skiprows=1).iloc[0]
col_names_ww_IH = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vQBi1bvkrF8c_46Ak5exKm07Nqej7Es1N-HHh9LHuR6M-tOF1H46H1ztCB5nSlPb_mJ7uGdsjA4proZ/pub?gid=168719565&single=true&output=csv', header=None, nrows=1, usecols=range(4), skiprows=1).iloc[0]

# Read the rest of the data, skipping the first two rows
ww_VCH_F = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vQBi1bvkrF8c_46Ak5exKm07Nqej7Es1N-HHh9LHuR6M-tOF1H46H1ztCB5nSlPb_mJ7uGdsjA4proZ/pub?gid=1782200355&single=true&output=csv', header=None, skiprows=2, usecols=range(6), names=col_names_ww_VCH_F)
ww_VIHA = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vQBi1bvkrF8c_46Ak5exKm07Nqej7Es1N-HHh9LHuR6M-tOF1H46H1ztCB5nSlPb_mJ7uGdsjA4proZ/pub?gid=1135929066&single=true&output=csv', header=None, skiprows=2, usecols=range(4), names=col_names_ww_VIHA)
ww_IH = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vQBi1bvkrF8c_46Ak5exKm07Nqej7Es1N-HHh9LHuR6M-tOF1H46H1ztCB5nSlPb_mJ7uGdsjA4proZ/pub?gid=168719565&single=true&output=csv', header=None, skiprows=2, usecols=range(4), names=col_names_ww_IH)

# Change first column name to date
ww_VCH_F.rename(columns={ww_VCH_F.columns[0]:'Date'}, inplace=True)
ww_VIHA.rename(columns={ww_VIHA.columns[0]:'Date'}, inplace=True)
ww_IH.rename(columns={ww_IH.columns[0]:'Date'}, inplace=True)

# Change date column to datetime format
ww_VCH_F['Date'] = pd.to_datetime(ww_VCH_F['Date'])
ww_VIHA['Date'] = pd.to_datetime(ww_VIHA['Date'])
ww_IH['Date'] = pd.to_datetime(ww_IH['Date'])

# Set date as index
ww_VCH_F.set_index('Date', inplace=True)
ww_VIHA.set_index('Date', inplace=True)
ww_IH.set_index('Date', inplace=True)

# Bring all values in VIHA and IH to billion gc / day from trillion gc / day
ww_VIHA = ww_VIHA * 1000
ww_IH = ww_IH * 1000

# Concatenate all dataframes
ww = pd.concat([ww_VCH_F, ww_VIHA, ww_IH], axis=1)

# Interpolate missing values in each column
ww = ww.interpolate(method='linear', axis=0)

# Save as csv
ww.to_csv('data/Canada/ww_BC_Canada.csv')
