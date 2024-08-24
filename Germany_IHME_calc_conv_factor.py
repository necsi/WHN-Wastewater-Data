import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from math import log10, floor

# Read wastewater data from RKI, use aggregated curve for nationwide data
df = pd.read_csv('https://raw.githubusercontent.com/robert-koch-institut/Abwassersurveillance_AMELAG/main/amelag_aggregierte_kurve.tsv', sep='\t')

# Interpolate missing values
df_inter = df.interpolate(method='linear')
# Select necessary columns
df_wastewater = df_inter[['datum', 'loess_vorhersage']]
# Convert 'datum' column to datetime
df_wastewater['datum'] = pd.to_datetime(df_wastewater['datum'])

# Define the conversion factor function
def conversion_factor(date, cf):
    # Define the transition periods
    start_date_omicron = pd.Timestamp('2021-12-17')
    end_date_omicron = start_date_omicron + pd.Timedelta(days=30)
    start_date_post_omicron = pd.Timestamp('2022-08-01')  # Start date for the second transition
    end_date_post_omicron = start_date_post_omicron + pd.Timedelta(days=30)

    if date < start_date_omicron:
        return cf  # Before Omicron
    elif start_date_omicron <= date <= end_date_omicron:
        # Linear interpolation for the first transition
        proportion = (date - start_date_omicron) / pd.Timedelta(days=30)
        return cf + proportion * ((cf * 1.53) - cf)
    elif end_date_omicron < date < start_date_post_omicron:
        # After first transition, before second
        return cf * 1.53
    elif start_date_post_omicron <= date <= end_date_post_omicron:
        # Linear interpolation for the second transition
        proportion = (date - start_date_post_omicron) / pd.Timedelta(days=30)
        return (cf * 1.53) + proportion * ((cf * 2.28) - (cf * 1.53))
    else:
        return cf * 2.28  # After the second transition

# Apply the conversion factor to the columns
df_wastewater['loess_vorhersage'] = df_wastewater.apply(lambda row: row['loess_vorhersage'] * conversion_factor(row['datum'], 1), axis=1)

# Read IHME data
ihme = pd.read_csv('https://ihmecovid19storage.blob.core.windows.net/archive/2022-12-16/data_download_file_reference_2022.csv')

# Filter the DataFrame for the 'Germany' location and only keep the date and the inf_mean columns
ih = ihme[ihme['location_name'] == 'Germany'].sort_values('date')
ih = ih[['date', 'inf_mean']]
ih['date'] = pd.to_datetime(ih['date'])

# Make a new dataframe containing only values from June 2022 to November 2022
ih_2022 = ih[(ih['date'] >= '2022-06-01') & (ih['date'] <= '2022-11-30')]
# Merge the two dataframes on the date column
merged = pd.merge(df_wastewater, ih_2022, left_on='datum', right_on='date', how='inner')

# Select date range from 2022-06-01 to 2022-11-30
merged = merged[(merged['datum'] >= '2022-06-01') & (merged['datum'] <= '2022-11-30')]

# Calculate the correlation between the two columns
correlation = merged['loess_vorhersage'].corr(merged['inf_mean'])
print(f"Correlation between loess_vorhersage and inf_mean from IHME: {correlation}")

# Plot the two columns
plt.plot(merged['datum'], merged['loess_vorhersage'], label='loess_vorhersage')
plt.plot(merged['datum'], merged['inf_mean'], label='inf_mean')
plt.legend()
plt.show()

# For each date divide loess_vorhersage by inf_mean, then take the average of the ratios
merged['ratio'] = merged['loess_vorhersage'] / merged['inf_mean']
conv_factor_wastewater_inf = merged['ratio'].mean()
print(f"Conversion factor from wastewater to infections: {conv_factor_wastewater_inf}")
