import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import minimize
import scipy.stats as stats

# Read wastewater data from RKI, use aggregated curve for nationwide data
df = pd.read_csv('https://raw.githubusercontent.com/robert-koch-institut/Abwassersurveillance_AMELAG/main/amelag_aggregierte_kurve.tsv', sep='\t')

# Interpolate missing values
df_inter = df.interpolate(method='linear')
df_wastewater = df_inter[['datum', 'loess_vorhersage']]
df_wastewater['datum'] = pd.to_datetime(df_wastewater['datum'])

# Define conversion factor function with adjustable parameters
def conversion_factor(date, cf, factor1, factor2):
    start_date_omicron = pd.Timestamp('2021-12-17')
    end_date_omicron = start_date_omicron + pd.Timedelta(days=30)
    start_date_post_omicron = pd.Timestamp('2022-08-01')
    end_date_post_omicron = start_date_post_omicron + pd.Timedelta(days=30)
    
    if date < start_date_omicron:
        return cf
    elif start_date_omicron <= date <= end_date_omicron:
        proportion = (date - start_date_omicron) / pd.Timedelta(days=30)
        return cf + proportion * ((cf * factor1) - cf)
    elif end_date_omicron < date < start_date_post_omicron:
        return cf * factor1
    elif start_date_post_omicron <= date <= end_date_post_omicron:
        proportion = (date - start_date_post_omicron) / pd.Timedelta(days=30)
        return (cf * factor1) + proportion * ((cf * factor2) - (cf * factor1))
    else:
        return cf * factor2

# Objective function to minimize (negative correlation)
def objective(factors):
    factor1, factor2 = factors
    df_wastewater['adjusted_loess_vorhersage'] = df_wastewater.apply(
        lambda row: row['loess_vorhersage'] * conversion_factor(row['datum'], 1, factor1, factor2), axis=1
    )
    
    merged = pd.merge(df_wastewater, ih_2022, left_on='datum', right_on='date', how='inner')
    correlation = merged['adjusted_loess_vorhersage'].corr(merged['inf_mean'])
    
    # We negate the correlation because we want to maximize it, and optimization minimizes by default
    return -correlation

# Read IHME data
ihme = pd.read_csv('https://ihmecovid19storage.blob.core.windows.net/archive/2022-12-16/data_download_file_reference_2022.csv')
ih = ihme[ihme['location_name'] == 'Germany'].sort_values('date')
ih = ih[['date', 'inf_mean']]
ih['date'] = pd.to_datetime(ih['date'])
ih_2022 = ih[(ih['date'] >= '2022-06-01') & (ih['date'] <= '2022-11-30')]

# Initial guess for the optimization (starting with factors 1.53 and 2.28, from https://www.medrxiv.org/content/10.1101/2024.02.03.24302274v1)
initial_guess = [1.53, 2.28]

# Perform optimization to find the best factors
result = minimize(objective, initial_guess, method='Nelder-Mead')

# Print the optimal factors
optimal_factors = result.x
print(f"Optimal factors: {optimal_factors}")

# Apply the optimal factors to the wastewater data
df_wastewater['adjusted_loess_vorhersage'] = df_wastewater.apply(
    lambda row: row['loess_vorhersage'] * conversion_factor(row['datum'], 1, optimal_factors[0], optimal_factors[1]), axis=1
)

# Calculate the final correlation
merged = pd.merge(df_wastewater, ih_2022, left_on='datum', right_on='date', how='inner')
final_correlation = merged['adjusted_loess_vorhersage'].corr(merged['inf_mean'])
print(f"Final correlation after optimization: {final_correlation}")

# Plot the results
plt.plot(merged['datum'], merged['adjusted_loess_vorhersage'], label='Adjusted loess_vorhersage')
plt.plot(merged['datum'], merged['inf_mean'], label='inf_mean')
plt.legend()
plt.show()

# For each date divide loess_vorhersage by inf_mean, then take the average of the ratios
merged['ratio'] = merged['adjusted_loess_vorhersage'] / merged['inf_mean']
conv_factor_wastewater_inf = merged['ratio'].mean()
print(f"Conversion factor from wastewater to infections: {conv_factor_wastewater_inf}")
