# Purpose: To read and process the wastewater data from the US to estimate the number of newly infected individuals
import pandas as pd
import numpy as np
from sodapy import Socrata

# Get US data
client = Socrata("data.cdc.gov", None)
results_data = client.get("g653-rqe2", limit=200000000)

# Convert to pandas DataFrame
ww_us = pd.DataFrame.from_records(results_data)
#ww_us = pd.read_csv('NWSS_Public_SARS-CoV-2_Concentration_in_Wastewater_Data.csv')

# Get all rows for which "normalization" column has the value "flow-population"
flo = ww_us[ww_us['normalization'] == 'flow-population']

# Get all rows for which "normalization" column has the value "microbial"
#mic = ww_us[ww_us['normalization'] == 'microbial']

# Select columns of interest and rename them
flo = flo[['date', 'key_plot_id', 'pcr_conc_lin']]
flo.columns = ['Date', 'key_plot_id', 'gc/capita/day']

#mic = mic[['date', 'key_plot_id', 'pcr_conc_lin']]
#mic.columns = ['Date', 'key_plot_id', 'SARS-CoV-2/PMMoV']

# Convert date to datetime
flo['Date'] = pd.to_datetime(flo['Date'])
#mic['Date'] = pd.to_datetime(mic['Date'])

# Remove rows with missing values
flo = flo.dropna()
#mic = mic.dropna()

# Sort dates from oldest to newest for each location
flo = flo.sort_values(by=['key_plot_id', 'Date'])
#mic = mic.sort_values(by=['key_plot_id', 'Date'])

# Get all unique Locations
#locations = ww['key_plot_id'].unique()

# Load the population data
results_pop = client.get("2ew6-ywp6", limit=200000000)

# Convert to pandas DataFrame
pop = pd.DataFrame.from_records(results_pop)

# Select only the columns of interest
pop = pop[['wwtp_jurisdiction','key_plot_id', 'date_end', 'population_served']]

# Rename the columns
pop.columns = ['State','key_plot_id', 'Date', 'Population']

# Convert date to datetime
pop['Date'] = pd.to_datetime(pop['Date'])

# Remove duplicates
pop = pop.drop_duplicates()

# Add the state to the wastewater data in a new column
flo = flo.merge(pop, how='left', on=['key_plot_id', 'Date'])
#mic = mic.merge(pop, how='left', on=['key_plot_id', 'Date'])

# Make sure that the values are numeric
flo['gc/capita/day'] = pd.to_numeric(flo['gc/capita/day'], errors='coerce')
#mic['SARS-CoV-2/PMMoV'] = pd.to_numeric(mic['SARS-CoV-2/PMMoV'], errors='coerce')
flo['Population'] = pd.to_numeric(flo['Population'], errors='coerce')
#mic['Population'] = pd.to_numeric(mic['Population'], errors='coerce')

# Convert negative values to 0
flo['gc/capita/day'] = flo['gc/capita/day'].clip(lower=0)
#mic['SARS-CoV-2/PMMoV'] = mic['SARS-CoV-2/PMMoV'].clip(lower=0)

# Create a complete date range from the earliest to the latest date in the dataset
#full_date_range = pd.date_range(start=flo['Date'].min(), end=flo['Date'].max())

# Function to reindex and interpolate data for each treatment plant
def reindex_and_interpolate(df):
    full_date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max())
    df = df.set_index('Date').reindex(full_date_range).interpolate(method='linear').reset_index()
    df['key_plot_id'] = df['key_plot_id'].fillna(method='ffill')
    df['State'] = df['State'].fillna(method='ffill')
    df['Population'] = df['Population'].fillna(method='ffill')
    df.columns = ['Date'] + list(df.columns[1:])
    return df

# Apply the function to each treatment plant
flo_interpolated = flo.groupby('key_plot_id').apply(reindex_and_interpolate).reset_index(drop=True)

# Aggregate data by state
state_aggregated = flo_interpolated.groupby(['State', 'Date']).apply(
    lambda x: (x['gc/capita/day'] * x['Population']).sum() / x['Population'].sum()
).reset_index(name='Weighted_gc/capita/day')

# Count the number of treatment plants contributing to each state's estimate on each date
contributing_plants_count = flo_interpolated.groupby(['State', 'Date']).size().reset_index(name='Contributing_Plants')

# Merge this count with the state aggregated data
state_aggregated_with_counts = pd.merge(state_aggregated, contributing_plants_count, on=['State', 'Date'])

# Calculate the population covered for each state on each date
state_population_covered = flo_interpolated.groupby(['State', 'Date'])['Population'].sum().reset_index(name='Population_Covered')

# Merge this data with the state aggregated data with counts
state_aggregated_with_counts_and_coverage = pd.merge(state_aggregated_with_counts, state_population_covered, on=['State', 'Date'])

# Load state population estimates
state_population_estimates = {
    'Alabama': 5024279, 'Alaska': 733391, 'Arizona': 7151502, 'Arkansas': 3011524,
    'California': 39538223, 'Colorado': 5773714, 'Connecticut': 3605944, 'Delaware': 989948,
    'Florida': 21538187, 'Georgia': 10711908, 'Hawaii': 1455271, 'Idaho': 1839106,
    'Illinois': 12812508, 'Indiana': 6785528, 'Iowa': 3190369, 'Kansas': 2937880,
    'Kentucky': 4505836, 'Louisiana': 4657757, 'Maine': 1362359, 'Maryland': 6177224,
    'Massachusetts': 7029917, 'Michigan': 10077331, 'Minnesota': 5706494, 'Mississippi': 2961279,
    'Missouri': 6154913, 'Montana': 1084225, 'Nebraska': 1961504, 'Nevada': 3104614,
    'New Hampshire': 1377529, 'New Jersey': 9288994, 'New Mexico': 2117522, 'New York': 20201249,
    'North Carolina': 10439388, 'North Dakota': 779094, 'Ohio': 11799448, 'Oklahoma': 3959353,
    'Oregon': 4237256, 'Pennsylvania': 13002700, 'Rhode Island': 1097379, 'South Carolina': 5118425,
    'South Dakota': 886667, 'Tennessee': 6910840, 'Texas': 29145505, 'Utah': 3271616,
    'Vermont': 643077, 'Virginia': 8631393, 'Washington': 7693612, 'West Virginia': 1793716,
    'Wisconsin': 5893718, 'Wyoming': 576851
}

# Create a DataFrame from the dictionary
state_population_df = pd.DataFrame(list(state_population_estimates.items()), columns=['State', 'State_Population'])

# Merge state population estimates to calculate the percentage of the state covered
state_aggregated_with_full_population = pd.merge(state_aggregated_with_counts_and_coverage, state_population_df, on='State')

# Calculate the percentage of the state's population covered
state_aggregated_with_full_population['Percentage_Covered'] = (state_aggregated_with_full_population['Population_Covered'] / state_aggregated_with_full_population['State_Population']) * 100

# Apply a centered 7-week rolling average to smooth the data
state_aggregated_with_full_population['Smoothed_gc/capita/day'] = state_aggregated_with_full_population.groupby('State')['Weighted_gc/capita/day'].transform(lambda x: x.rolling(window=7, center=True, min_periods=1).mean())

# Calculate the national weighted average
national_aggregated = state_aggregated_with_full_population.groupby('Date').apply(
    lambda x: (x['Weighted_gc/capita/day'] * x['Population_Covered']).sum() / x['Population_Covered'].sum()
).reset_index(name='National_Weighted_gc/capita/day')

# Apply a centered 7-day rolling average to smooth the national data
national_aggregated['National_Smoothed_gc/capita/day'] = national_aggregated['National_Weighted_gc/capita/day'].rolling(window=7, center=True, min_periods=1).mean()

# Calculate the total contributing plants and population covered for the nationwide data
national_aggregated['Contributing_Plants'] = state_aggregated_with_full_population.groupby('Date')['Contributing_Plants'].sum().values
national_aggregated['Population_Covered'] = state_aggregated_with_full_population.groupby('Date')['Population_Covered'].sum().values

# State population for nationwide is the sum of all state populations
national_aggregated['State_Population'] = state_population_df['State_Population'].sum()

# Calculate the percentage covered for the nationwide data
national_aggregated['Percentage_Covered'] = (national_aggregated['Population_Covered'] / national_aggregated['State_Population']) * 100

# Rename columns to match the state-level columns
national_aggregated.rename(columns={'National_Weighted_gc/capita/day': 'Weighted_gc/capita/day',
                                    'National_Smoothed_gc/capita/day': 'Smoothed_gc/capita/day'}, inplace=True)

# Add a column for the state
national_aggregated['State'] = 'Nationwide'

# Merge the national data with the state data
merged_data = pd.concat([state_aggregated_with_full_population, national_aggregated], ignore_index=True)

# Save to CSV
#merged_data.to_csv('state_and_national_NWSS_flow.csv', index=False)

# Load Biobot data
file2_path = 'United_States_states_cleaned.csv'

state_and_national_NWSS_flow = merged_data
united_states_states_cleaned = pd.read_csv(file2_path)

# Ensure dates are correctly formatted
state_and_national_NWSS_flow['Date'] = pd.to_datetime(state_and_national_NWSS_flow['Date'])
united_states_states_cleaned['Date'] = pd.to_datetime(united_states_states_cleaned['Date'])

# Filter data for the year 2024
NWSS_2024 = state_and_national_NWSS_flow[
    (state_and_national_NWSS_flow['Date'].dt.year == 2024) &
    (state_and_national_NWSS_flow['State'] == 'Nationwide')]

Biobot_2024 = united_states_states_cleaned[
    (united_states_states_cleaned['Country'] == 'United_States') &
    (united_states_states_cleaned['Measure'] == 'inf') &
    (united_states_states_cleaned['Region'] == 'Nationwide') &
    (united_states_states_cleaned['Date'].dt.year == 2024)
]

# Merge on date
merged_2024 = pd.merge(NWSS_2024, Biobot_2024, on='Date')

# Add column dividing NWSS wastewater by inf
merged_2024['NWSS/Biobot_inf'] = merged_2024['Smoothed_gc/capita/day'] / merged_2024['Value']

# Calculate average of NWSS/Biobot_inf
conv_fact = merged_2024['NWSS/Biobot_inf'].mean()


# Split Nationwide data into "Nationwide (Biobot)" and "Nationwide (NWSS)"
Biobot = united_states_states_cleaned[
    (united_states_states_cleaned['Region'] == 'Nationwide')
]
Biobot['Region'] = 'Nationwide'
Biobot['Source'] = 'Biobot'
Biobot = Biobot[['Source', 'Region', 'Date', 'Measure', 'Value']]
united_states_states_cleaned['Source'] = 'Biobot'
united_states_states_cleaned[['Source', 'Region', 'Date', 'Measure', 'Value']]

# Find max date for Biobot data
max_date = Biobot['Date'].max()

# Filter NWSS data for dates after the max date of Biobot data
NWSS = state_and_national_NWSS_flow[
    (state_and_national_NWSS_flow['State'] == 'Nationwide') &
    (state_and_national_NWSS_flow['Date'] > max_date)
]

# Add smoothed values as "wastewater" measures starting from the max date of Biobot data
smoothed_NWSS = NWSS[['Date', 'Smoothed_gc/capita/day']].rename(columns={'Smoothed_gc/capita/day': 'Value'})
smoothed_NWSS['Country'] = 'United_States'
smoothed_NWSS['Region'] = 'Nationwide'
smoothed_NWSS['Measure'] = 'wastewater'
smoothed_NWSS['Source'] = 'NWSS'

# Calculate new "inf" values
smoothed_NWSS['inf'] = smoothed_NWSS['Value'] / conv_fact

# Reshape to match the format of united_states_states_cleaned.csv
inf_values_2024 = smoothed_NWSS[['Source', 'Region', 'Date', 'Measure', 'inf']].rename(columns={'inf': 'Value'})
inf_values_2024['Measure'] = 'inf'

# Combine the data
updated_nationwide_data = pd.concat([Biobot, smoothed_NWSS[['Source', 'Region', 'Date', 'Measure', 'Value']], inf_values_2024])
updated_nationwide_NWSS_data = pd.concat([smoothed_NWSS[['Source', 'Region', 'Date', 'Measure', 'Value']], inf_values_2024])

# Rename united_states_states_cleaned to Biobot
united_states_states_cleaned.to_csv('Biobot_ww.csv', index=False)
united_states_states_cleaned.to_json('Biobot_ww.json', orient='records')

# Save nationwide for Joe
updated_nationwide_data.to_csv('Nationwide_Joe.csv', index=False)

# Save the updated CSV
updated_nationwide_NWSS_data.to_csv('NWSS_ww.csv', index=False)
# json
updated_nationwide_NWSS_data.to_json('NWSS_ww.json', orient='records')

