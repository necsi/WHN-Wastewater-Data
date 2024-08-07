# Purpose: To read and process the wastewater data from the US to estimate the number of newly infected individuals
import pandas as pd
import numpy as np
from sodapy import Socrata
from datetime import timedelta

# Get US data
client = Socrata("data.cdc.gov", None)
results_data = client.get("g653-rqe2", limit=200000000)

# Convert to pandas DataFrame
nwss_data = pd.DataFrame.from_records(results_data)

# Load the Biobot data
biobot_file_path = 'United_States_states_cleaned.csv'
biobot_data = pd.read_csv(biobot_file_path)

# Convert date columns to datetime format
nwss_data['date'] = pd.to_datetime(nwss_data['date'])
biobot_data['Date'] = pd.to_datetime(biobot_data['Date'])

# Filter NWSS data for normalization type "flow-population"
nwss_data = nwss_data[nwss_data['normalization'] == 'flow-population']

# Rename columns to match the format
nwss_data = nwss_data[['date', 'key_plot_id', 'pcr_conc_lin']]
nwss_data.columns = ['Date', 'key_plot_id', 'gc/capita/day']

# Load population data
client = Socrata("data.cdc.gov", None)
pop_data = client.get("2ew6-ywp6", limit=200000000)
pop_data = pd.DataFrame.from_records(pop_data)

# Select only the columns of interest
pop_data = pop_data[['wwtp_jurisdiction', 'key_plot_id', 'date_end', 'population_served']]
pop_data.columns = ['State', 'key_plot_id', 'Date', 'Population']
pop_data['Date'] = pd.to_datetime(pop_data['Date'])
pop_data = pop_data.drop_duplicates()

# Merge population data with NWSS data
nwss_data = nwss_data.merge(pop_data, how='left', on=['key_plot_id', 'Date'])
nwss_data['gc/capita/day'] = pd.to_numeric(nwss_data['gc/capita/day'], errors='coerce')
nwss_data['Population'] = pd.to_numeric(nwss_data['Population'], errors='coerce')
nwss_data['gc/capita/day'] = nwss_data['gc/capita/day'].clip(lower=0)

# Interpolate data for each treatment plant
def reindex_and_interpolate(df):
    full_date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max())
    df = df.set_index('Date').reindex(full_date_range).interpolate(method='linear').reset_index()
    df['key_plot_id'] = df['key_plot_id'].fillna(method='ffill')
    df['State'] = df['State'].fillna(method='ffill')
    df['Population'] = df['Population'].fillna(method='ffill')
    df.columns = ['Date'] + list(df.columns[1:])
    return df

nwss_data_interpolated = nwss_data.groupby('key_plot_id').apply(reindex_and_interpolate).reset_index(drop=True)

# Aggregate data by state
state_aggregated = nwss_data_interpolated.groupby(['State', 'Date']).apply(
    lambda x: (x['gc/capita/day'] * x['Population']).sum() / x['Population'].sum()
).reset_index(name='Weighted_gc/capita/day')

# Count contributing plants and calculate population coverage
contributing_plants_count = nwss_data_interpolated.groupby(['State', 'Date']).size().reset_index(name='Contributing_Plants')
state_population_covered = nwss_data_interpolated.groupby(['State', 'Date'])['Population'].sum().reset_index(name='Population_Covered')

# Merge the counts and population coverage with state aggregated data
state_aggregated_with_counts = pd.merge(state_aggregated, contributing_plants_count, on=['State', 'Date'])
state_aggregated_with_full_population = pd.merge(state_aggregated_with_counts, state_population_covered, on=['State', 'Date'])

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

state_population_df = pd.DataFrame(list(state_population_estimates.items()), columns=['State', 'State_Population'])
state_aggregated_with_full_population = pd.merge(state_aggregated_with_full_population, state_population_df, on='State')
state_aggregated_with_full_population['Percentage_Covered'] = (state_aggregated_with_full_population['Population_Covered'] / state_aggregated_with_full_population['State_Population']) * 100

# Apply a centered 7-day rolling average to smooth the data
state_aggregated_with_full_population['Smoothed_gc/capita/day'] = state_aggregated_with_full_population.groupby('State')['Weighted_gc/capita/day'].transform(lambda x: x.rolling(window=7, center=True, min_periods=1).mean())

# Calculate the national weighted average
national_aggregated = state_aggregated_with_full_population.groupby('Date').apply(
    lambda x: (x['Weighted_gc/capita/day'] * x['Population_Covered']).sum() / x['Population_Covered'].sum()
).reset_index(name='National_Weighted_gc/capita/day')

national_aggregated['National_Smoothed_gc/capita/day'] = national_aggregated['National_Weighted_gc/capita/day'].rolling(window=7, center=True, min_periods=1).mean()
national_aggregated['Contributing_Plants'] = state_aggregated_with_full_population.groupby('Date')['Contributing_Plants'].sum().values
national_aggregated['Population_Covered'] = state_aggregated_with_full_population.groupby('Date')['Population_Covered'].sum().values
national_aggregated['State_Population'] = state_population_df['State_Population'].sum()
national_aggregated['Percentage_Covered'] = (national_aggregated['Population_Covered'] / national_aggregated['State_Population']) * 100

# Rename columns to match the state-level columns
national_aggregated.rename(columns={'National_Weighted_gc/capita/day': 'Weighted_gc/capita/day',
                                    'National_Smoothed_gc/capita/day': 'Smoothed_gc/capita/day'}, inplace=True)
national_aggregated['State'] = 'Nationwide'

# Merge the national data with the state data
merged_data = pd.concat([state_aggregated_with_full_population, national_aggregated], ignore_index=True)

# Ensure dates are correctly formatted in Biobot data
biobot_data['Date'] = pd.to_datetime(biobot_data['Date'])

# Filter the Biobot data to include only the 'inf' measure
biobot_data_inf = biobot_data[biobot_data['Measure'] == 'inf']

# Get a dictionary for state abbreviations to full names
state_abbreviations = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
}

# Convert state abbreviations to full names in Biobot data
biobot_data_inf['State'] = biobot_data_inf['Region'].map(state_abbreviations)

# Determine the last 4 months of available Biobot data for each state, with an exception for Colorado
def filter_dates(state, df):
    if state == 'Colorado':
        return df[df['Date'] > (df['Date'].max() - pd.DateOffset(days=109))]
    else:
        return df[df['Date'] > (df['Date'].max() - pd.DateOffset(months=4))]

biobot_data_last_months = biobot_data_inf.groupby('State').apply(lambda x: filter_dates(x.name, x)).reset_index(drop=True)


# Calculate conversion factors for each state
state_conversion_factors = {}
for state, group in biobot_data_last_months.groupby('State'):
    nwss_state_data = merged_data[(merged_data['State'] == state) & (merged_data['Date'].isin(group['Date']))]
    if not nwss_state_data.empty:
        merged_state_data = pd.merge(nwss_state_data, group, left_on='Date', right_on='Date')
        merged_state_data['NWSS/Biobot_inf'] = merged_state_data['Smoothed_gc/capita/day'] / merged_state_data['Value']
        conv_fact = merged_state_data['NWSS/Biobot_inf'].mean()
        state_conversion_factors[state] = conv_fact
        print(f"State: {state}, Conversion Factor: {conv_fact}")

# Calculate conversion factor for Nationwide
biobot_nationwide_last_4_months = biobot_data_inf[(biobot_data_inf['Region'] == 'Nationwide') & (biobot_data_inf['Date'] > (biobot_data_inf['Date'].max() - pd.DateOffset(months=4)))]
nwss_nationwide_data = merged_data[(merged_data['State'] == 'Nationwide') & (merged_data['Date'].isin(biobot_nationwide_last_4_months['Date']))]
if not nwss_nationwide_data.empty:
    merged_nationwide_data = pd.merge(nwss_nationwide_data, biobot_nationwide_last_4_months, left_on='Date', right_on='Date')
    merged_nationwide_data['NWSS/Biobot_inf'] = merged_nationwide_data['Smoothed_gc/capita/day'] / merged_nationwide_data['Value']
    conv_fact_nationwide = merged_nationwide_data['NWSS/Biobot_inf'].mean()
    state_conversion_factors['Nationwide'] = conv_fact_nationwide
    print(f"Nationwide Conversion Factor: {conv_fact_nationwide}")

# Filter states based on data quality criteria
filtered_states = []
for state, group in state_aggregated_with_full_population.groupby('State'):
    dates_in_last_4_months = biobot_data_last_months[biobot_data_last_months['State'] == state]['Date']
    min_contributing_plants = group[group['Date'].isin(dates_in_last_4_months)]['Contributing_Plants'].min()
    max_contributing_plants = group[group['Date'].isin(dates_in_last_4_months)]['Contributing_Plants'].max()
    min_population_covered = group[group['Date'].isin(dates_in_last_4_months)]['Percentage_Covered'].min()
    max_population_covered = group[group['Date'].isin(dates_in_last_4_months)]['Percentage_Covered'].max()
    if state in state_conversion_factors and min_contributing_plants >= 3 and min_population_covered >= 25:
        biobot_state_data = biobot_data_last_months[biobot_data_last_months['State'] == state]
        nwss_state_data = merged_data[merged_data['State'] == state]
        merged_state_data = pd.merge(nwss_state_data, biobot_state_data, left_on='Date', right_on='Date')
        correlation = merged_state_data['Smoothed_gc/capita/day'].corr(merged_state_data['Value'])
        if correlation >= 0.6:
            filtered_states.append(state)
        print(f"State: {state}, Conversion Factor: {state_conversion_factors[state]}, "
                f"Dates in Last 4 Months: {dates_in_last_4_months.min()} to {dates_in_last_4_months.max()}, "            
                f"Min Contributing Plants: {min_contributing_plants}, Max Contributing Plants: {max_contributing_plants}, "
                f"Min Population Covered: {min_population_covered}, Max Population Covered: {max_population_covered}, "
                f"Correlation: {correlation}")

# Prepare the final dataset
final_rows = []

# Process nationwide data
nationwide_data = merged_data[merged_data['State'] == 'Nationwide']
last_date_biobot_nationwide = biobot_data_inf[biobot_data_inf['Region'] == 'Nationwide']['Date'].max()
nationwide_data_filtered = nationwide_data[nationwide_data['Date'] > last_date_biobot_nationwide]

for date, row in nationwide_data_filtered.iterrows():
    final_rows.append({
        'Source': 'NWSS',
        'Region': 'Nationwide',
        'Date': row['Date'],
        'Measure': 'wastewater',
        'Value': row['Smoothed_gc/capita/day']
    })
    final_rows.append({
        'Source': 'NWSS',
        'Region': 'Nationwide',
        'Date': row['Date'],
        'Measure': 'inf',
        'Value': row['Smoothed_gc/capita/day'] / state_conversion_factors['Nationwide']
    })

# Concatenate nationwide data from Biobot and NWSS for Joe
joe_biobot_nationwide_data = biobot_data[biobot_data['Region'] == 'Nationwide']
joe_biobot_nationwide_data['Source'] = 'Biobot'
joe_biobot_nationwide_data = joe_biobot_nationwide_data[['Source', 'Region', 'Date', 'Measure', 'Value']]
joe_nwss_nationwide_data = pd.DataFrame(final_rows)
joe_nationwide_data = pd.concat([joe_biobot_nationwide_data, joe_nwss_nationwide_data], ignore_index=True)
joe_nationwide_data.to_csv('Nationwide_Joe.csv', index=False)


# Process each filtered state
for state in filtered_states:
    state_data = merged_data[merged_data['State'] == state]
    last_date_biobot_state = biobot_data_inf[biobot_data_inf['State'] == state]['Date'].max()
    state_data_filtered = state_data[state_data['Date'] > last_date_biobot_state]
    
    for date, row in state_data_filtered.iterrows():
        final_rows.append({
            'Source': 'NWSS',
            'Region': state,
            'Date': row['Date'],
            'Measure': 'wastewater',
            'Value': row['Smoothed_gc/capita/day']
        })
        final_rows.append({
            'Source': 'NWSS',
            'Region': state,
            'Date': row['Date'],
            'Measure': 'inf',
            'Value': row['Smoothed_gc/capita/day'] / state_conversion_factors[state]
        })

# Convert final rows to DataFrame
final_data = pd.DataFrame(final_rows)

# Save the final dataset to CSV and JSON
final_data.to_csv('NWSS_wwf.csv', index=False)
final_data.to_json('NWSS_wwf.json', orient='records')

print("Final dataset generated and saved.")


