# States
import pandas as pd
import requests
from datetime import datetime, timedelta
from math import log10, floor

# Function to download the CSV file
def download_csv(base_url, start_date, days_back, file_extension):
    for i in range(days_back):
        # Calculate the date for the file
        date_for_file = start_date - timedelta(days=i)
        date_str = date_for_file.strftime("%Y-%m-%d") 

        # Construct the file URL
        file_url = f"{base_url}{date_str}{file_extension}"

        # Attempt to download the file
        response = requests.get(file_url)
        if response.status_code == 200:
            # Save the file if found
            file_path = f'US_Biobot_county_data_{date_str}.csv'
            with open(file_path, 'wb') as file:
                file.write(response.content)
            return f"CSV file for {date_str} downloaded successfully at {file_path}.", file_path

    return "No CSV file found in the specified date range.", None

# Base URL and other parameters
base_url = "https://d1t7q96h7r5kqm.cloudfront.net/" 
start_date = datetime.now()
days_back = 14  # Number of days to go back from today
file_extension = "_automated_csvs/wastewater_by_county.csv"  # File extension

# Call the function to download the CSV
result, file_path = download_csv(base_url, start_date, days_back, file_extension)


# Read the CSV into a pandas DataFrame, making sure to parse the first column as dates
# Specify the correct date format if pandas does not recognize it automatically
ww = pd.read_csv(file_path, skiprows=2, parse_dates=['Date'], usecols=[1, 2, 5, 6], 
                 names=['County_FIPS', 'Date', 'Concentration', 'State_Abbrev'],
                 date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d'))

# Turn County FIPS to int
ww['County_FIPS'] = ww['County_FIPS'].astype(int)

# Clean non-numeric characters from the concentration column if necessary
# For example, if there are commas in the numbers or there are strings like '<1'
ww['Concentration'] = pd.to_numeric(ww['Concentration'].replace('[^0-9.]', '', regex=True), errors='coerce')


# Read FIPS and population data
fips_pop_data = pd.read_csv('Fips_pop_short.csv')

# Prepare the population data
fips_pop_data['CENSUS_2020_POP'] = fips_pop_data['CENSUS_2020_POP'].fillna('0').str.replace(',', '').astype(int)

# Merge the wastewater data with the population data
merged_data = pd.merge(ww, fips_pop_data, left_on='County_FIPS', right_on='FIPStxt', how='left')

# Load the conversion factors CSV
conversion_factors_df = pd.read_csv('ConversionFactors/conversion_factors_by_state.csv')

# Create a dictionary mapping state abbreviations to conversion factors
conversion_factor_mapping = pd.Series(conversion_factors_df['Conversion Factor'].values, index=conversion_factors_df.State).to_dict()

def round_to_two_significant_digits(num):
    if num == 0:
        return 0
    else:
        round_digits = -int(floor(log10(abs(num)))) + 1
        return round(num, round_digits)

# Dictionary mapping state names to abbreviations
state_name_to_abbreviation = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'United States Virgin Islands': 'VI',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

rows_list = []
states = merged_data['State_Abbrev'].unique()

for state in states:
    # Filter for current state
    state_merged_data = merged_data[merged_data['State_Abbrev'] == state]

    # Calculate the weighted average for the current state
    state_weighted_avg_data = state_merged_data.groupby('Date').apply(lambda x: (x['Concentration'] * x['CENSUS_2020_POP']).sum() / x['CENSUS_2020_POP'].sum()).reset_index(name='weighted_avg_conc')
    state_weighted_avg_data['Date'] = pd.to_datetime(state_weighted_avg_data['Date'])
    state_weighted_avg_data = state_weighted_avg_data.sort_values('Date')

    # Prepare the wastewater data for the analysis period
    ww_state = state_weighted_avg_data.copy()
    if ww_state['Date'].size > 20:
        ww_state = ww_state.set_index('Date').resample('D').interpolate().reset_index()
        ww_state = ww_state[['Date', 'weighted_avg_conc']]
        ww_state['Date'] = pd.to_datetime(ww_state['Date'])
    else:
        # Use national average for comparison if there is not enough data
        ww_state = ww_state.set_index('Date').resample('D').interpolate().reset_index()
        ww_state = ww_state[['Date', 'weighted_avg_conc']]
        ww_state['Date'] = pd.to_datetime(ww_state['Date'])

    # Retrieve the conversion factor for the current state
    state_cf = conversion_factor_mapping[state]

    def conversion_factor(date):
        # Define the transition periods
        start_date_omicron = pd.Timestamp('2021-12-17')
        end_date_omicron = start_date_omicron + pd.Timedelta(days=30)
        start_date_post_omicron = pd.Timestamp('2022-08-01')  # Adjusted start date for the second transition
        end_date_post_omicron = start_date_post_omicron + pd.Timedelta(days=30)

        if date < start_date_omicron:
            return state_cf  # Before Omicron
        elif start_date_omicron <= date <= end_date_omicron:
            # Linear interpolation for the first transition
            proportion = (date - start_date_omicron) / pd.Timedelta(days=30)
            return state_cf + proportion * ((state_cf * 1.53) - state_cf)
        elif end_date_omicron < date < start_date_post_omicron:
            # After first transition, before second
            return state_cf * 1.53
        elif start_date_post_omicron <= date <= end_date_post_omicron:
            # Linear interpolation for the second transition
            proportion = (date - start_date_post_omicron) / pd.Timedelta(days=30)
            return (state_cf * 1.53) + proportion * ((state_cf * 2.28) - (state_cf * 1.53))
        else:
            return state_cf * 2.28  # After the second transition
    
    # Apply the conversion factor function
    ww_state['conversion_factor'] = ww_state['Date'].apply(conversion_factor)
    ww_state['estimated_infections'] = ww_state['weighted_avg_conc'] * ww_state['conversion_factor']
    
    # Apply the rounding function to the relevant columns
    ww_state['weighted_avg_conc'] = ww_state['weighted_avg_conc'].apply(round_to_two_significant_digits)

    for index, row in ww_state.iterrows():
        rows_list.append({'Country': 'United_States', 'Region': state, 'Date': row['Date'], 'Measure': 'inf', 'Value': row['estimated_infections']})
        rows_list.append({'Country': 'United_States', 'Region': state, 'Date': row['Date'], 'Measure': 'wastewater', 'Value': row['weighted_avg_conc']})

# Convert the combined DataFrame to JSON for use with ECharts
combined_df = pd.concat([pd.DataFrame(rows_list)], ignore_index=True)

combined_df.to_csv('US_states_cleaned.csv', index=False)

combined_df.to_json('US_states_cleaned.json', orient='records')