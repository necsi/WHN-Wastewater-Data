# Nationwide
import pandas as pd
import requests
from datetime import datetime, timedelta
from math import log10, floor

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
            file_path = f'US_Biobot_nationwide_data_{date_str}.csv'
            with open(file_path, 'wb') as file:
                file.write(response.content)
            return f"CSV file for {date_str} downloaded successfully at {file_path}.", file_path

    return "No CSV file found in the specified date range.", None

# Base URL and other parameters
base_url = "https://d1t7q96h7r5kqm.cloudfront.net/"
start_date = datetime.now()
days_back = 14  # Number of days to go back from today
file_extension = "_automated_csvs/wastewater_by_census_region_nationwide.csv"  # File extension

# Call the function to download the CSV
result, file_path = download_csv(base_url, start_date, days_back, file_extension)

# Read the CSV into a pandas DataFrame, making sure to parse the first column as dates
# Specify the correct date format if pandas does not recognize it automatically
df = pd.read_csv(file_path, skiprows=2, parse_dates=['Date'], usecols=[0, 1, 3], 
                 names=['Date', 'Location', 'Concentration'], 
                 date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d'))

# Clean non-numeric characters from the concentration column if necessary
# For example, if there are commas in the numbers or there are strings like '<1'
df['Concentration'] = pd.to_numeric(df['Concentration'].replace('[^0-9.]', '', regex=True), errors='coerce')

# Filter the DataFrame for the 'Nationwide' location
nationwide_df = df[df['Location'] == 'Nationwide'].sort_values('Date')

# Interpolate the missing dates
biob = nationwide_df.set_index('Date').resample('D').interpolate().reset_index()
biob = biob[['Date', 'Concentration']]
biob['Date'] = pd.to_datetime(biob['Date'])

cf = 915.6749186924305 # Conversion factor based on ihme and biobot data for the first 5 months of 2021 when testing was good and correlation was over 0.99

def conversion_factor(date):
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


biob['conversion_factor'] = biob['Date'].apply(lambda date: conversion_factor(date))
biob['estimated_infections'] = biob['Concentration'] * biob['conversion_factor']

# Define a function to round a number to two significant digits
def round_to_two_significant_digits(num):
    if num == 0:
        return 0
    else:
        # Calculate the number of digits to round to
        round_digits = -int(floor(log10(abs(num)))) + 1
        # Round the number
        return round(num, round_digits)

# Apply the rounding function to the relevant columns
biob['Concentration'] = biob['Concentration'].apply(round_to_two_significant_digits)

rows_list = []

for index, row in biob.iterrows():
        rows_list.append({'Country': 'United_States', 'Region': 'Nationwide', 'Date': row['Date'], 'Measure': 'inf', 'Value': row['estimated_infections']})
        rows_list.append({'Country': 'United_States', 'Region': 'Nationwide', 'Date': row['Date'], 'Measure': 'wastewater', 'Value': row['Concentration']})

combined_df = pd.concat([pd.DataFrame(rows_list)], ignore_index=True)

states_df = pd.read_csv('US_states_cleaned.csv')

final_combined_df = pd.concat([combined_df, states_df], ignore_index=True)

final_combined_df.to_json('United_States_states_cleaned.json', orient='records', date_format='iso')
final_combined_df.to_csv('United_States_states_cleaned.csv', index=False)