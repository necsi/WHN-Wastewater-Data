import pandas as pd
from datetime import datetime, timedelta
from math import log10, floor

# Read wastewater data from RKI, use aggregated curve for nationwide data
df = pd.read_csv('https://raw.githubusercontent.com/robert-koch-institut/Abwassersurveillance_AMELAG/main/amelag_aggregierte_kurve.tsv', sep='\t')

# Define a function to round a number to two significant digits
def round_to_two_significant_digits(num):
    if num == 0:
        return 0
    else:
        # Calculate the number of digits to round to
        round_digits = -int(floor(log10(abs(num)))) + 1
        # Round the number
        return round(num, round_digits)

# Interpolate missing values
df_inter = df.interpolate(method='linear')
# Select necessary columns
df_wastewater = df_inter[['datum', 'loess_vorhersage']]
# Change date column to English
df_en = df_wastewater.rename(columns={'datum':'Date'})
# Change date string into datetime object
df_en['Date'] = pd.to_datetime(df_en['Date'])

infections = []
for index, row in df_en.iterrows():
    infection_level = row['loess_vorhersage'] * 1.7360232142524559 # Conversion factor based on IHME and RKI data from mid to end 2022 (see Germany_IHME_conversion_factor.py)
    infections.append(infection_level)

df_en = df_en.assign(estimated_infections=infections)

rows_list_json_nationwide = []
# Fill in json list for the regions
for index, row in df_en.iterrows():
    rows_list_json_nationwide.append({'Country': 'Germany', 'Region': 'Nationwide', 'Date': row['Date'], 'Measure': 'inf', 'Value': round_to_two_significant_digits(row['estimated_infections'])})
    rows_list_json_nationwide.append({'Country': 'Germany', 'Region': 'Nationwide', 'Date': row['Date'], 'Measure': 'wastewater', 'Value': round_to_two_significant_digits(row['loess_vorhersage'])})
        
combined_df = pd.DataFrame(rows_list_json_nationwide)


# Saving to JSON (Change name of file to "Germany_wwa.json" to visualize in existing graph for the US)
json_result = combined_df.to_json('Germany_states_cleaned.json', orient='records', date_format='iso')
