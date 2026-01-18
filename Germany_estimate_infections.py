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

# Look at typ SARS-CoV-2 only
df = df[df['typ'] == 'SARS-CoV-2']
# Interpolate missing values
df_inter = df.interpolate(method='linear')
# Select necessary columns
df_wastewater = df_inter[['datum', 'vorhersage']]
# Change date column to English
df_en = df_wastewater.rename(columns={'datum':'Date'})
# Change date string into datetime object
df_en['Date'] = pd.to_datetime(df_en['Date'])

# Define the conversion factor function
def conversion_factor(date, cf):
    # Define the transition periods
    start_date_omicron = pd.Timestamp('2021-12-17')
    end_date_omicron = start_date_omicron + pd.Timedelta(days=30)
    start_date_post_omicron = pd.Timestamp('2022-08-01')  # Start date for the second transition
    end_date_post_omicron = start_date_post_omicron + pd.Timedelta(days=30)

    max_factors = [1.23382138,2.74038831] # Factors based on IHME and RKI data from mid to end 2022 (optim_initial_max)

    if date < start_date_omicron:
        return cf  # Before Omicron
    elif start_date_omicron <= date <= end_date_omicron:
        # Linear interpolation for the first transition
        proportion = (date - start_date_omicron) / pd.Timedelta(days=30)
        return cf + proportion * ((cf * max_factors[0]) - cf)
    elif end_date_omicron < date < start_date_post_omicron:
        # After first transition, before second
        return cf * max_factors[0]
    elif start_date_post_omicron <= date <= end_date_post_omicron:
        # Linear interpolation for the second transition
        proportion = (date - start_date_post_omicron) / pd.Timedelta(days=30)
        return (cf * max_factors[0]) + proportion * ((cf * max_factors[1]) - (cf * max_factors[0]))
    else:
        return cf * max_factors[1]  # After the second transition

# Apply the conversion factor to the columns
df_en['vorhersage'] = df_en.apply(lambda row: row['vorhersage'] * conversion_factor(row['Date'], 1), axis=1)

infections = []
for index, row in df_en.iterrows():
    infection_level = row['vorhersage'] / 1.7651333303853443 # Conversion factor based on IHME and RKI data from mid to end 2022 (optim_initial_max)
    infections.append(infection_level)

df_en = df_en.assign(estimated_infections=infections)

rows_list_json_nationwide = []
# Fill in json list for the regions
for index, row in df_en.iterrows():
    rows_list_json_nationwide.append({'Country': 'Germany', 'Region': 'Nationwide', 'Date': row['Date'], 'Measure': 'inf', 'Value': round_to_two_significant_digits(row['estimated_infections'])})
    rows_list_json_nationwide.append({'Country': 'Germany', 'Region': 'Nationwide', 'Date': row['Date'], 'Measure': 'wastewater', 'Value': round_to_two_significant_digits(row['vorhersage'])})

combined_df = pd.DataFrame(rows_list_json_nationwide)


# Saving to JSON (Change name of file to "Germany_wwa.json" to visualize in existing graph for the US)
json_result = combined_df.to_json('Germany_wwb.json', orient='records', date_format='iso')
