import pandas as pd
import requests
import os
from pathlib import Path
from datetime import datetime, timedelta
from math import log10, floor
from json import loads, dumps

def download_rki_data_file(base_url, folder, file_path_disk):
    # Attempt to download the file
    response = requests.get(base_url)
    if response.status_code == 200:
        # Save the file if found
        file_path = file_path_disk
        pathObj = Path(file_path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return f"CSV file downloaded successfully at {file_path}.", file_path

    return "No file found in the specified url", None

# Returns the correct conversion factor for a date passed in
def conversion_factor(date):
    # Conversion factor based on ihme and biobot data for the first 5 months of 2021 when testing was good and correlation was over 0.99
    cf = 915.6749186924305 
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

# Define a function to round a number to two significant digits
def round_to_two_significant_digits(num):
    if num == 0:
        return 0
    else:
        # Calculate the number of digits to round to
        round_digits = -int(floor(log10(abs(num)))) + 1
        # Round the number
        return round(num, round_digits)
    
if __name__ == "__main__":
    # Base URL and other parameters
    base_url = "https://raw.githubusercontent.com/robert-koch-institut/Abwassersurveillance_AMELAG/main/amelag_einzelstandorte.tsv"
    file_extension = os.path.splitext(base_url)[1]
    file_folder_disk = "_automated_csvs/"
    file_path_disk = f"{file_folder_disk}2024_wastewater_by_state_RKI_Germany{file_extension}"  # File extension

    # Call the function to download the file
    result, file_path = download_rki_data_file(base_url, file_folder_disk, file_path_disk)

    # Read the CSV into a pandas DataFrame, making sure to parse the first column as dates
    # Specify the correct date format if pandas does not recognize it automatically
    df = pd.read_csv(file_path, sep='\t')
    df_inter = df.interpolate(method='linear')
    # How many entries in viral load are NA?
    print(f"viruslast with NA in raw df = {len(df[df['viruslast'].isna()])}")
    print(f"viruslast with NA in interpolated df = {len(df_inter[df_inter['viruslast'].isna()])}")
    print(f"loess_vorhersage with NA in raw df = {len(df[df['loess_vorhersage'].isna()])}")
    print(f"loess_vorhersage with NA in interpolated df = {len(df_inter[df_inter['loess_vorhersage'].isna()])}")

    # Select a subset of the dataframe for furhter processing
    df_wastewater = df_inter[['standort', 'bundesland', 'datum', 'viruslast']]
    # Change name of columns to english from now on
    df_en = df_wastewater.rename(columns={'standort':'City', 'bundesland':'Region', 'datum':'Date'})
    # Change date string into datetime object
    df_en['Date'] = pd.to_datetime(df_en['Date'])
    # Add Country column at the beginning of the dataframe
    df_en.insert(1, "Country", "Germany")

    cf = 915.6749186924305 # Conversion factor based on ihme and biobot data for the first 5 months of 2021 when testing was good and correlation was over 0.99

    infections = []
    for index, row in df_en.iterrows():
        infection_level = row['viruslast'] * conversion_factor(row['Date'])
        infections.append(infection_level)
    
    df_en = df_en.assign(estimated_infections=infections)
    print("done")

    # Get all the different regions in the dataframe 
    regions = pd.unique(df_en['Region'])
    df_regions = []
    rows_list_json_regions = []
    # Average all values per region for a given date to get a regional value
    for region in regions:
        # Get unique region dataframe and average by date 
        df_per_region = df_en.loc[df_en['Region'] == region]
        df_region_avg = df_per_region.groupby('Date').mean()
        # Add column for region at the beginning
        df_region_avg.insert(1, "Region", region)
        df_region_avg.insert(1, "Country", "Germany")        
        df_regions.append(df_region_avg)
        # Fill in json list for the regions
        for index, row in df_region_avg.iterrows():
            rows_list_json_regions.append({'Country': 'Germany', 'Region': region, 'Date': row.name, 'Measure': 'inf', 'Value': row['estimated_infections']})
            rows_list_json_regions.append({'Country': 'Germany', 'Region': region, 'Date': row.name, 'Measure': 'wastewater', 'Value': round_to_two_significant_digits(row['viruslast'])})
            
    # Get a national average
    df_all_regions = pd.concat(df_regions)
    df_german_avg = df_all_regions.groupby('Date').mean()
    # Add column for Germany at the beginning as a single region (used for visualisation)
    df_german_avg.insert(1, "Region", "Nationwide")
    df_german_avg.insert(1, "Country", "Germany")

    # Prepare list to generate JSON nationwide file in the required visualisation format
    rows_list_json_nationwide = []
    # First generate nationwide
    for index, row in df_german_avg.iterrows():
            rows_list_json_nationwide.append({'Country': 'Germany', 'Region': 'Nationwide', 'Date': row.name, 'Measure': 'inf', 'Value': row['estimated_infections']})
            rows_list_json_nationwide.append({'Country': 'Germany', 'Region': 'Nationwide', 'Date': row.name, 'Measure': 'wastewater', 'Value': round_to_two_significant_digits(row['viruslast'])})
    # Concat lists into single dataframe
    rows_list_json = rows_list_json_nationwide + rows_list_json_regions
    combined_df = pd.concat([pd.DataFrame(rows_list_json)], ignore_index=True)

    # Saving to JSON
    json_result = combined_df.to_json('Germany_states_cleaned.json', orient='records', date_format='iso')
    #parsed = loads(json_result)
    #dumps(parsed, indent=4) 
