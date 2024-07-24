import pandas as pd
import requests
import os
from pathlib import Path
from datetime import datetime, timedelta
from math import log10, floor

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
df_aux = df[df['loess_vorhersage'].isna()]
# How many entries in viral load are NA?
print(f"viruslast with NA = {len(df[df['viruslast'].isna()])}")
print(f"loess_vorhersage with NA = {len(df[df['loess_vorhersage'].isna()])}")


cf = 915.6749186924305 # Conversion factor based on ihme and biobot data for the first 5 months of 2021 when testing was good and correlation was over 0.99

# Apply the rounding function to the relevant columns
#df['Concentration'] = biob['Concentration'].apply(round_to_two_significant_digits)
