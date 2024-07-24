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

# Base URL and other parameters
base_url = "https://raw.githubusercontent.com/robert-koch-institut/Abwassersurveillance_AMELAG/main/amelag_einzelstandorte.tsv"
file_extension = os.path.splitext(base_url)[1]
file_folder_disk = "_automated_csvs/"
file_path_disk = f"{file_folder_disk}2024_wastewater_by_state_RKI_Germany{file_extension}"  # File extension

# Call the function to download the file
result, file_path = download_rki_data_file(base_url, file_folder_disk, file_path_disk)


