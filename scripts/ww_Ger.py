import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import re


# URL of the web page containing Plotly chart
url = "https://www.rki.de/DE/Content/Institut/OrgEinheiten/Abt3/FG32/Abwassersurveillance/Bericht_Abwassersurveillance.html?__blob=publicationFile"

# Fetch the HTML content
response = requests.get(url)
html_content = response.text

# Parse HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Find the first script tag containing the initial part of JSON data
script_tag_first_part = soup.find('script', {'data-for': 'id'})
if script_tag_first_part:
    # Extract and load the first part of JSON data
    json_data_first_part = json.loads(script_tag_first_part.contents[0])
else:
    print("First part script tag not found. Check if the HTML structure has changed.")

# Find the second script tag containing the remaining part of JSON data
script_tag_second_part = soup.find('script', {'data-for': re.compile(r'htmlwidget.*')})
if script_tag_second_part:
    # Extract and load the second part of JSON data
    json_data_second_part = json.loads(script_tag_second_part.contents[0])
else:
    print("Second part script tag not found. Check if the HTML structure has changed.")

# Concatenate the two parts to get the complete JSON data
data = {**json_data_first_part, **json_data_second_part}

# Read the JSON file and assign it to a DataFrame
df = pd.json_normalize(data)

df2 = df.copy()

# Select all columns starting with "map." and the column with the name "x.data"
df2 = df2.filter(regex='^(map\.|x\.data$)')

# Get all columns but the last one
df_map = df2.iloc[:, :-1]
#dfmap.to_csv('dfmap.csv', index=False)

# Make new dataframe for x.data
df3 = df2["x.data"].copy()
#df3.to_csv("xdata.csv")

# Select LOESS regression data, linear
d_json = df3[0][5]

# DataFrame with variables of interest
df_data = pd.DataFrame({
    'key': d_json['key'],
    'x': d_json['x'],
    'y': d_json['y']
})

# Make sure that the key column is numeric and replace none entries with nan
df_data['key'] = pd.to_numeric(df_data['key'], errors='coerce')


# Make sure that the y column is of type float
df_data['y'] = df_data['y'].astype(float)

# Function to extract and transform data for each region
def extract_transform_data(region_name, df_map, df_data):
    # Extract indices for the region
    indices = df_map[region_name].iloc[0]
    indices = [int(x) for x in indices]
    

    # Filter the data for the region
    df_region = df_data[df_data['key'].isin(indices)]
    df_region['x'] = pd.to_datetime(df_region['x'])  # Convert to datetime

    # Add a column for region name
    df_region['region'] = region_name.split('.')[1]  # Extract region name from column name

    return df_region

# Create an empty dataframe to store data for all regions
df_all_regions = pd.DataFrame()

# Iterate over all regions in df_map and append to df_all_regions
for region in df_map.columns:
    df_region = extract_transform_data(region, df_map, df_data)
    # append using concat
    df_all_regions = pd.concat([df_all_regions, df_region])

# Reordering the columns
df_all_regions = df_all_regions[['region', 'x', 'y']]

# Saving the dataframe to a CSV file
output_csv_path = 'data/Germany/German_wastewater_data.csv'
df_all_regions.to_csv(output_csv_path, index=False)
