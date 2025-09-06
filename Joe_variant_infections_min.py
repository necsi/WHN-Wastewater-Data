import pandas as pd
import numpy as np
import zipfile
import os

# Load the data files
estimated_infections = pd.read_csv('Joe_EstimatedInfections_min.csv')
nationwide_variant_proportions = pd.read_csv('4_week_variant_nationwide.csv')
hhs_variant_proportions_files = {
    1: pd.read_csv('4_week_variant_hhs1.csv'),
    2: pd.read_csv('4_week_variant_hhs2.csv'),
    3: pd.read_csv('4_week_variant_hhs3.csv'),
    4: pd.read_csv('4_week_variant_hhs4.csv'),
    5: pd.read_csv('4_week_variant_hhs5.csv'),
    6: pd.read_csv('4_week_variant_hhs6.csv'),
    7: pd.read_csv('4_week_variant_hhs7.csv'),
    8: pd.read_csv('4_week_variant_hhs8.csv'),
    9: pd.read_csv('4_week_variant_hhs9.csv'),
    10: pd.read_csv('4_week_variant_hhs10.csv')  
}

# Define HHS region mapping
hhs_region_mapping = {
    "CT": 1, "ME": 1, "MA": 1, "NH": 1, "RI": 1, "VT": 1,
    "NJ": 2, "NY": 2,
    "DE": 3, "MD": 3, "PA": 3, "VA": 3, "WV": 3,
    "AL": 4, "FL": 4, "GA": 4, "KY": 4, "MS": 4, "NC": 4, "SC": 4, "TN": 4,
    "IL": 5, "IN": 5, "MI": 5, "MN": 5, "OH": 5, "WI": 5,
    "AR": 6, "LA": 6, "NM": 6, "OK": 6, "TX": 6,
    "IA": 7, "KS": 7, "MO": 7, "NE": 7,
    "CO": 8, "MT": 8, "ND": 8, "SD": 8, "UT": 8, "WY": 8,
    "AZ": 9, "CA": 9, "HI": 9, "NV": 9,
    "AK": 10, "ID": 10, "OR": 10, "WA": 10
}

# Extract variant columns from the nationwide variant proportions
variant_columns = nationwide_variant_proportions.columns[1:]
variant_columns = [col for col in variant_columns if col != 'NA']

# Define a function to process data for each state
def process_state_data(state, estimated_infections, variant_proportions, nationwide_proportions):
    # Filter the estimated infections for the given state
    if state == "US":
        state_data = estimated_infections[["Date", "Nationwide"]].rename(columns={"Nationwide": "State_Infections"})
    else:
        state_data = estimated_infections[["Date", state]].rename(columns={state: "State_Infections"})
    
    # Remove rows with missing state infections
    state_data = state_data.dropna(subset=["State_Infections"])
    
    # Merge with HHS-specific variant proportions where available
    merged_data = pd.merge(state_data, variant_proportions, left_on="Date", right_on="Unnamed: 0", how="left").drop(columns=["Unnamed: 0"])
    
    # Identify rows where HHS-specific data is missing and use nationwide proportions instead
    missing_mask = merged_data[variant_columns].isna().all(axis=1)
    merged_data.loc[missing_mask, variant_columns] = pd.merge(
        merged_data[["Date"]],
        nationwide_proportions,
        left_on="Date",
        right_on="Unnamed: 0",
        how="left"
    ).drop(columns=["Unnamed: 0"]).loc[missing_mask, variant_columns]
    
    # Calculate variant-specific infections for the state
    for variant in variant_columns:
        merged_data[variant] = merged_data["State_Infections"] * merged_data[variant]
    
    # Keep only relevant columns: Date, Region, State Infections, and calculated variant infections
    merged_data_final = merged_data[["Date"]].copy()
    merged_data_final["Region"] = state
    merged_data_final["Total_State_Infections"] = merged_data["State_Infections"].round(0).astype(int)
    merged_data_final = pd.concat([merged_data_final, merged_data[variant_columns]], axis=1)
    
    # Rename columns for output format
    merged_data_final.columns = ["Date", "Region", "Total_State_Infections"] + [variant for variant in variant_columns]
    
    return merged_data_final

# Remove initial empty rows for each state
def remove_initial_empty_rows(state_data):
    # Find the first row index where any variant column has a non-NaN value or State Infections is non-NaN
    first_valid_index = state_data[variant_columns].notna().any(axis=1).idxmax()
    # Slice the DataFrame from that row onward
    return state_data.loc[first_valid_index:].reset_index(drop=True)

# Round the infections to integers for each state
def round_variant_infections(state_data):
    variant_columns_to_round = state_data.columns[3:]  # Columns from the fourth onward (variant-specific infections)
    state_data[variant_columns_to_round] = state_data[variant_columns_to_round].fillna(0).round(0).astype(int)
    return state_data

# Process data for each state and save individual files
state_files = {}
output_dir = "state_variant_infections_min"
os.makedirs(output_dir, exist_ok=True)

for region, variant_proportions in hhs_variant_proportions_files.items():
    # Filter the estimated infections data to only include states in the current HHS region
    region_states = [state for state, hhs_region in hhs_region_mapping.items() if hhs_region == region]
    region_estimated_infections = estimated_infections[["Date"] + region_states]
    
    for state in region_states:
        # Process state data
        state_data_final = process_state_data(state, region_estimated_infections, variant_proportions, nationwide_variant_proportions)
        
        # Remove initial empty rows
        state_data_final = remove_initial_empty_rows(state_data_final)
        
        # Round the infections to integers
        state_data_final = round_variant_infections(state_data_final)
        
        # Save to dictionary
        state_files[state] = state_data_final
        
        # Save to CSV
        state_data_final.to_csv(f"{output_dir}/{state}_variant_infections_min.csv", index=False)

# Process nationwide data
nationwide_data = process_state_data("US", estimated_infections, nationwide_variant_proportions, nationwide_variant_proportions)
nationwide_data = remove_initial_empty_rows(nationwide_data)
nationwide_data = round_variant_infections(nationwide_data)

# Save nationwide data to dictionary
state_files["US"] = nationwide_data

# Save nationwide data to CSV
nationwide_data.to_csv(f"{output_dir}/US_variant_infections_min.csv", index=False)

# Create a zip file containing all CSVs
with zipfile.ZipFile('state_variant_infections_min.zip', 'w') as zipf:
    for state, df in state_files.items():
        csv_filename = f"{output_dir}/{state}_variant_infections_min.csv"
        zipf.write(csv_filename, arcname=f"{state}_variant_infections_min.csv")
