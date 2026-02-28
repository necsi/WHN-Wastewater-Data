# Purpose: To read and process the wastewater data from the US to estimate the number of newly infected individuals
import pandas as pd
import numpy as np
from sodapy import Socrata
from datetime import timedelta

# -------------------------------------------------------------------
# NEW: Pull from CDC consolidated dataset (j9g8-acpt), not the 2 old tables
# -------------------------------------------------------------------
client = Socrata("data.cdc.gov", None, timeout=180)
results_data = client.get("j9g8-acpt", limit=200000000)
nwss_raw = pd.DataFrame.from_records(results_data)

# Basic schema checks (fail fast if CDC changes names)
needed_cols = [
    "sample_collect_date",    # date of sample
    "sewershed_id",           # site identifier (old: key_plot_id)
    "wwtp_jurisdiction",      # state/jurisdiction
    "population_served",      # population served by sewershed
    "sample_matrix",          # matrix to filter influent-like samples
    "pcr_target_flowpop_lin"  # per-capita, flow+population normalized concentration
]
missing = [c for c in needed_cols if c not in nwss_raw.columns]
if missing:
    raise RuntimeError(f"CDC schema missing required columns: {missing}")

# Parse types
nwss_raw["sample_collect_date"]   = pd.to_datetime(nwss_raw["sample_collect_date"], errors="coerce")
nwss_raw["pcr_target_flowpop_lin"] = pd.to_numeric(nwss_raw["pcr_target_flowpop_lin"], errors="coerce")
nwss_raw["population_served"]      = pd.to_numeric(nwss_raw["population_served"], errors="coerce")

# -------------------------------------------------------------------
# Filter to influent-like matrices (exclude sludges/effluents)
# Keeping: raw wastewater, post grit removal
# -------------------------------------------------------------------
influent_keep = {"raw wastewater", "post grit removal"}
mat = nwss_raw["sample_matrix"].astype(str).str.lower()
nwss_raw = nwss_raw[mat.isin(influent_keep)]

# Drop rows missing core fields
nwss_raw = nwss_raw.dropna(subset=["sample_collect_date", "sewershed_id", "pcr_target_flowpop_lin"])

# -------------------------------------------------------------------
# Rename to EXACT legacy column names
#   Date, key_plot_id, gc/capita/day, State, Population
# -------------------------------------------------------------------
nwss_data = nwss_raw.rename(columns={
    "sample_collect_date":    "Date",
    "sewershed_id":           "key_plot_id",
    "pcr_target_flowpop_lin": "gc/capita/day",
    "wwtp_jurisdiction":      "State",
    "population_served":      "Population"
})[["Date", "key_plot_id", "gc/capita/day", "State", "Population"]]

# Remove negative values (defensive, should rarely occur)
nwss_data["gc/capita/day"] = nwss_data["gc/capita/day"].clip(lower=0)

# Average duplicate dates for each treatment plant (aligning with previous behavior)
nwss_data = (
    nwss_data
    .groupby(["key_plot_id", "Date"], as_index=False)
    .agg({
        "gc/capita/day": "mean",
        "Population": "mean",
        "State": "first"
    })
)

# -------------------------------------------------------------------
# Keep Erie County outlier site exclusions (unchanged)
# -------------------------------------------------------------------
outlier_plants = [
    "NWSS_ny_1012_Treatment plant_raw wastewater",
    "NWSS_ny_1013_Treatment plant_raw wastewater",
    "NWSS_ny_1000_Treatment plant_raw wastewater",
    "NWSS_ny_2178_Treatment plant_raw wastewater",
    "NWSS_ny_998_Treatment plant_raw wastewater"
]
nwss_data = nwss_data[~nwss_data["key_plot_id"].isin(outlier_plants)].reset_index(drop=True)

# -------------------------------------------------------------------
# Outlier filtering vs trailing local median
# -------------------------------------------------------------------
window = 5  
threshold_factor = 10

nwss_data["rolling_median"] = (
    nwss_data
    .sort_values(["key_plot_id", "Date"])
    .groupby("key_plot_id")["gc/capita/day"]
    .transform(lambda x: x.rolling(window=window, min_periods=3).median())
)

# Only flag where rolling_median is available
nwss_data["is_outlier"] = (
    nwss_data["rolling_median"].notna()
    & (nwss_data["gc/capita/day"] > threshold_factor * nwss_data["rolling_median"])
)

nwss_data = nwss_data[~nwss_data["is_outlier"]].drop(columns=["is_outlier", "rolling_median"])

# -------------------------------------------------------------------
# Extend recent sites to the overall most recent date; then interpolate (unchanged)
# -------------------------------------------------------------------
overall_most_recent_date = nwss_data["Date"].max()

def reindex_and_interpolate(df, overall_most_recent_date):
    last_reported_date = df["Date"].max()
    two_weeks = timedelta(weeks=2)
    if overall_most_recent_date - last_reported_date <= two_weeks:
        date_range = pd.date_range(start=df["Date"].min(), end=overall_most_recent_date)
    else:
        date_range = pd.date_range(start=df["Date"].min(), end=df["Date"].max())
    df = (
        df.set_index("Date")
          .reindex(date_range)
          .interpolate(method="linear")
          .reset_index()
          .rename(columns={"index": "Date"})
    )
    df["key_plot_id"] = df["key_plot_id"].ffill()
    df["State"]       = df["State"].ffill()
    df["Population"]  = df["Population"].ffill()
    return df

nwss_data_interpolated = (
    nwss_data
    .groupby("key_plot_id", group_keys=False)
    .apply(lambda d: reindex_and_interpolate(d, overall_most_recent_date))
    .reset_index(drop=True)
)

# --- STATE + NATIONAL AGGREGATION (keeps legacy column names) ---

# --- Normalize State from 2-letter codes to full names (to match legacy population dict) ---
abbr_to_name = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado",
    "CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho",
    "IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana",
    "ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
    "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey",
    "NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota",
    "TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
    "DC":"District of Columbia"  # only if present
}

# Map codes → names where applicable; leave as-is if already full name or unmatched
nwss_data_interpolated["State"] = (
    nwss_data_interpolated["State"]
      .astype(str).str.strip().str.upper()
      .map(abbr_to_name)
      .fillna(nwss_data_interpolated["State"])
)


# Aggregate data by state (population-weighted site signal)
state_aggregated = nwss_data_interpolated.groupby(['State', 'Date']).apply(
    lambda x: (x['gc/capita/day'] * x['Population']).sum() / x['Population'].sum()
).reset_index(name='Weighted_gc/capita/day')

# Count contributing plants and calculate population coverage
contributing_plants_count = (
    nwss_data_interpolated
    .groupby(['State', 'Date'])
    .size()
    .reset_index(name='Contributing_Plants')
)

state_population_covered = (
    nwss_data_interpolated
    .groupby(['State', 'Date'])['Population']
    .sum()
    .reset_index(name='Population_Covered')
)

# Merge the counts and population coverage with state aggregated data
state_aggregated_with_counts = pd.merge(
    state_aggregated, contributing_plants_count, on=['State', 'Date'], how='left'
)
state_aggregated_with_full_population = pd.merge(
    state_aggregated_with_counts, state_population_covered, on=['State', 'Date'], how='left'
)

# Load state population estimates (keep identical to legacy)
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

state_population_df = pd.DataFrame(
    list(state_population_estimates.items()), columns=['State', 'State_Population']
)

state_aggregated_with_full_population = pd.merge(
    state_aggregated_with_full_population, state_population_df, on='State', how='left'
)

state_aggregated_with_full_population['Percentage_Covered'] = (
    state_aggregated_with_full_population['Population_Covered']
    / state_aggregated_with_full_population['State_Population'] * 100
)

# Apply a centered 7-day rolling average to smooth the data (per state)
state_aggregated_with_full_population['Smoothed_gc/capita/day'] = (
    state_aggregated_with_full_population
    .groupby('State')['Weighted_gc/capita/day']
    .transform(lambda x: x.rolling(window=7, center=True, min_periods=1).mean())
)

# Calculate the national weighted average from state-level aggregates
national_aggregated = state_aggregated_with_full_population.groupby('Date').apply(
    lambda x: (x['Weighted_gc/capita/day'] * x['Population_Covered']).sum()
              / x['Population_Covered'].sum()
).reset_index(name='National_Weighted_gc/capita/day')

national_aggregated['National_Smoothed_gc/capita/day'] = (
    national_aggregated['National_Weighted_gc/capita/day']
    .rolling(window=7, center=True, min_periods=1).mean()
)

# Contributing plants & coverage (sum over states per day)
national_aggregated['Contributing_Plants'] = (
    state_aggregated_with_full_population
    .groupby('Date')['Contributing_Plants']
    .sum()
    .values
)
national_aggregated['Population_Covered'] = (
    state_aggregated_with_full_population
    .groupby('Date')['Population_Covered']
    .sum()
    .values
)

# Constant total US population (sum of the per-state constants)
national_aggregated['State_Population'] = state_population_df['State_Population'].sum()

national_aggregated['Percentage_Covered'] = (
    national_aggregated['Population_Covered']
    / national_aggregated['State_Population'] * 100
)

# Rename to match state-level column names and tag as Nationwide
national_aggregated = national_aggregated.rename(columns={
    'National_Weighted_gc/capita/day': 'Weighted_gc/capita/day',
    'National_Smoothed_gc/capita/day': 'Smoothed_gc/capita/day'
})
national_aggregated['State'] = 'Nationwide'

# Merge national + state-level rows (same schema as before)
merged_data = pd.concat(
    [state_aggregated_with_full_population, national_aggregated],
    ignore_index=True, sort=False
)

# Load the Biobot data (unchanged)
biobot_file_path = 'United_States_states_min.csv'
biobot_data = pd.read_csv(biobot_file_path)
biobot_data['Date'] = pd.to_datetime(biobot_data['Date'], errors='coerce')

# --- PART 3: Biobot-based conversion factors (keep legacy behavior) ---

# Ensure dates are correctly formatted in Biobot data
biobot_data['Date'] = pd.to_datetime(biobot_data['Date'], errors='coerce')

# Filter the Biobot data to include only the 'inf' measure
biobot_data_inf = biobot_data[biobot_data['Measure'] == 'inf'].copy()

# State abbreviations → full names (Biobot 'Region' carries 2-letter codes or 'Nationwide')
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

# Convert state abbreviations to full names in Biobot INF data (keep 'Nationwide' untouched)
biobot_data_inf = biobot_data_inf.assign(
    State = biobot_data_inf['Region'].map(state_abbreviations).fillna(biobot_data_inf['Region'])
)

# Helper: last-4-months window per state (Colorado = 109 days as exception)
def filter_dates(state, df):
    if state == 'Colorado':
        return df[df['Date'] > (df['Date'].max() - pd.DateOffset(days=109))]
    else:
        return df[df['Date'] > (df['Date'].max() - pd.DateOffset(months=4))]

biobot_data_last_months = (
    biobot_data_inf
    .groupby('State', group_keys=False)
    .apply(lambda x: filter_dates(x.name, x))
    .reset_index(drop=True)
)

# ---- Conversion factors: NWSS (smoothed gc/capita/day) ÷ Biobot INF ----
state_conversion_factors = {}
for state, group in biobot_data_last_months.groupby('State'):
    nwss_state_data = merged_data[
        (merged_data['State'] == state) &
        (merged_data['Date'].isin(group['Date']))
    ]
    if not nwss_state_data.empty:
        merged_state_data = pd.merge(
            nwss_state_data, group[['Date','Value']], on='Date', how='inner'
        )
        merged_state_data['NWSS/Biobot_inf'] = (
            merged_state_data['Smoothed_gc/capita/day'] / merged_state_data['Value']
        )
        conv_fact = merged_state_data['NWSS/Biobot_inf'].mean()
        state_conversion_factors[state] = conv_fact
        print(f"State: {state}, Conversion Factor: {conv_fact}")

# Nationwide conversion factor (INF)
biobot_nationwide_last_4_months = biobot_data_inf[
    (biobot_data_inf['State'] == 'Nationwide') &
    (biobot_data_inf['Date'] > (biobot_data_inf['Date'].max() - pd.DateOffset(months=4)))
]
nwss_nationwide_data = merged_data[
    (merged_data['State'] == 'Nationwide') &
    (merged_data['Date'].isin(biobot_nationwide_last_4_months['Date']))
]
if not nwss_nationwide_data.empty:
    merged_nationwide_data = pd.merge(
        nwss_nationwide_data, biobot_nationwide_last_4_months[['Date','Value']], on='Date', how='inner'
    )
    merged_nationwide_data['NWSS/Biobot_inf'] = (
        merged_nationwide_data['Smoothed_gc/capita/day'] / merged_nationwide_data['Value']
    )
    conv_fact_nationwide = merged_nationwide_data['NWSS/Biobot_inf'].mean()
    state_conversion_factors['Nationwide'] = conv_fact_nationwide
    print(f"Nationwide Conversion Factor: {conv_fact_nationwide}")

# ---- Wastewater → NWSS alignment for historic wastewater Biobot series ----
biobot_data_wastewater = biobot_data[biobot_data['Measure'] == 'wastewater'].copy()
biobot_data_wastewater = biobot_data_wastewater.assign(
    State = biobot_data_wastewater['Region'].map(state_abbreviations).fillna(biobot_data_wastewater['Region'])
)

biobot_data_last_months_wastewater = (
    biobot_data_wastewater
    .groupby('State', group_keys=False)
    .apply(lambda x: filter_dates(x.name, x))
    .reset_index(drop=True)
)

state_conversion_factors_wastewater = {}
for state, group in biobot_data_last_months_wastewater.groupby('State'):
    nwss_state_data = merged_data[
        (merged_data['State'] == state) &
        (merged_data['Date'].isin(group['Date']))
    ]
    if not nwss_state_data.empty:
        merged_state_data = pd.merge(
            nwss_state_data, group[['Date','Value']], on='Date', how='inner'
        )
        merged_state_data['NWSS/Biobot_wastewater'] = (
            merged_state_data['Smoothed_gc/capita/day'] / merged_state_data['Value']
        )
        conv_fact_wastewater = merged_state_data['NWSS/Biobot_wastewater'].mean()
        state_conversion_factors_wastewater[state] = conv_fact_wastewater
        print(f"State: {state}, Wastewater Conversion Factor: {conv_fact_wastewater}")

# Nationwide wastewater conversion factor
biobot_nationwide_last_4_months_wastewater = biobot_data_wastewater[
    (biobot_data_wastewater['State'] == 'Nationwide') &
    (biobot_data_wastewater['Date'] > (biobot_data_wastewater['Date'].max() - pd.DateOffset(months=4)))
]
nwss_nationwide_data = merged_data[
    (merged_data['State'] == 'Nationwide') &
    (merged_data['Date'].isin(biobot_nationwide_last_4_months_wastewater['Date']))
]
if not nwss_nationwide_data.empty:
    merged_nationwide_data = pd.merge(
        nwss_nationwide_data, biobot_nationwide_last_4_months_wastewater[['Date','Value']],
        on='Date', how='inner'
    )
    merged_nationwide_data['NWSS/Biobot_wastewater'] = (
        merged_nationwide_data['Smoothed_gc/capita/day'] / merged_nationwide_data['Value']
    )
    conv_fact_nationwide_wastewater = merged_nationwide_data['NWSS/Biobot_wastewater'].mean()
    state_conversion_factors_wastewater['Nationwide'] = conv_fact_nationwide_wastewater
    print(f"Nationwide Wastewater Conversion Factor: {conv_fact_nationwide_wastewater}")

# Apply the mapping to the 'Region' column (keep legacy behavior)
biobot_data = biobot_data.copy()
biobot_data['Region'] = biobot_data['Region'].map(state_abbreviations).fillna(biobot_data['Region'])

# Step 2: Replace 'Value' for 'wastewater' measure using the conversion factors
def apply_conversion(row):
    if row['Measure'] == 'wastewater':
        conversion_factor = state_conversion_factors_wastewater.get(row['Region'], 1)  # default 1 if missing
        return row['Value'] * conversion_factor
    return row['Value']

biobot_data['Value'] = biobot_data.apply(apply_conversion, axis=1)

# ---- Data-quality gating / filtered states (unchanged thresholds) ----
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
        merged_state_data = pd.merge(
            nwss_state_data[['Date', 'Smoothed_gc/capita/day']], 
            biobot_state_data[['Date', 'Value']], 
            on='Date', how='inner'
        )
        correlation = merged_state_data['Smoothed_gc/capita/day'].corr(merged_state_data['Value'])
        if pd.notna(correlation) and correlation >= 0.5:
            filtered_states.append(state)

        print(
            f"State: {state}, Conversion Factor: {state_conversion_factors.get(state, float('nan'))}, "
            f"Dates in Last 4 Months: {dates_in_last_4_months.min()} to {dates_in_last_4_months.max()}, "
            f"Min Contributing Plants: {min_contributing_plants}, Max Contributing Plants: {max_contributing_plants}, "
            f"Min Population Covered: {min_population_covered}, Max Population Covered: {max_population_covered}, "
            f"Correlation: {correlation}"
        )

# --- PART 4: Build final WHN files (United_States_wwb.{csv,json}) and Joe_EstimatedInfections.csv ---

# Prepare the final dataset
final_rows = []

# Process nationwide data
nationwide_data = merged_data[merged_data['State'] == 'Nationwide'].copy()
last_date_biobot_nationwide = biobot_data_inf[biobot_data_inf['Region'] == 'Nationwide']['Date'].max()
nationwide_data_filtered = nationwide_data[nationwide_data['Date'] > last_date_biobot_nationwide]

for _, row in nationwide_data_filtered.iterrows():
    final_rows.append({
        'Country': 'United_States',
        'Region': 'Nationwide',
        'Date': row['Date'],
        'Measure': 'wastewater',
        'Value': row['Smoothed_gc/capita/day']
    })
    final_rows.append({
        'Country': 'United_States',
        'Region': 'Nationwide',
        'Date': row['Date'],
        'Measure': 'inf',
        'Value': row['Smoothed_gc/capita/day'] / state_conversion_factors['Nationwide']
    })

# Process each filtered state
for state in filtered_states:
    state_data = merged_data[merged_data['State'] == state].copy()
    last_date_biobot_state = biobot_data_inf[biobot_data_inf['State'] == state]['Date'].max()
    state_data_filtered = state_data[state_data['Date'] > last_date_biobot_state]

    for _, row in state_data_filtered.iterrows():
        final_rows.append({
            'Country': 'United_States',
            'Region': state,
            'Date': row['Date'],
            'Measure': 'wastewater',
            'Value': row['Smoothed_gc/capita/day']
        })
        final_rows.append({
            'Country': 'United_States',
            'Region': state,
            'Date': row['Date'],
            'Measure': 'inf',
            'Value': row['Smoothed_gc/capita/day'] / state_conversion_factors[state]
        })

# Filter Biobot to only filtered states + Nationwide (keeps legacy content)
biobot_data_filtered = biobot_data[biobot_data['Region'].isin(filtered_states + ['Nationwide'])].copy()

# Convert final rows to DataFrame and append to Biobot
final_data = pd.DataFrame(final_rows)
final_merged_data = pd.concat([biobot_data_filtered, final_data], ignore_index=True)

# Save WHN wastewater+inf dataset exactly as before
final_merged_data.to_csv('United_States_min_wwb.csv', index=False)
final_merged_data.to_json('United_States_min_wwb.json', orient='records')

# -----------------------
# For Joe (MAPS) — pivoted infections by region
# -----------------------

# Step 1: Keep only rows where "Measure" is "inf"
df_inf = final_merged_data[final_merged_data['Measure'] == 'inf'].copy()

# Step 2: Remove the "Country" and "Measure" columns
df_inf = df_inf.drop(columns=['Country', 'Measure'])

# Step 3: Existing regions present
existing_regions = df_inf['Region'].unique()

# Define the nationwide population
nationwide_population = sum(state_population_estimates.values())

# State -> 2-letter abbreviations
state_abbreviations2 = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
    'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT',
    'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

# Step 4: For states not in dataset, apportion Nationwide by state population
nationwide_data_inf = df_inf[df_inf['Region'] == 'Nationwide'].copy()
for state, pop in state_population_estimates.items():
    if state not in existing_regions:
        state_infections = nationwide_data_inf['Value'] * (pop / nationwide_population)
        state_abbreviation2 = state_abbreviations2[state]
        new_state_data = pd.DataFrame({
            'Date': nationwide_data_inf['Date'],
            'Region': state_abbreviation2,
            'Value': state_infections
        })
        df_inf = pd.concat([df_inf, new_state_data], ignore_index=True)

# Step 5: Convert states to two-letter abbreviations where necessary
df_inf['Region'] = df_inf['Region'].replace(state_abbreviations2)

# Step 6: Pivot so Nationwide first, then states A–Z
df_pivot = df_inf.pivot(index='Date', columns='Region', values='Value').sort_index()
cols = ['Nationwide'] + sorted([c for c in df_pivot.columns if c != 'Nationwide'])
df_pivot = df_pivot[cols]

# Step 7: Save Joe’s file
df_pivot.to_csv('Joe_EstimatedInfections_min.csv')

print("Final dataset generated and saved: United_States_wwb.csv/.json and Joe_EstimatedInfections.csv")


