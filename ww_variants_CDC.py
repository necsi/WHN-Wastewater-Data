# Get variant proportions data from CDC

import pandas as pd
from sodapy import Socrata

client = Socrata("data.cdc.gov", None)
results = client.get("jr58-6ysp", limit=100000000, time_interval="4_week")

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)
print(results_df)

# Sort by date
results_df['week_ending'] = pd.to_datetime(results_df['week_ending'])
sort_results = results_df.sort_values(by='week_ending')

# Keep only columns 'usa_or_hhsregion', 'week_ending', 'variant', 'share' ###, 'share_hi', 'share_lo'
df_filtered = sort_results[['usa_or_hhsregion','week_ending', 'variant', 'share']]

# Ensure the 'share' column is numeric, in case there's some unexpected issue
df_filtered['share'] = pd.to_numeric(df_filtered['share'], errors='coerce')

# Make dataframe for each region
nationwide = df_filtered[df_filtered['usa_or_hhsregion'] == 'USA']
hhs1 = df_filtered[df_filtered['usa_or_hhsregion'] == '1']
hhs2 = df_filtered[df_filtered['usa_or_hhsregion'] == '2']
hhs3 = df_filtered[df_filtered['usa_or_hhsregion'] == '3']
hhs4 = df_filtered[df_filtered['usa_or_hhsregion'] == '4']
hhs5 = df_filtered[df_filtered['usa_or_hhsregion'] == '5']
hhs6 = df_filtered[df_filtered['usa_or_hhsregion'] == '6']
hhs7 = df_filtered[df_filtered['usa_or_hhsregion'] == '7']
hhs8 = df_filtered[df_filtered['usa_or_hhsregion'] == '8']
hhs9 = df_filtered[df_filtered['usa_or_hhsregion'] == '9']
hhs10 = df_filtered[df_filtered['usa_or_hhsregion'] == '10']

# Now aggregate by 'week_ending' and 'variant' using mean on the 'share' column
nationwide_agg = nationwide.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs1_agg = hhs1.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs2_agg = hhs2.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs3_agg = hhs3.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs4_agg = hhs4.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs5_agg = hhs5.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs6_agg = hhs6.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs7_agg = hhs7.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs8_agg = hhs8.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs9_agg = hhs9.groupby(['week_ending', 'variant'])['share'].mean().reset_index()
hhs10_agg = hhs10.groupby(['week_ending', 'variant'])['share'].mean().reset_index()

# Pivot the table with 'week_ending' as index and variants as columns
nationwide_pivoted = nationwide_agg.pivot(index='week_ending', columns='variant', values='share')
hhs1_pivoted = hhs1_agg.pivot(index='week_ending', columns='variant', values='share')
hhs2_pivoted = hhs2_agg.pivot(index='week_ending', columns='variant', values='share')
hhs3_pivoted = hhs3_agg.pivot(index='week_ending', columns='variant', values='share')
hhs4_pivoted = hhs4_agg.pivot(index='week_ending', columns='variant', values='share')
hhs5_pivoted = hhs5_agg.pivot(index='week_ending', columns='variant', values='share')
hhs6_pivoted = hhs6_agg.pivot(index='week_ending', columns='variant', values='share')
hhs7_pivoted = hhs7_agg.pivot(index='week_ending', columns='variant', values='share')
hhs8_pivoted = hhs8_agg.pivot(index='week_ending', columns='variant', values='share')
hhs9_pivoted = hhs9_agg.pivot(index='week_ending', columns='variant', values='share')
hhs10_pivoted = hhs10_agg.pivot(index='week_ending', columns='variant', values='share')

# Get the full date range
full_date_range = pd.date_range(start=nationwide_pivoted.index.min(), end=nationwide_pivoted.index.max(), freq='D')

# Reindex the data to include the daily date range, setting 'week_ending' as the index
nationwide_daily = nationwide_pivoted.reindex(full_date_range)
hhs1_daily = hhs1_pivoted.reindex(full_date_range)
hhs2_daily = hhs2_pivoted.reindex(full_date_range)
hhs3_daily = hhs3_pivoted.reindex(full_date_range)
hhs4_daily = hhs4_pivoted.reindex(full_date_range)
hhs5_daily = hhs5_pivoted.reindex(full_date_range)
hhs6_daily = hhs6_pivoted.reindex(full_date_range)
hhs7_daily = hhs7_pivoted.reindex(full_date_range)
hhs8_daily = hhs8_pivoted.reindex(full_date_range)
hhs9_daily = hhs9_pivoted.reindex(full_date_range)
hhs10_daily = hhs10_pivoted.reindex(full_date_range)

# Interpolate missing values
nationwide_interpolated = nationwide_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs1_interpolated = hhs1_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs2_interpolated = hhs2_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs3_interpolated = hhs3_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs4_interpolated = hhs4_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs5_interpolated = hhs5_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs6_interpolated = hhs6_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs7_interpolated = hhs7_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs8_interpolated = hhs8_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs9_interpolated = hhs9_daily.interpolate(method='linear', limit_direction='forward', axis=0)
hhs10_interpolated = hhs10_daily.interpolate(method='linear', limit_direction='forward', axis=0)

# For each column, ensure NaNs remain after the last valid value
for column in nationwide_interpolated.columns:
    # Get the last valid index for each column
    last_valid_index = nationwide_daily[column].last_valid_index()
    # Set values after the last valid index to NaN
    if last_valid_index is not None:
        nationwide_interpolated.loc[nationwide_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs1_interpolated.columns:
    last_valid_index = hhs1_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs1_interpolated.loc[hhs1_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs2_interpolated.columns:
    last_valid_index = hhs2_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs2_interpolated.loc[hhs2_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs3_interpolated.columns:
    last_valid_index = hhs3_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs3_interpolated.loc[hhs3_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs4_interpolated.columns:
    last_valid_index = hhs4_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs4_interpolated.loc[hhs4_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs5_interpolated.columns:
    last_valid_index = hhs5_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs5_interpolated.loc[hhs5_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs6_interpolated.columns:
    last_valid_index = hhs6_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs6_interpolated.loc[hhs6_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs7_interpolated.columns:
    last_valid_index = hhs7_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs7_interpolated.loc[hhs7_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs8_interpolated.columns:
    last_valid_index = hhs8_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs8_interpolated.loc[hhs8_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs9_interpolated.columns:
    last_valid_index = hhs9_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs9_interpolated.loc[hhs9_interpolated.index > last_valid_index, column] = pd.NA

for column in hhs10_interpolated.columns:
    last_valid_index = hhs10_daily[column].last_valid_index()
    if last_valid_index is not None:
        hhs10_interpolated.loc[hhs10_interpolated.index > last_valid_index, column] = pd.NA


# Save the data to csv
nationwide_interpolated.to_csv('4_week_variant_nationwide.csv', index=True)
hhs1_interpolated.to_csv('4_week_variant_hhs1.csv', index=True)
hhs2_interpolated.to_csv('4_week_variant_hhs2.csv', index=True)
hhs3_interpolated.to_csv('4_week_variant_hhs3.csv', index=True)
hhs4_interpolated.to_csv('4_week_variant_hhs4.csv', index=True)
hhs5_interpolated.to_csv('4_week_variant_hhs5.csv', index=True)
hhs6_interpolated.to_csv('4_week_variant_hhs6.csv', index=True)
hhs7_interpolated.to_csv('4_week_variant_hhs7.csv', index=True)
hhs8_interpolated.to_csv('4_week_variant_hhs8.csv', index=True)
hhs9_interpolated.to_csv('4_week_variant_hhs9.csv', index=True)
hhs10_interpolated.to_csv('4_week_variant_hhs10.csv', index=True)
