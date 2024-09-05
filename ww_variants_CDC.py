# Get variant proportions data from CDC

import pandas as pd
from sodapy import Socrata

client = Socrata("data.cdc.gov", None)
results = client.get("jr58-6ysp", limit=1000000 ,usa_or_hhsregion="USA", time_interval="biweekly", modeltype="weighted")

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

# Sort by date
results_df['week_ending'] = pd.to_datetime(results_df['week_ending'])
sort_results = results_df.sort_values(by='week_ending')

# Keep only columns 'usa_or_hhsregion', 'week_ending', 'variant', 'share' ###, 'share_hi', 'share_lo'
df_filtered = sort_results[['week_ending', 'variant', 'share']]

# Ensure the 'share' column is numeric, in case there's some unexpected issue
df_filtered['share'] = pd.to_numeric(df_filtered['share'], errors='coerce')

# Now aggregate by 'week_ending' and 'variant' using mean on the 'share' column
df_filtered_agg = df_filtered.groupby(['week_ending', 'variant'])['share'].mean().reset_index()

# Pivot the table with 'week_ending' as index and variants as columns
df_pivoted_agg = df_filtered_agg.pivot(index='week_ending', columns='variant', values='share')

df_pivoted_agg.to_csv('weighted_biweekly_variant_joe.csv', index=True)
