import pandas as pd
import numpy as np
import zipfile
import os

# ------------------------
# Load the data files
# ------------------------
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
    10: pd.read_csv('4_week_variant_hhs10.csv'),
}

# ------------------------
# HHS region mapping
# ------------------------
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

DATE_COL = "Unnamed: 0"  # date column name in the variant CSVs

# ------------------------
# Variant column handling
# ------------------------
# Build canonical list of variants from nationwide columns, excluding date and 'NA'
variant_columns = [c for c in nationwide_variant_proportions.columns if c not in (DATE_COL, 'NA')]

def align_variant_columns(df, variant_cols, date_col=DATE_COL):
    """Ensure df has exactly [date_col] + variant_cols; add missing as NaN and coerce to numeric."""
    # Add any missing lineage columns
    for c in variant_cols:
        if c not in df.columns:
            df[c] = np.nan
    # Keep only date + variant cols, in order
    df = df[[date_col] + variant_cols]
    # Coerce numeric (in case of strings)
    df[variant_cols] = df[variant_cols].apply(pd.to_numeric, errors='coerce')
    return df

# Align nationwide and every HHS table to the same lineage set/order
nationwide_variant_proportions = align_variant_columns(nationwide_variant_proportions, variant_columns, DATE_COL)
for r, df in list(hhs_variant_proportions_files.items()):
    hhs_variant_proportions_files[r] = align_variant_columns(df, variant_columns, DATE_COL)

# ------------------------
# Core processing
# ------------------------

def process_state_data(state, estimated_infections_df, hhs_variant_df, nationwide_proportions_df):
    """Merge state infections with HHS variant proportions; fall back to nationwide only when the whole HHS row is missing."""    # Select infections column for the state (or Nationwide)
    if state == "US":
        state_data = estimated_infections_df[["Date", "Nationwide"]].rename(columns={"Nationwide": "State_Infections"})
    else:
        state_data = estimated_infections_df[["Date", state]].rename(columns={state: "State_Infections"})
    state_data = state_data.dropna(subset=["State_Infections"])

    # Merge HHS-specific variant proportions (by date)
    merged = pd.merge(
        state_data, hhs_variant_df,
        left_on="Date", right_on=DATE_COL, how="left"
    ).drop(columns=[DATE_COL])

    # Nationwide fallback (merge on the same dates)
    nw = pd.merge(
        merged[["Date"]], nationwide_proportions_df,
        left_on="Date", right_on=DATE_COL, how="left"
    ).drop(columns=[DATE_COL])

    # Discontinued previous approach: Fill each lineage separately from nationwide if HHS is NaN
    # for v in variant_columns:
    #    merged[v] = merged[v].fillna(nw[v])
    # Previous approach end


    # GISAID-like row-level fallback:
    # If an HHS row/date has any variant data, use that HHS row as the complete source.
    # Only if the whole HHS row/date is missing, replace the whole row with nationwide values.
    hhs_has_any_data = merged[variant_columns].notna().any(axis=1)
    missing_hhs_row = ~hhs_has_any_data
    merged.loc[missing_hhs_row, variant_columns] = nw.loc[missing_hhs_row, variant_columns].to_numpy()

    # Do not fill individual missing HHS lineages from nationwide. Treat them as 0,
    # then normalize the selected row so the variant proportions sum to 1 when data exists.
    merged[variant_columns] = merged[variant_columns].fillna(0)
    row_sums = merged[variant_columns].sum(axis=1)
    has_props = row_sums > 0
    merged.loc[has_props, variant_columns] = merged.loc[has_props, variant_columns].div(row_sums[has_props], axis=0)

    # Compute variant-specific infections
    for v in variant_columns:
        merged[v] = merged["State_Infections"] * merged[v]
    

    # Final tidy frame
    out = merged[["Date"]].copy()
    out["Region"] = state
    out["Total_State_Infections"] = merged["State_Infections"].round(0).astype("Int64")
    out = pd.concat([out, merged[variant_columns]], axis=1)

    # Column order / names
    out.columns = ["Date", "Region", "Total_State_Infections"] + variant_columns
    return out


def remove_initial_empty_rows(state_df):
    """Trim leading rows where both Total_State_Infections and all variants are NaN."""
    if state_df.empty:
        return state_df
    has_any = state_df["Total_State_Infections"].notna() | state_df[variant_columns].notna().any(axis=1)
    if not has_any.any():
        return state_df  # nothing valid—return as is
    first_idx = has_any.idxmax()
    return state_df.loc[first_idx:].reset_index(drop=True)

def round_variant_infections(state_df):
    """Round all variant infection columns to ints (keep NaNs as 0)."""
    if state_df.empty:
        return state_df
    to_round = state_df.columns[3:]  # from the 4th column onward
    state_df[to_round] = state_df[to_round].fillna(0).round(0).astype(int)
    return state_df

def remove_trailing_zero_variant_rows(state_df):
    """Trim trailing rows where all variant infection columns are 0."""
    if state_df.empty:
        return state_df
    variant_cols = state_df.columns[3:]  # keep Date, Region, Total_State_Infections untouched
    has_variant_cases = state_df[variant_cols].fillna(0).ne(0).any(axis=1)
    if not has_variant_cases.any():
        return state_df.iloc[0:0].copy()
    last_idx = has_variant_cases[has_variant_cases].index[-1]
    return state_df.loc[:last_idx].reset_index(drop=True)

def sort_variant_columns_by_current_relevance(state_df):
    """Sort variant columns so the largest current contributor is leftmost.

    Tie-breaker: variants last seen more recently are placed farther left.
    Final tie-breaker: variant name, for stable/reproducible output.
    """
    if state_df.empty:
        return state_df

    fixed_cols = list(state_df.columns[:3])
    variant_cols = list(state_df.columns[3:])

    def sort_key(col):
        values = state_df[col].fillna(0)
        current_cases = values.iloc[-1]
        nonzero_positions = np.flatnonzero(values.to_numpy() != 0)
        last_seen = int(nonzero_positions[-1]) if len(nonzero_positions) else -1
        return (current_cases, last_seen, col)

    sorted_variant_cols = sorted(variant_cols, key=sort_key, reverse=True)
    return state_df[fixed_cols + sorted_variant_cols]

state_files = {}

for region, variant_df in hhs_variant_proportions_files.items():
    # States in this HHS region
    region_states = [s for s, h in hhs_region_mapping.items() if h == region]
    # Only keep those columns from estimated infections
    region_est_inf = estimated_infections[["Date"] + region_states]

    for state in region_states:
        df = process_state_data(state, region_est_inf, variant_df, nationwide_variant_proportions)
        df = remove_initial_empty_rows(df)
        df = round_variant_infections(df)
        df = remove_trailing_zero_variant_rows(df)
        df = sort_variant_columns_by_current_relevance(df)
        state_files[state] = df

# ------------------------
# Nationwide (US)
# ------------------------
us_df = process_state_data("US", estimated_infections, nationwide_variant_proportions, nationwide_variant_proportions)
us_df = remove_initial_empty_rows(us_df)
us_df = round_variant_infections(us_df)
us_df = remove_trailing_zero_variant_rows(us_df)
us_df = sort_variant_columns_by_current_relevance(us_df)
state_files["US"] = us_df

# ------------------------
# Output folder + files
# ------------------------
output_date = pd.to_datetime(us_df["Date"].iloc[-1]).strftime("%m-%d-%Y")
output_dir = f"variant_infections_CDC_{output_date}_min"
zip_filename = f"{output_dir}.zip"
os.makedirs(output_dir, exist_ok=True)

for state, df in state_files.items():
    df.to_csv(f"{output_dir}/{state}_variant_infections_min.csv", index=False)

# ------------------------
# Zip everything
# ------------------------
with zipfile.ZipFile(zip_filename, 'w') as zipf:
    for state in state_files.keys():
        csv_filename = f"{output_dir}/{state}_variant_infections_min.csv"
        zipf.write(csv_filename, arcname=f"{state}_variant_infections_min.csv")
