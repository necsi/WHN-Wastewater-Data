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
    """Merge state infections with HHS variant proportions; per-lineage fill from nationwide where HHS is NaN."""
    # Select infections column for the state (or Nationwide)
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

    # Fill each lineage separately from nationwide if HHS is NaN
    for v in variant_columns:
        merged[v] = merged[v].fillna(nw[v])

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

# ------------------------
# Run for all states by region
# ------------------------
state_files = {}
output_dir = "state_variant_infections_min"
os.makedirs(output_dir, exist_ok=True)

for region, variant_df in hhs_variant_proportions_files.items():
    # States in this HHS region
    region_states = [s for s, h in hhs_region_mapping.items() if h == region]
    # Only keep those columns from estimated infections
    region_est_inf = estimated_infections[["Date"] + region_states]

    for state in region_states:
        df = process_state_data(state, region_est_inf, variant_df, nationwide_variant_proportions)
        df = remove_initial_empty_rows(df)
        df = round_variant_infections(df)
        state_files[state] = df
        df.to_csv(f"{output_dir}/{state}_variant_infections_min.csv", index=False)

# ------------------------
# Nationwide (US)
# ------------------------
us_df = process_state_data("US", estimated_infections, nationwide_variant_proportions, nationwide_variant_proportions)
us_df = remove_initial_empty_rows(us_df)
us_df = round_variant_infections(us_df)
state_files["US"] = us_df
us_df.to_csv(f"{output_dir}/US_variant_infections_min.csv", index=False)

# ------------------------
# Zip everything
# ------------------------
with zipfile.ZipFile('state_variant_infections_min.zip', 'w') as zipf:
    for state in state_files.keys():
        csv_filename = f"{output_dir}/{state}_variant_infections_min.csv"
        zipf.write(csv_filename, arcname=f"{state}_variant_infections_min.csv")
