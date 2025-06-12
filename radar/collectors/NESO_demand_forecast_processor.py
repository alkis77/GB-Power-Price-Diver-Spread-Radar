import pandas as pd
import os

def load_and_parse_data():
    # Set project root directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Load raw forecast CSV
    df = pd.read_csv(os.path.join(project_root, "data", "raw", "archive_1dayahead.csv"))
    print(f"Loaded {len(df):,} rows with columns: {df.columns.tolist()}")

    # Parse TARGETDATE
    df['TARGETDATE'] = pd.to_datetime(df['TARGETDATE'], errors='coerce')

    # Convert CP_ST_TIME (minutes from midnight) to a timedelta
    df['minutes'] = (df['CP_ST_TIME'].astype(int) - 1) * 30

    # Build naive datetime, then localize directly to UTC
    df['datetime'] = df['TARGETDATE'] + pd.to_timedelta(df['minutes'], unit='m')
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')

    # Cleanup
    df.drop(columns=['minutes'], inplace=True)
    
    return df

def filter_data(df):
    # Define filter window
    start = pd.Timestamp("2024-01-01T00:00:00Z")
    end = pd.Timestamp("2025-05-01T00:00:00Z")

    # Filter
    df_filt = df[(df['datetime'] >= start) & (df['datetime'] < end)].copy()

    # Print diagnostics
    print(f"Rows after filter: {len(df_filt):,}")
    print(f"Date range: {df_filt['datetime'].min()} → {df_filt['datetime'].max()}")
    unique_periods = df_filt['datetime'].nunique()
    print(f"Unique periods: {unique_periods:,}")
    expected = int((end - start).total_seconds() / 1800)
    print(f"Expected periods (30-min steps): {expected:,}")
    print(f"Missing periods: {expected - unique_periods:,}")

    return df_filt

def check_data_quality(df):
    # Duplicates
    dups = df.duplicated(subset=["datetime"]).sum()
    print(f"Duplicate datetime rows: {dups}")

    # NaNs in key column
    nan_fc = df['FORECASTDEMAND'].isna().sum()
    print(f"Missing FORECASTDEMAND values: {nan_fc}")

    # If duplicates, show a few
    if dups:
        print(df[df.duplicated(subset=["datetime"], keep=False)].head())

def clean_and_save(df):
    # Deduplicate (keep last observation if any)
    df_clean = df.sort_values("TARGETDATE").drop_duplicates(subset=["datetime"], keep="last")
    print(f"After dedupe: {len(df_clean):,} rows")

    # Keep only datetime + forecast column for merging
    df_clean = df_clean[['datetime', 'FORECASTDEMAND']].rename(columns={"FORECASTDEMAND":"forecast_MW"})

    # Save
    out = "data/processed/da_demand_forecast.parquet"
    df_clean.to_parquet(out, index=False)
    print(f"✅ Saved cleaned forecast to {out}")

def main():
    # Load and parse the data
    df = load_and_parse_data()
    
    # Filter the data
    df_filtered = filter_data(df)
    
    # Check data quality
    check_data_quality(df_filtered)
    
    # Clean and save the data
    clean_and_save(df_filtered)

if __name__ == "__main__":
    main()
