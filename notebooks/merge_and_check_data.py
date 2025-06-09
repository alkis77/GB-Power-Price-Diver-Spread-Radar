import pandas as pd

PROC = "data/processed"
fn_forecast = f"{PROC}/neso_demand_forecast.parquet"
fn_actual   = f"{PROC}/demand_actual.parquet"
fn_mip      = f"{PROC}/intraday_prices.parquet"
fn_imb      = f"{PROC}/imbalance_prices.parquet"

# Load data
df_forecast = pd.read_parquet(fn_forecast)
df_actual   = pd.read_parquet(fn_actual)
df_mip      = pd.read_parquet(fn_mip)
df_imb      = pd.read_parquet(fn_imb)

# Clean forecast: keep latest forecast per period
df_forecast_clean = df_forecast.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")

# Ensure datetime is a column (not index)
for df in [df_forecast_clean, df_actual, df_mip, df_imb]:
    if df.index.name == "datetime":
        df.reset_index(inplace=True)

# Print coverage for each file
for name, df in [
    ("Forecast", df_forecast_clean),
    ("Actual", df_actual),
    ("MIP", df_mip),
    ("Imbalance", df_imb)
]:
    print(f"{name} coverage: {df['datetime'].min()} → {df['datetime'].max()} (n={len(df)})")

# Find intersection window (latest start, earliest end)
start_dates = [df['datetime'].min() for df in [df_forecast_clean, df_actual, df_mip, df_imb]]
end_dates = [df['datetime'].max() for df in [df_forecast_clean, df_actual, df_mip, df_imb]]
window_start = max(start_dates)
window_end = min(end_dates)
print(f"\nCommon window: {window_start} → {window_end}")

# Filter each df to the common window
def filter_window(df, col="datetime"):
    return df[(df[col] >= window_start) & (df[col] <= window_end)].copy()

df_forecast_win = filter_window(df_forecast_clean)
df_actual_win   = filter_window(df_actual)
df_mip_win      = filter_window(df_mip)
df_imb_win      = filter_window(df_imb)

# Set index for merging
for df in [df_forecast_win, df_actual_win, df_mip_win, df_imb_win]:
    df.set_index("datetime", inplace=True)

# Standardize columns
df_forecast_win = df_forecast_win.rename(columns={"forecast": "forecast_MW"})
df_actual_win = df_actual_win.rename(columns={"demand_MW": "actual_MW"})
df_mip_win = df_mip_win.rename(columns={"price": "mip_price"})

# Merge outer (so you can see missingness)
df_merged = df_forecast_win.join(
    [df_actual_win, df_mip_win, df_imb_win], how="outer"
)

# Check missingness after filtering to overlap
print("\nMissing value summary in overlap:")
missing = df_merged.isna().sum()
for col in df_merged.columns:
    print(f"  {col:12}: {missing[col]:6} missing ({missing[col] / len(df_merged) * 100:.1f}%)")

core_cols = ["forecast_MW", "actual_MW", "mip_price", "sbp", "ssp"]
df_merged['usable'] = df_merged[core_cols].notna().all(axis=1)
n_usable = df_merged['usable'].sum()
print(f"\nRows with all core data present in overlap: {n_usable} ({n_usable/len(df_merged)*100:.1f}%)")

# Save this new merged file for full analysis in next steps
df_merged.reset_index().to_parquet(f"{PROC}/merged_core_overlap.parquet", index=False)
print(f"✅ Saved merged overlap file to {PROC}/merged_core_overlap.parquet")
