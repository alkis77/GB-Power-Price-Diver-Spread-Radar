import pandas as pd
from pathlib import Path

# === CONFIG: List all raw MID files you want to process ===
RAW_FILES = [
    "data/raw/0000036990_MID_2024.csv",   # 2024 file
    "data/raw/MID_2025.csv",              # 2025 file (rename if needed)
]
RAW_OUT  = "data/processed/intraday_trades_raw.parquet"
PROC_OUT = "data/processed/intraday_prices.parquet"

# === LOAD & CONCATENATE ALL FILES ===
dfs = []
for file in RAW_FILES:
    print(f"Loading {file} ...")
    df = pd.read_csv(file, encoding="latin-1")
    dfs.append(df)
df = pd.concat(dfs, ignore_index=True)
print(f"Loaded total CSV rows: {len(df):,}")

# === DETECT COLUMNS DYNAMICALLY ===
date_c  = next(c for c in df.columns if "date" in c.lower())
sp_c    = next(c for c in df.columns if "period" in c.lower())
price_c = next(c for c in df.columns if "price" in c.lower())
vol_c   = next(c for c in df.columns if "volume" in c.lower())

# === ADD DATETIME COLUMN ===
df["datetime"] = (
    pd.to_datetime(df[date_c], dayfirst=True, utc=True) +
    pd.to_timedelta((df[sp_c].astype(int) - 1) * 30, unit="m")
)

# === SHOW UNIQUE PERIODS ===
n_unique_dt = df["datetime"].nunique()
print(f"Unique settlement periods (datetime): {n_unique_dt}")
print(f"First 5 unique datetimes: {df['datetime'].drop_duplicates().sort_values().head(5).to_list()}")

# === SAVE TRADE-LEVEL (RAW) DATA ===
Path(RAW_OUT).parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(RAW_OUT, index=False)
print(f"✅ Saved full trade-level data to {RAW_OUT} ({len(df)} rows)")

# === AGGREGATE TO 1 ROW PER PERIOD ===
agg = (
    df.groupby("datetime").agg(
        vwap_price = (price_c, lambda x: (x * df.loc[x.index, vol_c]).sum() / df.loc[x.index, vol_c].sum()),
        mean_price = (price_c, "mean"),
        min_price  = (price_c, "min"),
        max_price  = (price_c, "max"),
        std_price  = (price_c, "std"),
        total_volume = (vol_c, "sum"),
    ).reset_index()
)
print(f"Aggregated to {len(agg)} unique settlement periods.")

# === SAVE AGGREGATED DATA ===
agg.to_parquet(PROC_OUT, index=False)
print(f"✅ Saved aggregated VWAP data to {PROC_OUT} ({len(agg)} rows)")
print("First 5 aggregated rows:\n", agg.head())
