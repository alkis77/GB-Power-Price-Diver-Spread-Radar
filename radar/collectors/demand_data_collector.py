# forecast_actual_merge.py
import pandas as pd
from pathlib import Path

# === CONFIG ===
RAW_2024 = Path("data/raw/demanddata_2024.csv")
RAW_2025 = Path("data/raw/demanddata_2025.csv")
OUT_MERGED = Path("data/processed/forecast_actual.parquet")

# === LOAD ===
df_2024 = pd.read_csv(RAW_2024)
df_2025 = pd.read_csv(RAW_2025)

# === DATE FORMAT FIX ===
df_2024["SETTLEMENT_DATE"] = pd.to_datetime(df_2024["SETTLEMENT_DATE"], format="%d-%b-%Y")
df_2025["SETTLEMENT_DATE"] = pd.to_datetime(df_2025["SETTLEMENT_DATE"], format="%Y-%m-%d")

# === CONCATENATE ===
df = pd.concat([df_2024, df_2025], ignore_index=True)

# === ADD DATETIME ===
df["datetime"] = (
    pd.to_datetime(df["SETTLEMENT_DATE"], utc=True) +
    pd.to_timedelta((df["SETTLEMENT_PERIOD"].astype(int) - 1) * 30, unit="m")
)

# === SAVE ===
OUT_MERGED.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUT_MERGED, index=False)
print(f"✅ Saved merged forecast/actual file to {OUT_MERGED}")
