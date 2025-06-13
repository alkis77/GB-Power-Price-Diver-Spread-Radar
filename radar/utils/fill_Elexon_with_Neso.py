# notebooks/patch_elexon_with_neso.py

from pathlib import Path
import re
import pandas as pd

def find_forecast_column(df, label):
    # Add all possible column names we've seen
    candidates = {
        c for c in df.columns
        if c in {'nationalDemand', 'transmissionSystemDemand', 'nd', 'forecastDemand', 'forecast_demand'}
        or re.fullmatch(
            r"(nd|n\d*demandforecast|forecastdemand|forcastdemand|forecast_demand|nationaldemand|transmissionsystemdemand)",
            c,
            flags=re.I
        )
    }
    if not candidates:
        raise ValueError(
            f"❌  No national-demand forecast column found in {label} "
            f"(columns: {list(df.columns)[:10]} …)"
        )
    col = sorted(candidates, key=str.lower)[0]
    print(f"🛈  Using {label} column → '{col}'")
    return col

ROOT = Path(__file__).resolve().parents[2]  # Changed from parents[1] to parents[2]
PROC = ROOT / "data" / "processed"

# Add debug prints
print(f"Root directory: {ROOT}")
print(f"Processing directory: {PROC}")

# Check if directory exists
if not PROC.exists():
    raise FileNotFoundError(f"Processing directory not found at: {PROC}")

elexon_pq = PROC / "demand_forecast.parquet"
neso_pq = PROC / "da_demand_forecast.parquet"
out_pq = PROC / "demand_forecast_neso_patched.parquet"

# Check if files exist
print(f"Checking for Elexon file: {elexon_pq}")
if not elexon_pq.exists():
    raise FileNotFoundError(f"Elexon forecast file not found at: {elexon_pq}")

print(f"Checking for NESO file: {neso_pq}")
if not neso_pq.exists():
    raise FileNotFoundError(f"NESO forecast file not found at: {neso_pq}")

elexon = pd.read_parquet(elexon_pq)
neso = pd.read_parquet(neso_pq)

for df in (elexon, neso):
    if not pd.api.types.is_datetime64_any_dtype(df["datetime"]):
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    elif df["datetime"].dt.tz is None:
        df["datetime"] = df["datetime"].dt.tz_localize("UTC")
    else:
        df["datetime"] = df["datetime"].dt.tz_convert("UTC")

elexon_fc = find_forecast_column(elexon, "Elexon")
neso_fc = find_forecast_column(neso, "NESO")
elexon = elexon.rename(columns={elexon_fc: "nd"})
neso = neso.rename(columns={neso_fc: "nd"})

full_idx = pd.date_range(
    elexon["datetime"].min(), elexon["datetime"].max(), freq="30min", tz="UTC"
)
missing_dt = full_idx.difference(elexon["datetime"])
print(f"🔍 Missing half-hours in Elexon: {len(missing_dt):,}")

neso_avail = neso.set_index("datetime").reindex(missing_dt)["nd"].dropna()
print(f"🩹 NESO can supply         : {len(neso_avail):,} of those half-hours")
print(
    f"✅ Fill-rate               : {len(neso_avail) / len(missing_dt):.1%}"
    if missing_dt.size else "✅ No gaps :-)"
)

patched = pd.concat(
    [
        elexon[["datetime", "nd"]],
        pd.DataFrame({"datetime": neso_avail.index, "nd": neso_avail.values}),
    ],
    ignore_index=True,
).drop_duplicates("datetime", keep="first")

patched.sort_values("datetime", inplace=True)
patched.reset_index(drop=True).to_parquet(out_pq)
print(f"💾 Saved patched parquet → {out_pq.relative_to(ROOT)}")

full_expected = pd.date_range(
    patched["datetime"].min(), patched["datetime"].max(), freq="30min", tz="UTC"
)
print(
    f"🔧 Remaining blanks        : {len(full_expected.difference(patched['datetime'])):,}"
)
