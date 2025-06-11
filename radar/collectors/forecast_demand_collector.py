"""
ELEXON/BMRS Demand Forecast Collector (ND Forecast)
---------------------------------------------------
* Uses the portal's ND_FC_FILE (scripting key)
* Caches raw CSV (<24h) to data/raw/
* Processes to data/processed/demand_forecast.parquet
"""

import os, sys, time, requests, pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

# --- Config ---
load_dotenv()
KEY = os.getenv("ELEXON_SCRIPT_KEY")
if not KEY or len(KEY) != 15:
    sys.exit("❌ ELEXON_SCRIPT_KEY missing or wrong length (15 chars)")

RAW = Path("data/raw"); RAW.mkdir(parents=True, exist_ok=True)
PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

# See all available files here: https://www.elexon.co.uk/guidance-note/file-naming-conventions/
URL = f"https://downloads.elexonportal.co.uk/file/download/ND_FC_FILE?key={KEY}"

def download_ndfc() -> Path:
    """Download latest ND_FC_FILE (National Demand Forecast) from Elexon portal (cache <24h)."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    raw_path = RAW / f"ndfc_{today}.csv"
    if raw_path.exists() and (time.time() - raw_path.stat().st_mtime) < 86400:
        print(f"✓ Using cached copy: {raw_path}")
        return raw_path

    sess = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504], allowed_methods=["GET"])
    sess.mount("https://", HTTPAdapter(max_retries=retries))

    print(f"\nFetching ND_FC_FILE from Portal → {URL}")
    r = sess.get(URL, timeout=(10,300))
    r.raise_for_status()
    if b"Scripting Error" in r.content[:100] or not r.content.strip():
        raise RuntimeError("⚠️ Portal returned error or empty ND_FC file")

    raw_path.write_bytes(r.content)
    print(f"✓ Downloaded ND_FC_FILE → {raw_path}")
    return raw_path

def tidy(raw_csv: Path, start_date=None, end_date=None) -> pd.DataFrame:
    """Process raw National Demand Forecast CSV to tidy parquet."""
    df = pd.read_csv(raw_csv, encoding="latin-1")

    # Auto-detect columns
    date_c = next(c for c in df.columns if "date" in c.lower())
    sp_c   = next(c for c in df.columns if "period" in c.lower())
    forecast_c = next((c for c in df.columns if ("forecast" in c.lower() and "demand" in c.lower()) or c.upper() == "ND_FC"), None)
    if forecast_c is None:
        forecast_c = next(c for c in df.columns if "demand" in c.lower())
    
    # Add datetime column (UTC)
    df["datetime"] = (
        pd.to_datetime(df[date_c], dayfirst=True, utc=True) +
        pd.to_timedelta((df[sp_c].astype(int) - 1) * 30, unit="m")
    )
    df["forecast"] = pd.to_numeric(df[forecast_c], errors="coerce")

    # Filter by date range if specified
    if start_date and end_date:
        st = pd.Timestamp(start_date, tz="UTC")
        en = pd.Timestamp(end_date, tz="UTC")
        df = df[(df["datetime"] >= st) & (df["datetime"] <= en)]

    # Only keep datetime and forecast for merging, but keep all columns in file
    return df.sort_values("datetime")

def main(start_date="2024-01-01", end_date="2025-05-01"):
    raw = download_ndfc()
    clean = tidy(raw, start_date, end_date)
    if clean.empty:
        sys.exit("❌ No demand forecast data in the requested range!")
    out = PROC / "demand_forecast.parquet"
    clean.to_parquet(out, index=False)
    print(f" Demand Forecast range: {clean['datetime'].min()} → {clean['datetime'].max()}")
    print(f" Settlement periods: {len(clean):,}")
    print(f" Saved → {out}")

if __name__ == "__main__":
    main()
