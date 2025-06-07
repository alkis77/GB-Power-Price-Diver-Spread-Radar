"""
NESO Historic Day-Ahead Demand Forecast Collector  (2018-present)
-----------------------------------------------------------------
* Queries the CKAN datastore via SQL for a rolling window (days_back)
* Builds a tz-aware UTC timestamp from TARGETDATE + CP_ST_TIME
* Saves:   raw JSON  -> data/raw/
           tidy Parquet -> data/processed/
"""

import json, urllib.parse, requests
from datetime import datetime, timedelta, timezone
import pandas as pd
from pathlib import Path

HIST_RESOURCE = "9847e7bb-986e-49be-8138-717b25933fbb"  # historic dataset
BASE_SQL_URL  = "https://api.neso.energy/api/3/action/datastore_search_sql"
RAW_DIR  = Path("data/raw");       RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR = Path("data/processed"); PROC_DIR.mkdir(parents=True, exist_ok=True)

def query_ckan_sql(sql: str) -> list[dict]:
    url = f"{BASE_SQL_URL}?sql={urllib.parse.quote_plus(sql)}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()["result"]["records"]

def fetch_historic(days_back: int = 180) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    sql = (
        f'SELECT * FROM "{HIST_RESOURCE}" '
        f'WHERE "TARGETDATE" >= \'{cutoff.isoformat()}\''
    )
    return query_ckan_sql(sql)

def save_raw(records: list[dict]) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    path = RAW_DIR / f"neso_hist_raw_{ts}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row) + "\n")
    return path

def tidy_df(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["TARGETDATE"]  = pd.to_datetime(df["TARGETDATE"], utc=True)
    df["CP_ST_TIME"]  = pd.to_timedelta(df["CP_ST_TIME"].astype(int), unit="m")
    df["datetime"]    = df["TARGETDATE"] + df["CP_ST_TIME"]
    df["forecast"]    = pd.to_numeric(df["FORECASTDEMAND"], errors="coerce")
    return (
        df[["datetime", "forecast"]]
        .sort_values("datetime")
        .reset_index(drop=True)
    )

def main(days_back: int = 180):
    recs = fetch_historic(days_back)
    save_raw(recs)
    tidy = tidy_df(recs)
    out = PROC_DIR / "neso_demand_forecast.parquet"
    tidy.to_parquet(out, index=False)
    print(f"✅ Saved {tidy.shape[0]:,} rows → {out}")

if __name__ == "__main__":
    main(days_back=365)      # <— change window here
