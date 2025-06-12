"""
Scalable BMRS National Demand Forecast downloader (all columns)
===============================================================
• Endpoint : /forecast/demand/day-ahead/latest
• Strategy : 7-day chunks with retries & polite pacing
• Outputs  :
    - weekly JSON in  data/raw/forecast/
    - full parquet  in data/processed/demand_forecast.parquet
"""

from __future__ import annotations
import os, json, time, requests
from datetime import date, timedelta
from pathlib import Path
from dateutil.parser import parse as dtparse
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv

# ─── Config ────────────────────────────────────────────────────
START_DATE  = "2024-01-01"
END_DATE    = "2025-05-01"
CHUNK_DAYS  = 7          # BMRS limit
PAUSE_S     = 1.0        # polite pause between calls
MAX_RETRY   = 3
RAW_DIR     = Path("data/raw/forecast")
OUT_PARQUET = Path("data/processed/demand_forecast.parquet")

load_dotenv()
API_KEY  = os.getenv("ELEXON_SCRIPT_KEY")  # optional
BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1/forecast/demand/day-ahead/latest"

RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

# ─── Fetch helper ──────────────────────────────────────────────
def fetch_chunk(d_from: date, d_to: date, retries: int = MAX_RETRY) -> dict:
    params  = {"from": d_from.isoformat(), "to": d_to.isoformat(), "format": "json"}
    headers = {"apikey": API_KEY} if API_KEY else {}
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                raise
            print(f"   ⚠️  {d_from}–{d_to} attempt {attempt}/{retries} failed: {e}")
            time.sleep(5)

# ─── Main routine ──────────────────────────────────────────────
def main(start=START_DATE, end=END_DATE):
    d_start, d_end = dtparse(start).date(), dtparse(end).date()
    cur = d_start
    files: list[Path] = []

    total_chunks = ((d_end - d_start).days // CHUNK_DAYS) + 1
    with tqdm(total=total_chunks, unit="chunk") as bar:
        while cur <= d_end:
            d_to   = min(cur + timedelta(days=CHUNK_DAYS - 1), d_end)
            fname  = RAW_DIR / f"forecast_{cur:%Y%m%d}_{d_to:%Y%m%d}.json"

            if not fname.exists():                         # download if missing
                data = fetch_chunk(cur, d_to)
                fname.write_text(json.dumps(data), encoding="utf-8")
                time.sleep(PAUSE_S)
            files.append(fname)
            cur += timedelta(days=CHUNK_DAYS)
            bar.update(1)

    # ── Parse & concat ─────────────────────────────────────────
    frames = []
    for fp in files:
        rows = json.loads(fp.read_text(encoding="utf-8")).get("data", [])
        if not rows:
            continue
        df = pd.DataFrame(rows)
        # build UTC datetime
        df["datetime"] = (
            pd.to_datetime(df["settlementDate"], utc=True)
            + pd.to_timedelta((df["settlementPeriod"].astype(int) - 1) * 30, unit="m")
        )
        # cast numeric demand cols
        for col in ("transmissionSystemDemand", "nationalDemand"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df)

    if not frames:
        raise SystemExit("❌  No data rows parsed.")

    tidy = (pd.concat(frames, ignore_index=True)
              .drop_duplicates(subset=["datetime", "boundary"])
              .sort_values("datetime"))

    tidy.to_parquet(OUT_PARQUET, index=False)
    print(f"✓ Saved {len(tidy):,} rows → {OUT_PARQUET}")

# ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
