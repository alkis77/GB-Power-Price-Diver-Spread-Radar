"""
ROBUST MID (MIP) COLLECTOR
--------------------------
• Tries three Elexon-portal URL patterns until one succeeds:
      1) /file/download/MID_<YEAR>.csv?key=...
      2) /file/download?key=...&filename=MID_<YEAR>.csv
      3) /file/download/LATEST_MID_FILE?key=...
• Rejects bodies that start with 'Scripting Error'.
• Auto-detects column names, builds tz-aware UTC timestamps, computes VWAP.
• Saves raw CSV (audit) + tidy Parquet (analytics).
"""

import os, sys, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

# ── Config & key check ────────────────────────────────────────────────────────
load_dotenv()
KEY = os.getenv("ELEXON_SCRIPT_KEY")
if not KEY or len(KEY) != 15:
    sys.exit("❌  ELEXON_SCRIPT_KEY missing or wrong length (15 chars)")

YEAR  = datetime.now().year
UA    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"   # browser UA
HEAD  = {"User-Agent": UA}
RAW   = Path("data/raw");  RAW.mkdir(parents=True, exist_ok=True)
PROC  = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

# ── Helper: try URL and validate body ─────────────────────────────────────────
def try_url(url: str) -> bytes | None:
    print("…trying", url)
    r = requests.get(url, headers=HEAD, timeout=60, allow_redirects=True)
    if r.status_code != 200:
        print("  ⚠️ HTTP status", r.status_code)
        return None
    if r.content.startswith(b"Scripting Error"):
        print("  ⚠️ body starts with 'Scripting Error'")
        return None
    return r.content

# ── Step-1: Download with fallback patterns ───────────────────────────────────
def download_mid() -> Path:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    dest  = RAW / f"mid_{today}.csv"
    if dest.exists():
        print("✓ Using cached copy", dest)
        return dest

    base = "https://downloads.elexonportal.co.uk/file/download"
    patterns = [
        f"{base}/MID_{YEAR}.csv?key={KEY}",
        f"{base}?key={KEY}&filename=MID_{YEAR}.csv",
        f"{base}/LATEST_MID_FILE?key={KEY}",
    ]
    for url in patterns:
        data = try_url(url)
        if data:
            dest.write_bytes(data)
            print("✓ Downloaded →", dest)
            return dest

    raise RuntimeError("All URL patterns returned Scripting Error 002")

# ── Step-2: Tidy with dynamic header matching ─────────────────────────────────
def find_col(df: pd.DataFrame, needle: str) -> str:
    needle = needle.lower().replace(" ", "")
    for c in df.columns:
        if needle in c.lower().replace(" ", ""):
            return c
    raise KeyError(f"Column containing '{needle}' not found in {list(df.columns)}")

def tidy(path: Path, days: int = 365) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="latin-1")
    print("Columns →", list(df.columns))

    date_c  = find_col(df, "settlementdate")
    sp_c    = find_col(df, "settlementperiod")
    price_c = find_col(df, "marketindexprice")
    vol_c   = find_col(df, "marketindexvolume")

    # Build tz-aware datetime
    df["datetime"] = (
        pd.to_datetime(df[date_c], dayfirst=True, utc=True) +
        pd.to_timedelta((df[sp_c].astype(int) - 1) * 30, unit="m")
    )
    df["price"]  = pd.to_numeric(df[price_c], errors="coerce")
    df["volume"] = pd.to_numeric(df[vol_c],   errors="coerce")

    # VWAP per interval
    vwap = (
        df.groupby("datetime", group_keys = False)
          .apply(lambda g: (g["price"] * g["volume"]).sum() / g["volume"].sum())
          .rename("price")
    )
    vol  = df.groupby("datetime")["volume"].sum().rename("volume")
    tidy = pd.concat([vwap, vol], axis=1).reset_index()

    # Rolling window
    cutoff = pd.Timestamp.now(tz=timezone.utc) - timedelta(days=days)
    return tidy[tidy["datetime"] >= cutoff]

# ── Main ──────────────────────────────────────────────────────────────────────
def main(days: int = 365):
    csv   = download_mid()
    clean = tidy(csv, days)
    out   = PROC / "intraday_prices.parquet"
    clean.to_parquet(out, index=False)
    print(f"✅ Saved {clean.shape[0]} rows → {out}")

if __name__ == "__main__":
    main()
