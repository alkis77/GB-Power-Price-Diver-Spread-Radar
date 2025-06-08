import os, sys, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
KEY = os.getenv("ELEXON_SCRIPT_KEY")
if not KEY or len(KEY) != 15:
    sys.exit("❌  ELEXON_SCRIPT_KEY missing or wrong length (15 chars)")

URL = f"https://downloads.elexonportal.co.uk/file/download/SSPSBPNIV_FILE?key={KEY}"
RAW = Path("data/raw"); RAW.mkdir(parents=True, exist_ok=True)
PROC = Path("data/processed"); PROC.mkdir(parents=True, exist_ok=True)

def download_sbpssp():
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    raw_path = RAW / f"sspsbp_{today}.csv"
    if raw_path.exists():
        print("✓ Using cached copy", raw_path)
        return raw_path
    r = requests.get(URL, timeout=60)
    if not r.ok or b"Scripting Error" in r.content[:60]:
        sys.exit("❌ Download failed or scripting error.")
    raw_path.write_bytes(r.content)
    print("✓ Downloaded →", raw_path)
    return raw_path

def tidy(raw_csv, days=365):
    df = pd.read_csv(raw_csv, encoding="latin-1")
    print("Columns in CSV:", df.columns.tolist())

    # Robust, case-insensitive search for column names
    date_c = [c for c in df.columns if "date" in c.lower()][0]
    sp_c   = [c for c in df.columns if "period" in c.lower()][0]
    sbp_c  = [c for c in df.columns if "buy price" in c.lower()][0]
    ssp_c  = [c for c in df.columns if "sell price" in c.lower()][0]
    niv_c  = [c for c in df.columns if "net imbalance volume" in c.lower()][0]  # <--- updated search

    df["datetime"] = (
        pd.to_datetime(df[date_c], dayfirst=True, utc=True) +
        pd.to_timedelta((df[sp_c].astype(int) - 1) * 30, unit="m")
    )
    tidy = pd.DataFrame({
        "datetime": df["datetime"],
        "sbp": pd.to_numeric(df[sbp_c], errors="coerce"),
        "ssp": pd.to_numeric(df[ssp_c], errors="coerce"),
        "niv": pd.to_numeric(df[niv_c], errors="coerce"),
    })
    cutoff = pd.Timestamp.utcnow() - timedelta(days=days)
    return tidy[tidy["datetime"] >= cutoff]


def main(days=365):
    raw = download_sbpssp()
    clean = tidy(raw, days)
    out = PROC / "imbalance_prices.parquet"
    clean.to_parquet(out, index=False)
    print(f"✅ Saved {clean.shape[0]} rows → {out}")

if __name__ == "__main__":
    main()
