"""
Merge all processed parquet files into one tidy table.

 • Works whatever directory you launch it from ( --root PATH optional )
 • Prints size *and columns* of every file on load
 • Checks for missing half-hours
 • Calculates intraday VWAP 
 • Renames forecast columns only if they really exist
 • Writes <root>/data/processed/final_merged.parquet  
"""

from __future__ import annotations

import argparse, sys, textwrap, warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

UTC       = "UTC"
HALF_HOUR = pd.Timedelta(minutes=30)


# ───────────────────────── CLI ──────────────────────────
def cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent(
            """\
            Typical use (run from repo root):
                python notebooks/merging.py

            Run from anywhere:
                python notebooks/merging.py --root "C:/…/GB-Power-Price-Diver-Spread-Radar"

            Extra options:
              --start 2024-01-01T00:00:00Z   (UTC start filter)
              --end   2025-05-01T23:30:00Z   (UTC end   filter)
              --out   C:/tmp/merged.parquet  (custom output)
            """
        ),
    )
    p.add_argument("--root", type=str, help="Project root folder")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--out")
    return p.parse_args()


# ──────────────── path helpers / loader ────────────────
def locate_root(cli_root: str | None) -> Path:
    if cli_root:
        root = Path(cli_root).expanduser().resolve()
    else:
        root = Path().resolve()

    if not (root / "data" / "processed").exists():
        sys.exit(
            "❌  Can't find data/processed/.  Either cd to the repo root "
            "or pass --root PATH"
        )
    return root


def file_map(root: Path) -> dict[str, Path]:
    proc = root / "data" / "processed"
    return {
        "INTRADAY": proc / "intraday_trades_raw.parquet",
        "IMBALANCE": proc / "imbalance_prices.parquet",
        "DEMAND": proc / "forecast_actual.parquet",
        "FORECAST": proc / "demand_forecast.parquet",
    }


def load_parquet(path: Path, tag: str) -> pd.DataFrame:
    if not path.exists():
        sys.exit(f"❌  {tag}: expected file not found → {path}")
    try:
        df = pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover
        sys.exit(f"❌  {tag}: can't read parquet → {exc}")

    if "datetime" not in df.columns:
        sys.exit(f"❌  {tag}: missing 'datetime' column")

    # ensure UTC tz & proper dtype
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    return df


def print_columns(df: pd.DataFrame, tag: str) -> None:
    cols = ", ".join(df.columns)
    print(f"   ↳ {tag} columns ({len(df.columns)}): {cols}")


# ────────────── data quality utilities ────────────────
def missing_half_hours(df: pd.DataFrame) -> int:
    full = pd.date_range(df["datetime"].min(), df["datetime"].max(), freq=HALF_HOUR, tz=UTC)
    return len(full) - df["datetime"].nunique()


def show_missing(df: pd.DataFrame, tag: str) -> None:
    miss = missing_half_hours(df)
    if miss == 0:
        print(f"✓  {tag:<9}: no missing half-hours")
        return
    rng = pd.date_range(df["datetime"].min(), df["datetime"].max(), freq=HALF_HOUR, tz=UTC)
    sample = sorted(set(rng) - set(df["datetime"]))[:3]
    print(f"⚠️  {tag:<9}: {miss:,} missing half-hours; e.g. {sample}")


def compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
    """Return df with new 'mip_price' column (VWAP of price*volume)."""
    price = next(c for c in df.columns if "price" in c.lower())
    vol = next(c for c in df.columns if "volume" in c.lower())

    vwap = (
        df.groupby("datetime", group_keys=False)[[price, vol]]
        .apply(lambda g: np.average(g[price], weights=g[vol]) if g[vol].sum() else np.nan)
        .rename("mip_price")
        .reset_index()
    )

    return df.drop_duplicates("datetime").merge(vwap, on="datetime", how="left")


# ───────────────────── main merge ──────────────────────
def main() -> None:
    args = cli()
    ROOT = locate_root(args.root)
    FILES = file_map(ROOT)

    # 1 ── LOAD & REPORT
    print("── Loading ──────────────────────────────────────────────")
    dfs = {tag: load_parquet(path, tag) for tag, path in FILES.items()}
    for tag, df in dfs.items():
        print(
            f"✓  {tag:<9}: {len(df):>8,} rows | {df['datetime'].min()} → {df['datetime'].max()}"
        )
        print_columns(df, tag)

    # 2 ── QUICK QUALITY CHECKS
    print("\n── Checks ──────────────────────────────────────────────")
    for tag, df in dfs.items():
        show_missing(df, tag)

    # 3 ── COLUMN NORMALISATION
    dfs["INTRADAY"] = compute_vwap(dfs["INTRADAY"])

    # DEMAND
    rename_demand = {
        "forecast": "forecast_MW",
        "actual": "actual_MW",
    }
    dfs["DEMAND"] = dfs["DEMAND"].rename(columns=rename_demand)

    # FORECAST – rename only if the original columns are present
    forecast_map = {
        "transmissionSystemDemand": "forecast_TSD",
        "nationalDemand": "forecast_ND",
    }
    original_cols = dfs["FORECAST"].columns
    valid_map = {k: v for k, v in forecast_map.items() if k in original_cols}
    if not valid_map:
        print("\n⚠️  FORECAST: expected columns missing – keeping original names")
    else:
        dfs["FORECAST"] = dfs["FORECAST"].rename(columns=valid_map)
        print(f"\n✓  FORECAST columns renamed: {valid_map}")

    # 4 ── OPTIONAL DATE FILTER
    if args.start or args.end:
        s = pd.to_datetime(args.start, utc=True) if args.start else None
        e = pd.to_datetime(args.end, utc=True) if args.end else None
        for tag in dfs:
            before = len(dfs[tag])
            if s is not None:
                dfs[tag] = dfs[tag][dfs[tag]["datetime"] >= s]
            if e is not None:
                dfs[tag] = dfs[tag][dfs[tag]["datetime"] <= e]
            print(f"{tag:<9}: {before:,} → {len(dfs[tag]):,} rows after date filter")

    # 5 ── INDEX & OUTER JOIN
    for tag in dfs:
        dfs[tag] = (
            dfs[tag]
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .set_index("datetime")
        )

    merged = (
        dfs["DEMAND"]
        .join(dfs["INTRADAY"], how="outer", rsuffix="_intraday")
        .join(dfs["IMBALANCE"], how="outer", rsuffix="_imb")
        .join(
            dfs["FORECAST"][
                [c for c in ["forecast_TSD", "forecast_ND"] if c in dfs["FORECAST"].columns]
            ],
            how="outer",
        )
    )

    # 6 ── SAVE
    out_path = Path(args.out) if args.out else ROOT / "data" / "processed" / "final_merged.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.reset_index().to_parquet(out_path, index=False)

    # 7 ── FINAL SUMMARY
    print("\n── Final sanity checks ─────────────────────────────────")
    print("Rows:", f"{len(merged):,}")
    print("Cols:", len(merged.columns))
    print("Span:", merged.index.min(), "→", merged.index.max())
    print("NaN per column (top 10):")
    print(merged.isna().sum().sort_values(ascending=False).head(10))

    print(f"\n✅  Saved merged parquet → {out_path}")


# —————————————————————————————————————————————
if __name__ == "__main__":
    main()
