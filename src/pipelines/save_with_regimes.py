"""
Run:
    python -m src.pipelines.save_with_regimes  --in data/processed/final_merged_with_features.parquet \
                                              --out data/processed/final_merged_with_regimes.parquet
"""

import argparse, sys
import pandas as pd
from pathlib import Path
from src.features.regime_flags import add_regime_flags

def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--in",  dest="input_path",  required=True, help="feature parquet")
    p.add_argument("--out", dest="output_path", required=True, help="output parquet")
    return p.parse_args()

def main():
    args = cli()
    df = pd.read_parquet(args.input_path)

    # Build regime flags (uses 48-period rolling window)
    df = add_regime_flags(df, config={}, window=48)

    Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output_path, index=False)
    print("✅  Saved file with regime flags →", args.output_path)

if __name__ == "__main__":
    sys.exit(main())
