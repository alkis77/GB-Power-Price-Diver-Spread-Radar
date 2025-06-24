import argparse
import pandas as pd
from pathlib import Path
from gbpower.events.detection import detect_extreme_events
from gbpower.events.annotate import annotate_df
from gbpower.events.plotting import plot_event

def main():
    p = argparse.ArgumentParser(description="Build event log + figs")
    p.add_argument("--input",  required=True, default="data/processed/final_merged_with_regimes.parquet",)
    p.add_argument("--config", default="config/detection.yml")
    p.add_argument("--outdir", default="data/processed")
    p.add_argument("--figdir", default="reports/figures")
    p.add_argument("--top",    type=int, default=20,
                   help="How many largest events to plot")
    args = p.parse_args()

    df  = pd.read_parquet(args.input)
    log = detect_extreme_events(df, args.config)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    log_path = outdir / "event_log.parquet"; log.to_parquet(log_path)
    print(f"✅ Event log saved → {log_path}")

    # annotate & overwrite parquet ready for ML
    annotated = annotate_df(df, log)
    ann_path = outdir / "features_with_events.parquet"
    annotated.to_parquet(ann_path)
    print(f"✅ Annotated feature set → {ann_path}")

    # plots for the top-N events by |peak_value|
    figdir = Path(args.figdir); figdir.mkdir(parents=True, exist_ok=True)
    top_log = log.nlargest(args.top, "peak_value")
    for _, row in top_log.iterrows():
        fpath = figdir / f"event_{int(row.event_id):03}.png"
        plot_event(annotated, row, path=fpath)
        print("📊", fpath)

if __name__ == "__main__":
    main()
