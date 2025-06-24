import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path

REGIME_COLOURS = {"NORMAL":"#4CAF50","HIGH_VOL":"#FFC107","EXTREME":"#F44336"}

def plot_event(df: pd.DataFrame,
               event_row: pd.Series,
               cols = ("err_TSD_MW","cashout_cost_GBP","spread_SBP_vs_MIP"),
               path: str | Path | None = None
               ):
    """Multi-panel diagnostic plot for a single event."""
    start, end = event_row["start"], event_row["end"]
    sub = df[(df["datetime"] >= start - pd.Timedelta(days=1))
             & (df["datetime"] <= end   + pd.Timedelta(days=1))]

    fig, axs = plt.subplots(len(cols)+1, 1, figsize=(14, 3.2*len(cols)+1),
                            sharex=True, gridspec_kw={"height_ratios": [2]*len(cols)+[0.6]})

    for ax, col in zip(axs[:-1], cols):
        ax.plot(sub["datetime"], sub[col], lw=.8)
        ax.set_ylabel(col)
        ax.axvspan(start, end, color="grey", alpha=.2)

    # Regime ribbon
    for regime, col in REGIME_COLOURS.items():
        mask = sub["regime_flag"] == regime
        axs[-1].bar(sub.loc[mask,"datetime"], 1, width=.03, color=col, align="center")
    axs[-1].set_yticks([]); axs[-1].set_xlabel("Datetime")

    fig.suptitle(f"Event #{event_row.event_id} — {event_row.peak_dt:%Y-%m-%d %H:%M}")
    fig.tight_layout()

    if path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=150)
    return fig
