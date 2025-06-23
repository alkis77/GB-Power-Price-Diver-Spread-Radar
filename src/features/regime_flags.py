"""
Utility functions to create stress-event / regime flags.

Usage:
    >>> from src.features.regime_flags import add_regime_flags
    >>> df = add_regime_flags(df, config, perc_95=0.95, perc_99=0.99)
"""

import pandas as pd
import numpy as np

def _calc_percentile_thresholds(df: pd.DataFrame, cols: list[str], perc: float) -> dict[str, float]:
    """Return {col: percentile_value} for each col."""
    return {c: df[c].abs().quantile(perc) for c in cols}

def add_regime_flags(
    df: pd.DataFrame,
    config: dict,
    perc_95: float = 0.95,
    perc_99: float = 0.99,
    window: int | None = None,
) -> pd.DataFrame:
    """
    Adds:
        • is_stress_event  (0/1)
        • regime_flag      ('NORMAL'|'HIGH_VOL'|'EXTREME')
    Parameters
    ----------
    window : optional
        If given, recompute rolling volatility with that window; otherwise
        expect 'vol_spread_SBP_vs_MIP' & 'vol_err_TSD_%' already exist.
    """
    df = df.copy()

    # --- ensure volatility columns -----------------------------------------
    if window:
        df["vol_spread_SBP_vs_MIP"] = (
            df["spread_SBP_vs_MIP"].rolling(window, min_periods=1).std()
        )
        df["vol_err_TSD_%"] = (
            df["err_TSD_%"].rolling(window, min_periods=1).std()
        )

    drivers = ["vol_spread_SBP_vs_MIP", "vol_err_TSD_%", "spread_SBP_vs_MIP"]

    thr_95 = _calc_percentile_thresholds(df, drivers, perc_95)
    thr_99 = _calc_percentile_thresholds(df, drivers, perc_99)

    # compute z-score-like flags
    for c in drivers:
        df[f"driver_{c}_gt95"] = (df[c].abs() > thr_95[c]).astype(int)
        df[f"driver_{c}_gt99"] = (df[c].abs() > thr_99[c]).astype(int)

    # --- regime logic -------------------------------------------------------
    df["is_high_vol"] = df[[f"driver_{c}_gt95" for c in drivers]].max(axis=1)
    df["is_extreme"]  = (df[[f"driver_{c}_gt99" for c in drivers]].sum(axis=1) >= 2).astype(int)

    df["regime_flag"] = "NORMAL"
    df.loc[df["is_high_vol"] == 1, "regime_flag"]  = "HIGH_VOL"
    df.loc[df["is_extreme"]  == 1, "regime_flag"]  = "EXTREME"

    df["is_stress_event"] = (df["regime_flag"] != "NORMAL").astype(int)

    return df
