import pandas as pd
import yaml
from pathlib import Path

def _load_rules(path: Path):
    with open(path, "r") as fp:
        return yaml.safe_load(fp)

def _threshold_series(series: pd.Series, rule: dict) -> pd.Series:
    """Return Boolean mask where series breaches the rule."""
    if rule["method"] == "percentile":
        cut = series.quantile(rule["threshold"]/100)
        return series.abs() >= abs(cut)
    elif rule["method"] == "abs":
        return series.abs() >= abs(rule["threshold"])
    else:
        raise ValueError(f"Unknown rule method: {rule['method']}")

def detect_extreme_events(df: pd.DataFrame,
                          config_path: str | Path = "config/detection.yml"
                          ) -> pd.DataFrame:
    """
    Build tidy event log from a half-hourly dataframe.

    Returns
    -------
    event_log : pd.DataFrame
        Columns: event_id, start, end, driver_col, peak_dt, peak_value, regime_flag_mode
    """
    rules = _load_rules(Path(config_path))
    drivers = rules["drivers"]

    base = df.copy()
    base = base.sort_values("datetime").reset_index(drop=True)

    # 1 – build a single Boolean mask for each driver
    masks = {
        col: _threshold_series(base[col], drivers[col])
        for col in drivers
    }

    # 2 – unify into one DataFrame of candidate rows
    rows = []
    for driver, mask in masks.items():
        tmp = base.loc[mask, ["datetime", driver, "regime_flag"]].copy()
        tmp["driver_col"] = driver
        tmp["driver_value"] = tmp[driver]
        rows.append(tmp)
    cand = pd.concat(rows).sort_values("datetime").reset_index(drop=True)

    if cand.empty:
        return pd.DataFrame(
            columns=["event_id","start","end","driver_col","peak_dt","peak_value","regime_flag_mode"]
        )

    # 3 – assign provisional event_id by time gap
    merge_win = max(v["merge_window"] for v in drivers.values())
    gap = cand["datetime"].diff().dt.total_seconds().div(1800).fillna(1)
    cand["event_id"] = (gap > merge_win).cumsum()

    # 4 – collapse duplicates (two drivers overlap) by priority list
    priority = {c: i for i, c in enumerate(rules["priority"])}
    event_groups = []
    for eid, sub in cand.groupby("event_id"):
        # pick highest-priority driver row with max driver_value
        best = (
            sub.sort_values(["driver_col"], key=lambda s: s.map(priority))
               .sort_values("driver_value", ascending=False)
               .iloc[0]
        )
        event_groups.append({
            "event_id":   eid,
            "start":      sub["datetime"].min(),
            "end":        sub["datetime"].max(),
            "driver_col": best["driver_col"],
            "peak_dt":    best["datetime"],
            "peak_value": best["driver_value"],
            "regime_flag_mode": sub["regime_flag"].mode().iloc[0]
        })
    return pd.DataFrame(event_groups)
