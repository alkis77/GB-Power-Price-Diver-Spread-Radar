import pandas as pd

def annotate_df(df: pd.DataFrame, event_log: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *df* with `event_id` and `event_age` columns."""
    out = df.copy()
    out["event_id"] = pd.NA

    for _, row in event_log.iterrows():
        mask = (out["datetime"] >= row["start"]) & (out["datetime"] <= row["end"])
        out.loc[mask, "event_id"] = row["event_id"]

    # event_age = periods since start (0,1,2,… within each event)
    out["event_age"] = (
        out.groupby("event_id").cumcount().where(out["event_id"].notna())
    )
    return out
