import pandas as pd
from gbpower.events.detection import detect_extreme_events

def test_event_merge():
    # artificial 6-period toy frame
    data = {
        "datetime": pd.date_range("2024-01-01", periods=6, freq="30min", tz="UTC"),
        "cashout_cost_GBP": [0,0,200,180,50,0],
        "spread_SBP_vs_MIP": [0,0,0,0,0,0],
        "err_TSD_MW": [0,0,0,0,0,0],
        "regime_flag": ["NORMAL"]*6
    }
    df = pd.DataFrame(data)
    log = detect_extreme_events(df, "config/detection.yml")
    assert len(log) == 1, "Two neighbouring spikes should merge"
    ev = log.iloc[0]
    assert ev.start == pd.Timestamp("2024-01-01 00:00+0000", tz="UTC")
    assert ev.end   == pd.Timestamp("2024-01-01 02:30+0000", tz="UTC")
