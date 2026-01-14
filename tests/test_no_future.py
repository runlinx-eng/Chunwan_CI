import pandas as pd

from src.scoring import compute_indicators


def test_no_future_data_used():
    data = [
        {"date": pd.Timestamp("2026-01-19"), "ticker": "A0001", "name": "STOCK_0001", "industry": "X", "close": 100.0, "volume": 1000},
        {"date": pd.Timestamp("2026-01-20"), "ticker": "A0001", "name": "STOCK_0001", "industry": "X", "close": 110.0, "volume": 1100},
        {"date": pd.Timestamp("2026-01-21"), "ticker": "A0001", "name": "STOCK_0001", "industry": "X", "close": 999.0, "volume": 9999},
    ]
    df = pd.DataFrame(data)
    as_of = pd.Timestamp("2026-01-20")
    result = compute_indicators(df, as_of)
    assert (result["date"] <= as_of).all()
    assert float(result.iloc[0]["close"]) == 110.0
    assert result.iloc[0]["date"] == as_of
