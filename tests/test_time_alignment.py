import pandas as pd

from src.utils import previous_trading_date


def test_previous_trading_date_weekend():
    date = pd.Timestamp("2026-01-18")  # Sunday
    aligned = previous_trading_date(date)
    assert aligned == pd.Timestamp("2026-01-16")


def test_previous_trading_date_weekday():
    date = pd.Timestamp("2026-01-20")  # Tuesday
    aligned = previous_trading_date(date)
    assert aligned == date
