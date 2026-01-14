import hashlib
from datetime import datetime
from typing import Iterable

import pandas as pd


def parse_date(date_str: str) -> pd.Timestamp:
    return pd.Timestamp(datetime.strptime(date_str, "%Y-%m-%d").date())


def previous_trading_date(date: pd.Timestamp) -> pd.Timestamp:
    # Simple A-share approximation: weekdays only
    if date.weekday() < 5:
        return date
    # Weekend -> previous Friday
    return date - pd.Timedelta(days=date.weekday() - 4)


def trading_calendar(end_date: pd.Timestamp, lookback_days: int) -> pd.DatetimeIndex:
    start_date = end_date - pd.Timedelta(days=lookback_days)
    return pd.bdate_range(start=start_date, end=end_date)


def stable_hash(parts: Iterable[str]) -> str:
    m = hashlib.md5()
    for part in parts:
        m.update(part.encode("utf-8"))
    return m.hexdigest()
