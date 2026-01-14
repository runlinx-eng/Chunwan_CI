import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd


CACHE_DIR = Path(".cache")


def cache_paths(cache_key: str) -> Tuple[Path, Path, Path]:
    CACHE_DIR.mkdir(exist_ok=True)
    data_path = CACHE_DIR / f"features_{cache_key}.csv"
    report_path = CACHE_DIR / f"report_{cache_key}.json"
    meta_path = CACHE_DIR / f"meta_{cache_key}.json"
    return data_path, report_path, meta_path


def load_cached(cache_key: str) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
    data_path, report_path, _ = cache_paths(cache_key)
    if data_path.exists():
        df = pd.read_csv(data_path, parse_dates=["date"])
    else:
        df = None
    if report_path.exists():
        with report_path.open("r", encoding="utf-8") as f:
            report = json.load(f)
    else:
        report = None
    return df, report


def save_cached(cache_key: str, df: pd.DataFrame, report: Dict, meta: Dict) -> None:
    data_path, report_path, meta_path = cache_paths(cache_key)
    df.to_csv(data_path, index=False)
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
