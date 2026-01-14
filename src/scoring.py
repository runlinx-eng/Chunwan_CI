from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .signals import Signal


def compute_indicators(price_df: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    df = price_df[price_df["date"] <= as_of].copy()
    df = df.sort_values(["ticker", "date"])
    df["return"] = df.groupby("ticker")["close"].pct_change()
    df["momentum_20"] = df.groupby("ticker")["close"].pct_change(20)
    # 60 trading days inclusive -> 59-period change to allow exactly 60 rows
    df["momentum_60"] = df.groupby("ticker")["close"].pct_change(59)
    df["volatility_20"] = df.groupby("ticker")["return"].rolling(20).std().reset_index(level=0, drop=True)
    df["avg_volume_20"] = df.groupby("ticker")["volume"].rolling(20).mean().reset_index(level=0, drop=True)

    latest = df[df["date"] == as_of].copy()
    indicator_cols = ["momentum_20", "momentum_60", "volatility_20", "avg_volume_20"]
    latest["indicator_missing"] = latest[indicator_cols].isna().any(axis=1)
    latest[indicator_cols] = latest[indicator_cols].fillna(0)
    return latest


def _rank_series(series: pd.Series) -> pd.Series:
    return series.rank(pct=True)


def score_stocks(
    indicator_df: pd.DataFrame,
    signals: List[Signal],
    theme_map: Dict[str, List[Dict[str, List[str]]]],
) -> Tuple[pd.DataFrame, Dict[str, List[Dict[str, object]]]]:
    signal_hit_tickers: Dict[str, set] = {}
    hit_details: Dict[str, Dict[str, Dict[str, set]]] = {}

    label_columns = [col for col in ("industry", "concept", "description") if col in indicator_df.columns]
    label_map: Dict[str, List[Tuple[str, str]]] = {}
    for _, row in indicator_df.iterrows():
        labels: List[Tuple[str, str]] = []
        for col in label_columns:
            value = str(row.get(col, "")).strip()
            if value and value.lower() != "nan":
                labels.append((col, value))
        label_map[row["ticker"]] = labels

    for signal in signals:
        entries = theme_map.get(signal.id, [])
        signal_hit_tickers[signal.id] = set()

        # Map-based matching
        for entry in entries:
            map_type = str(entry["type"]).lower()
            values = entry["values"]
            if map_type == "ticker":
                hit_mask = indicator_df["ticker"].isin(values)
            elif map_type == "concept" and "concept" in indicator_df.columns:
                hit_mask = indicator_df["concept"].isin(values)
            else:
                hit_mask = indicator_df["industry"].isin(values)
            hit_rows = indicator_df[hit_mask]
            for _, row in hit_rows.iterrows():
                ticker = row["ticker"]
                signal_hit_tickers[signal.id].add(ticker)
                detail = hit_details.setdefault(ticker, {}).setdefault(signal.id, {})
                match_path = "concept" if map_type == "industry" else map_type
                detail.setdefault("match_paths", set()).add(match_path)
                if map_type == "ticker":
                    matched_term = row["ticker"]
                elif map_type == "concept":
                    matched_term = row.get("concept", "")
                else:
                    matched_term = row.get("industry", "")
                if matched_term:
                    detail.setdefault("matched_terms", set()).add(str(matched_term))
                detail.setdefault("matched_source", set()).add("map")

        # Keyword-based matching
        for ticker, labels in label_map.items():
            matched_terms = []
            matched_paths = set()
            for keyword in signal.keywords:
                key_lower = keyword.lower()
                for label_type, label in labels:
                    if key_lower in label.lower():
                        matched_terms.append(keyword)
                        if label_type == "description":
                            matched_paths.add("concept")
                        else:
                            matched_paths.add(label_type)
                        break
            if matched_terms:
                signal_hit_tickers[signal.id].add(ticker)
                detail = hit_details.setdefault(ticker, {}).setdefault(signal.id, {})
                detail.setdefault("match_paths", set()).update(matched_paths)
                detail.setdefault("matched_terms", set()).update(matched_terms)
                detail.setdefault("matched_source", set()).add("signals")

    score = pd.Series(0.0, index=indicator_df.index)
    for signal in signals:
        if signal.weight <= 0:
            continue
        tickers = signal_hit_tickers.get(signal.id, set())
        if not tickers:
            continue
        hit_mask = indicator_df["ticker"].isin(tickers)
        score += hit_mask.astype(float) * signal.weight

    indicator_df = indicator_df.copy()
    indicator_df["theme_score"] = score
    indicator_df["momentum_20_rank"] = _rank_series(indicator_df["momentum_20"].fillna(0))
    indicator_df["momentum_60_rank"] = _rank_series(indicator_df["momentum_60"].fillna(0))
    indicator_df["volume_rank"] = _rank_series(indicator_df["avg_volume_20"].fillna(0))

    indicator_df["final_score"] = (
        indicator_df["theme_score"]
        + 0.5 * indicator_df["momentum_20_rank"]
        + 0.3 * indicator_df["momentum_60_rank"]
        + 0.2 * indicator_df["volume_rank"]
    )

    hit_map: Dict[str, List[Dict[str, object]]] = {}
    for ticker, signal_data in hit_details.items():
        hit_entries = []
        for signal in signals:
            if signal.id not in signal_data:
                continue
            detail = signal_data[signal.id]
            hit_entries.append(
                {
                    "signal_id": signal.id,
                    "theme": signal.core_theme,
                    "signal_theme": signal.theme,
                    "weight": signal.weight,
                    "match_paths": sorted(detail.get("match_paths", set())),
                    "matched_terms": sorted(detail.get("matched_terms", set())),
                    "matched_source": sorted(detail.get("matched_source", set())),
                }
            )
        hit_map[ticker] = hit_entries
    return indicator_df, hit_map
