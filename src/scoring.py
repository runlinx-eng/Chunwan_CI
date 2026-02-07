from __future__ import annotations

import os
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
    df["volatility_60"] = df.groupby("ticker")["return"].rolling(60).std().reset_index(level=0, drop=True)
    df["avg_volume_20"] = df.groupby("ticker")["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["amount"] = df["close"] * df["volume"]
    df["avg_amount_20"] = df.groupby("ticker")["amount"].rolling(20).mean().reset_index(level=0, drop=True)
    df["volume_ratio_20"] = df["volume"] / df["avg_volume_20"].replace(0, np.nan)
    df["trend_stability_20"] = df["momentum_20"] / df["volatility_20"].replace(0, np.nan)
    df["volatility_contraction_20_60"] = 1.0 - (
        df["volatility_20"] / df["volatility_60"].replace(0, np.nan)
    )

    latest = df[df["date"] == as_of].copy()
    indicator_cols = [
        "momentum_20",
        "momentum_60",
        "volatility_20",
        "volatility_60",
        "avg_volume_20",
        "avg_amount_20",
        "volume_ratio_20",
        "trend_stability_20",
        "volatility_contraction_20_60",
    ]
    latest["indicator_missing"] = latest[indicator_cols].isna().any(axis=1)
    latest[indicator_cols] = latest[indicator_cols].fillna(0)
    return latest


def _rank_series(series: pd.Series) -> pd.Series:
    return series.rank(pct=True)


def _collect_signal_hits(
    indicator_df: pd.DataFrame,
    signals: List[Signal],
    theme_map: Dict[str, List[Dict[str, List[str]]]],
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, Dict[str, set]]]]:
    signal_hit_strengths: Dict[str, Dict[str, float]] = {}
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
        signal_hit_strengths[signal.id] = {}

        # Map-based matching
        for entry in entries:
            map_type = str(entry["type"]).lower()
            values = entry["values"]
            weights_raw = entry.get("weights", {})
            weight_map = weights_raw if isinstance(weights_raw, dict) else {}
            if map_type == "ticker":
                hit_mask = indicator_df["ticker"].isin(values)
            elif map_type == "concept" and "concept" in indicator_df.columns:
                hit_mask = indicator_df["concept"].isin(values)
            else:
                hit_mask = indicator_df["industry"].isin(values)
            hit_rows = indicator_df[hit_mask]
            for _, row in hit_rows.iterrows():
                ticker = row["ticker"]
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
                strength = 1.0
                if matched_term and str(matched_term) in weight_map:
                    try:
                        strength = float(weight_map[str(matched_term)])
                    except (TypeError, ValueError):
                        strength = 1.0
                signal_hit_strengths[signal.id][ticker] = max(
                    signal_hit_strengths[signal.id].get(ticker, 0.0), strength
                )

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
                detail = hit_details.setdefault(ticker, {}).setdefault(signal.id, {})
                detail.setdefault("match_paths", set()).update(matched_paths)
                detail.setdefault("matched_terms", set()).update(matched_terms)
                detail.setdefault("matched_source", set()).add("signals")
                strength = 1.0 + min(0.2, 0.05 * len(matched_terms))
                signal_hit_strengths[signal.id][ticker] = max(
                    signal_hit_strengths[signal.id].get(ticker, 0.0), strength
                )

    return signal_hit_strengths, hit_details


def _score_weights() -> Tuple[float, float, float]:
    def _read_env(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    # Defaults preserve current behavior (no risk penalty applied).
    theme_w = _read_env("SCORE_W_THEME", 1.0)
    tech_w = _read_env("SCORE_W_TECH", 1.0)
    risk_w = _read_env("SCORE_W_RISK", 0.0)
    return theme_w, tech_w, risk_w


def compute_theme_score(
    indicator_df: pd.DataFrame,
    signals: List[Signal],
    signal_hit_strengths: Dict[str, Dict[str, float]],
) -> pd.Series:
    score = pd.Series(0.0, index=indicator_df.index)
    ticker_to_index = {str(t): idx for idx, t in indicator_df["ticker"].items()}
    for signal in signals:
        if signal.weight <= 0:
            continue
        ticker_strength = signal_hit_strengths.get(signal.id, {})
        if not ticker_strength:
            continue
        for ticker, strength in ticker_strength.items():
            idx = ticker_to_index.get(str(ticker))
            if idx is None:
                continue
            score.at[idx] += signal.weight * float(strength)
    return score


def compute_technical_score(indicator_df: pd.DataFrame) -> Tuple[pd.Series, pd.DataFrame]:
    enriched = indicator_df.copy()
    enriched["momentum_20_rank"] = _rank_series(enriched["momentum_20"].fillna(0))
    enriched["momentum_60_rank"] = _rank_series(enriched["momentum_60"].fillna(0))
    enriched["volume_rank"] = _rank_series(enriched["avg_volume_20"].fillna(0))
    enriched["liquidity_rank"] = _rank_series(enriched["avg_amount_20"].fillna(0))
    enriched["trend_stability_rank"] = _rank_series(enriched["trend_stability_20"].fillna(0))
    enriched["volatility_contraction_rank"] = _rank_series(
        enriched["volatility_contraction_20_60"].fillna(0)
    )
    technical_score = (
        0.35 * enriched["momentum_20_rank"]
        + 0.20 * enriched["momentum_60_rank"]
        + 0.15 * enriched["volume_rank"]
        + 0.15 * enriched["liquidity_rank"]
        + 0.10 * enriched["trend_stability_rank"]
        + 0.05 * enriched["volatility_contraction_rank"]
    )
    return technical_score, enriched


def compute_risk_penalty(indicator_df: pd.DataFrame) -> pd.Series:
    # Keep penalty available for future hardening; default score weight is zero.
    if "volatility_20" not in indicator_df.columns:
        return pd.Series(0.0, index=indicator_df.index)
    return _rank_series(indicator_df["volatility_20"].fillna(0))


def score_stocks(
    indicator_df: pd.DataFrame,
    signals: List[Signal],
    theme_map: Dict[str, List[Dict[str, List[str]]]],
) -> Tuple[pd.DataFrame, Dict[str, List[Dict[str, object]]]]:
    signal_hit_strengths, hit_details = _collect_signal_hits(indicator_df, signals, theme_map)
    indicator_df = indicator_df.copy()

    theme_score = compute_theme_score(indicator_df, signals, signal_hit_strengths)
    technical_score, indicator_df = compute_technical_score(indicator_df)
    risk_penalty = compute_risk_penalty(indicator_df)
    theme_w, tech_w, risk_w = _score_weights()

    indicator_df["theme_score"] = theme_score
    indicator_df["technical_score"] = technical_score
    indicator_df["risk_penalty"] = risk_penalty
    indicator_df["score_w_theme"] = theme_w
    indicator_df["score_w_tech"] = tech_w
    indicator_df["score_w_risk"] = risk_w
    indicator_df["final_score"] = (
        theme_w * indicator_df["theme_score"]
        + tech_w * indicator_df["technical_score"]
        - risk_w * indicator_df["risk_penalty"]
    )

    hit_map: Dict[str, List[Dict[str, object]]] = {}
    theme_strength_components_by_ticker: Dict[str, Dict[str, float]] = {}
    for ticker, signal_data in hit_details.items():
        hit_entries = []
        all_terms = set()
        all_paths = set()
        map_hit_signals = 0
        for signal in signals:
            if signal.id not in signal_data:
                continue
            detail = signal_data[signal.id]
            matched_terms = set(detail.get("matched_terms", set()))
            match_paths = set(detail.get("match_paths", set()))
            matched_source = set(detail.get("matched_source", set()))
            all_terms.update(matched_terms)
            all_paths.update(match_paths)
            if "map" in matched_source:
                map_hit_signals += 1
            hit_entries.append(
                {
                    "signal_id": signal.id,
                    "theme": signal.core_theme,
                    "signal_theme": signal.theme,
                    "weight": signal.weight,
                    "match_paths": sorted(match_paths),
                    "matched_terms": sorted(matched_terms),
                    "matched_source": sorted(matched_source),
                }
            )
        hit_map[ticker] = hit_entries
        total_signals = len(hit_entries)
        coverage_ratio = (map_hit_signals / total_signals) if total_signals else 0.0
        theme_strength_components_by_ticker[ticker] = {
            "hit_signal_count": float(total_signals),
            "matched_terms_count": float(len(all_terms)),
            "match_path_count": float(len(all_paths)),
            "map_source_signal_ratio": float(coverage_ratio),
        }

    indicator_df["theme_strength_components"] = indicator_df["ticker"].map(
        lambda t: theme_strength_components_by_ticker.get(
            t,
            {
                "hit_signal_count": 0.0,
                "matched_terms_count": 0.0,
                "match_path_count": 0.0,
                "map_source_signal_ratio": 0.0,
            },
        )
    )
    return indicator_df, hit_map
