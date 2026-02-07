import numpy as np
import pandas as pd

from src.scoring import compute_indicators, score_stocks
from src.signals import load_signals, load_theme_industry_map


def _build_price_frame() -> pd.DataFrame:
    dates = pd.date_range("2025-10-01", periods=80, freq="B")
    rows = []
    for idx, ticker in enumerate(["000001", "000002"], start=1):
        base_price = 10.0 + idx
        for i, day in enumerate(dates):
            close = base_price + (0.12 * i) + (0.03 * idx)
            volume = 1_000_000 + (idx * 40_000) + (i * 2_500)
            rows.append(
                {
                    "date": day,
                    "ticker": ticker,
                    "name": f"Stock{ticker}",
                    "industry": "测试行业",
                    "concept": "测试概念",
                    "description": "",
                    "close": close,
                    "volume": volume,
                }
            )
    return pd.DataFrame(rows)


def test_compute_indicators_contains_a2_market_features():
    price_df = _build_price_frame()
    as_of = pd.Timestamp(price_df["date"].max())
    latest = compute_indicators(price_df, as_of)

    required_cols = [
        "volatility_60",
        "avg_amount_20",
        "volume_ratio_20",
        "trend_stability_20",
        "volatility_contraction_20_60",
    ]
    for col in required_cols:
        assert col in latest.columns
        assert np.isfinite(latest[col]).all()


def test_score_stocks_uses_extended_technical_ranks():
    price_df = _build_price_frame()
    as_of = pd.Timestamp(price_df["date"].max())
    indicator_df = compute_indicators(price_df, as_of)
    signals = load_signals("signals.yaml")
    theme_map = load_theme_industry_map("theme_to_industry.csv")

    scored_df, _ = score_stocks(indicator_df, signals, theme_map)

    for col in ("liquidity_rank", "trend_stability_rank", "volatility_contraction_rank"):
        assert col in scored_df.columns
        assert np.isfinite(scored_df[col]).all()
    assert np.isfinite(scored_df["technical_score"]).all()
    assert np.isfinite(scored_df["final_score"]).all()
