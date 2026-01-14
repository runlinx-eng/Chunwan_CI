from src.data_provider import SnapshotProvider
from src.report import build_report
from src.scoring import compute_indicators, score_stocks
from src.signals import load_signals, load_theme_industry_map
from src.utils import parse_date, previous_trading_date


def _flatten_concepts(theme_map):
    concepts = []
    for entries in theme_map.values():
        for entry in entries:
            if entry["type"] in ("concept", "industry"):
                for value in entry["values"]:
                    if value not in concepts:
                        concepts.append(value)
    return concepts


def test_snapshot_no_future_and_alignment():
    as_of = previous_trading_date(parse_date("2026-01-20"))
    provider = SnapshotProvider(as_of=as_of)
    theme_map = load_theme_industry_map("theme_to_industry.csv")
    concepts = _flatten_concepts(theme_map)
    stocks = provider.get_stock_universe(concepts)
    price_df = provider.get_price_history(stocks, as_of, lookback_days=60, seed=1)
    assert price_df["date"].max() <= as_of
    assert (price_df["date"] == as_of).any()


def test_snapshot_reproducible():
    as_of = previous_trading_date(parse_date("2026-01-20"))
    provider = SnapshotProvider(as_of=as_of)
    theme_map = load_theme_industry_map("theme_to_industry.csv")
    concepts = _flatten_concepts(theme_map)
    signals = load_signals("signals.yaml")
    stocks = provider.get_stock_universe(concepts)
    price_df = provider.get_price_history(stocks, as_of, lookback_days=60, seed=1)

    indicators = compute_indicators(price_df, as_of)
    scored_1, hit_1 = score_stocks(indicators, signals, theme_map)
    report_1 = build_report(scored_1, signals, hit_1, as_of, top_n=5)

    scored_2, hit_2 = score_stocks(indicators, signals, theme_map)
    report_2 = build_report(scored_2, signals, hit_2, as_of, top_n=5)

    assert report_1 == report_2


def test_snapshot_no_nan_and_theme_unique():
    as_of = previous_trading_date(parse_date("2026-01-20"))
    provider = SnapshotProvider(as_of=as_of)
    theme_map = load_theme_industry_map("theme_to_industry.csv")
    concepts = _flatten_concepts(theme_map)
    signals = load_signals("signals.yaml")
    stocks = provider.get_stock_universe(concepts)
    price_df = provider.get_price_history(stocks, as_of, lookback_days=60, seed=1)

    indicators = compute_indicators(price_df, as_of)
    scored, hit_map = score_stocks(indicators, signals, theme_map)
    report = build_report(scored, signals, hit_map, as_of, top_n=5)

    for row in report["results"]:
        indicators = row.get("indicators", {})
        assert indicators.get("momentum_60") == indicators.get("momentum_60")
        themes = [hit.get("theme") for hit in row.get("theme_hits", []) if hit.get("theme")]
        assert len(themes) == len(set(themes))
