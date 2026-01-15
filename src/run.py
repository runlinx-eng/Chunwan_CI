import argparse
import json
from pathlib import Path

import pandas as pd

from .cache import load_cached, save_cached
from .data_provider import build_provider, provider_seed
from .report import build_report
from .scoring import compute_indicators, score_stocks
from .signals import load_signals, load_theme_industry_map
from .utils import parse_date, previous_trading_date, stable_hash


def flatten_concepts(theme_map):
    seen = []
    for entries in theme_map.values():
        for entry in entries:
            if entry["type"] in ("industry", "concept"):
                for value in entry["values"]:
                    if value not in seen:
                        seen.append(value)
    return seen


def read_text_hash(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return stable_hash([f.read()])


def write_outputs(report: dict, output_prefix: Path) -> None:
    output_prefix.parent.mkdir(exist_ok=True)
    with output_prefix.with_suffix(".json").open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    df = pd.json_normalize(report["results"])
    df.to_csv(output_prefix.with_suffix(".csv"), index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-holiday A-share stock screener")
    parser.add_argument("--date", required=True, help="As-of date YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=20, help="Top N results")
    parser.add_argument("--signals", default="signals.yaml", help="Signals YAML path")
    parser.add_argument(
        "--theme-map",
        default="theme_to_industry.csv",
        help="Theme to industry mapping CSV",
    )
    parser.add_argument("--provider", default="mock", help="Data provider: mock/akshare/snapshot")
    parser.add_argument("--no-fallback", action="store_true", help="Fail when provider errors")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache usage")
    parser.add_argument("--snapshot-as-of", help="Snapshot date YYYY-MM-DD for snapshot provider")
    args = parser.parse_args()

    signals = load_signals(args.signals)
    theme_map = load_theme_industry_map(args.theme_map)
    industries = flatten_concepts(theme_map)

    input_date = parse_date(args.date)
    as_of = previous_trading_date(input_date)
    snapshot_as_of = parse_date(args.snapshot_as_of) if args.snapshot_as_of else None

    signals_hash = read_text_hash(args.signals)
    map_hash = read_text_hash(args.theme_map)
    cache_key = stable_hash(
        [
            args.date,
            str(args.top),
            args.provider,
            signals_hash,
            map_hash,
        ]
    )

    cached_df = None
    cached_report = None
    if not args.no_cache:
        cached_df, cached_report = load_cached(cache_key)

    if cached_report is not None and cached_df is not None:
        report = cached_report
        report.setdefault("meta", {}).setdefault("excluded", {"insufficient_history_60": 0})
        report.setdefault("meta", {}).setdefault("min_history", 0)
        issue_list = report.get("meta", {}).get("issue_list", [])
        if not isinstance(issue_list, list):
            issue_list = []
        report["issues"] = int(len(issue_list))
    else:
        try:
            provider = build_provider(args.provider, as_of=as_of, snapshot_as_of=snapshot_as_of)
        except Exception:
            if args.no_fallback:
                raise
            provider = build_provider("mock", as_of=as_of)

        stocks = provider.get_stock_universe(industries)
        seed = provider_seed(args.date, signals_hash)
        price_df = provider.get_price_history(stocks, as_of, lookback_days=130, seed=seed)

        history_counts = price_df[price_df["date"] <= as_of].groupby("ticker").size()
        min_history = 61
        valid_tickers = history_counts[history_counts >= min_history].index
        insufficient_history_60 = int((history_counts < 61).sum())
        insufficient_history_min = int((history_counts < min_history).sum())
        price_df = price_df[price_df["ticker"].isin(valid_tickers)]

        indicator_df = compute_indicators(price_df, as_of)
        issue_list = []
        fallback_used = False
        if indicator_df.empty:
            report = {
                "as_of": as_of.strftime("%Y-%m-%d"),
                "data_date": as_of.strftime("%Y-%m-%d"),
                "top_n": args.top,
                "count": 0,
                "results": [],
            }
        else:
            scored_df, hit_map = score_stocks(indicator_df, signals, theme_map)
            primary = scored_df.sort_values("final_score", ascending=False)
            selected = primary.head(args.top)
            if len(selected) < args.top and len(scored_df) >= args.top:
                remaining = scored_df[~scored_df["ticker"].isin(selected["ticker"])]
                fallback = remaining.sort_values("technical_score", ascending=False).head(
                    args.top - len(selected)
                )
                selected = pd.concat([selected, fallback], ignore_index=True)
                issue_list.append("fallback_used:theme_insufficient")
                fallback_used = True
            report = build_report(selected, signals, hit_map, as_of, args.top)
            report["data_date"] = as_of.strftime("%Y-%m-%d")

        report["meta"] = report.get("meta", {})
        report["meta"]["excluded"] = {"insufficient_history_60": insufficient_history_60}
        if min_history != 61:
            report["meta"]["excluded"]["insufficient_history_min"] = insufficient_history_min
        report["meta"]["min_history"] = min_history
        report["meta"]["universe_count"] = len(stocks)
        report["meta"]["scored_count"] = int(indicator_df.shape[0])
        if indicator_df.empty:
            issue_list.append("no_candidates_after_filters")
        if fallback_used:
            report["meta"]["fallback_used"] = True
            report["meta"]["fallback_reason"] = "theme_insufficient"
        report["meta"]["issue_list"] = issue_list
        report["issues"] = int(len(issue_list))

        meta = {
            "as_of": as_of.strftime("%Y-%m-%d"),
            "provider": provider.name,
            "signals_hash": signals_hash,
            "theme_map_hash": map_hash,
        }
        save_cached(cache_key, indicator_df, report, meta)

    output_prefix = Path("outputs") / f"report_{as_of.strftime('%Y-%m-%d')}_top{args.top}"
    write_outputs(report, output_prefix)

    print(f"As-of date: {report['as_of']}")
    print(f"Top N: {report['top_n']}")
    excluded = report.get("meta", {}).get("excluded", {}).get("insufficient_history_60", 0)
    if excluded:
        print(f"Excluded (insufficient_history_60): {excluded}")
    for idx, row in enumerate(report["results"], 1):
        print(
            f"{idx:02d} {row['ticker']} {row['name']} | {row['reason']} | data_date={row['data_date']}"
        )


if __name__ == "__main__":
    main()
