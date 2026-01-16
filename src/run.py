import argparse
import json
import subprocess
from pathlib import Path
from typing import Optional

import pandas as pd

from .cache import load_cached, save_cached
from .data_provider import build_provider, provider_seed
from .report import build_report
from .scoring import compute_indicators
from .signals import load_signals, load_theme_industry_map
from .theme_pipeline import DefaultConceptMapper, DefaultThemeExtractor, DefaultThemeScorer
from .utils import parse_date, previous_trading_date, stable_hash


def read_text_hash(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return stable_hash([f.read()])


def write_outputs(report: dict, output_prefix: Path) -> None:
    output_prefix.parent.mkdir(exist_ok=True)
    with output_prefix.with_suffix(".json").open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    df = pd.json_normalize(report["results"])
    df.to_csv(output_prefix.with_suffix(".csv"), index=False)

def read_manifest(snapshot_as_of: Optional[pd.Timestamp]) -> dict:
    if snapshot_as_of is None:
        return {}
    manifest_path = Path("data/snapshots") / snapshot_as_of.strftime("%Y-%m-%d") / "manifest.json"
    if not manifest_path.exists():
        return {"path": str(manifest_path), "missing": True}
    content = json.loads(manifest_path.read_text(encoding="utf-8"))
    stats = content.get("stats", {})
    if "min_concept_members" not in stats and "min_count" in stats:
        stats["min_concept_members"] = stats.get("min_count")
    if "min_price_bars" not in stats:
        stats["min_price_bars"] = None
    content["stats"] = stats
    return {
        "path": str(manifest_path),
        "content": content,
        "hash": stable_hash([manifest_path.read_text(encoding="utf-8")]),
    }


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def build_provenance(
    args, signals_hash: str, map_hash: str, snapshot_as_of: Optional[pd.Timestamp]
) -> dict:
    manifest = {}
    if args.provider == "snapshot" and args.snapshot_as_of:
        manifest = read_manifest(snapshot_as_of)
    return {
        "args": {
            "date": args.date,
            "top": args.top,
            "provider": args.provider,
            "signals": args.signals,
            "theme_map": args.theme_map,
            "no_cache": args.no_cache,
            "no_fallback": args.no_fallback,
            "snapshot_as_of": args.snapshot_as_of,
        },
        "git_commit": git_commit(),
        "signals_hash": signals_hash,
        "theme_map_hash": map_hash,
        "manifest": manifest,
    }


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

    as_of = None
    snapshot_as_of = None

    input_date = parse_date(args.date)
    as_of = previous_trading_date(input_date)
    snapshot_as_of = parse_date(args.snapshot_as_of) if args.snapshot_as_of else None

    signals = load_signals(args.signals)
    theme_map = load_theme_industry_map(args.theme_map)
    extractor = DefaultThemeExtractor()
    mapper = DefaultConceptMapper()
    scorer = DefaultThemeScorer()

    core_themes = extractor.extract(signals, as_of)
    mapped_theme_map = mapper.map(signals, theme_map, core_themes)
    industries = mapper.flatten(mapped_theme_map)

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
            scored_df, hit_map = scorer.score(indicator_df, signals, mapped_theme_map)
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
            report = build_report(
                selected,
                signals,
                hit_map,
                as_of,
                args.top,
                themes_used=core_themes,
                provider=args.provider,
                snapshot_as_of=args.snapshot_as_of,
            )
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

    report["provenance"] = build_provenance(args, signals_hash, map_hash, snapshot_as_of)
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
