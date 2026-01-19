import argparse
import json
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from .cache import load_cached, save_cached
from .data_provider import StockInfo, build_provider, provider_seed
from .report import build_report
from .scoring import compute_indicators
from .signals import Signal, load_signals, load_theme_industry_map
from .theme_pipeline import (
    DefaultConceptMapper,
    DefaultThemeExtractor,
    DefaultThemeScorer,
    build_snapshot_candidates,
)
from .utils import parse_date, previous_trading_date, stable_hash


def read_text_hash(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return stable_hash([f.read()])


def normalize_ticker(x) -> str:
    s = str(x).strip()
    return s.zfill(6) if s.isdigit() else s


def write_outputs(report: dict, output_prefix: Path) -> None:
    output_prefix.parent.mkdir(exist_ok=True)
    with output_prefix.with_suffix(".json").open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    df = pd.json_normalize(report["results"])
    df.to_csv(output_prefix.with_suffix(".csv"), index=False)


def _mode_label(theme_weight: float) -> str:
    return "tech_only" if abs(theme_weight) < 1e-12 else "enhanced"


def _candidate_entry(row: dict, mode: str, snapshot_id: str) -> dict:
    reason_struct = row.get("reason_struct", {}) if isinstance(row.get("reason_struct"), dict) else {}
    return {
        "item_id": row.get("ticker", ""),
        "ticker": row.get("ticker", ""),
        "mode": mode,
        "final_score": row.get("final_score"),
        "score_breakdown": row.get("score_breakdown", {}),
        "data_date": row.get("data_date"),
        "snapshot_id": snapshot_id,
        "theme_hits": row.get("theme_hits", []) or [],
        "concept_hits": reason_struct.get("concept_hits", []) or [],
    }


def write_candidates(report: dict, mode: str, output_path: Path, snapshot_id: str) -> None:
    results = report.get("results", [])
    if not isinstance(results, list):
        results = []

    new_entries = [_candidate_entry(row, mode, snapshot_id) for row in results]
    existing_entries = []
    if output_path.exists():
        for line in output_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(entry, dict):
                existing_entries.append(entry)

    merged = [entry for entry in existing_entries if entry.get("mode") != mode]
    merged.extend(new_entries)

    mode_order = {"enhanced": 0, "tech_only": 1, "all": 2}

    def sort_key(entry: dict) -> tuple:
        mode_value = mode_order.get(entry.get("mode"), 9)
        score = entry.get("final_score")
        try:
            score_value = float(score)
        except (TypeError, ValueError):
            score_value = float("-inf")
        item_id = str(entry.get("item_id") or entry.get("ticker") or "")
        return (mode_value, -score_value, item_id)

    merged = sorted(merged, key=sort_key)
    if merged:
        content = "\n".join(json.dumps(entry, ensure_ascii=False) for entry in merged) + "\n"
    else:
        content = ""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")

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
            "theme_weight": float(args.theme_weight),
        },
        "git_commit": git_commit(),
        "signals_hash": signals_hash,
        "theme_map_hash": map_hash,
        "manifest": manifest,
    }


def compute_issue_lists(
    report: dict,
    top_n: int,
    fallback_used: bool = False,
    provider_fallback: bool = False,
    warnings: Optional[List[str]] = None,
) -> Tuple[List[str], List[str]]:
    results = report.get("results", [])
    if not isinstance(results, list):
        results = []
    fatal = []
    if len(results) < top_n:
        fatal.append("insufficient_results")
    if provider_fallback:
        fatal.append("provider_fallback")
    if fallback_used:
        fatal.append("fallback_used:theme_insufficient")
    warn_list = warnings or []
    return fatal, warn_list


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-holiday A-share stock screener")
    parser.add_argument("--date", required=True, help="As-of date YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=20, help="Top N results")
    parser.add_argument("--signals", default="signals.yaml", help="Signals YAML path")
    parser.add_argument(
        "--theme-map",
        default="theme_to_industry_em_2026-01-20.csv",
        help="Theme to industry mapping CSV",
    )
    parser.add_argument("--provider", default="mock", help="Data provider: mock/akshare/snapshot")
    parser.add_argument("--no-fallback", action="store_true", help="Fail when provider errors")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache usage")
    parser.add_argument("--snapshot-as-of", help="Snapshot date YYYY-MM-DD for snapshot provider")
    parser.add_argument(
        "--theme-weight",
        type=float,
        default=1.0,
        help="Theme weight multiplier (0 disables theme boost)",
    )
    args = parser.parse_args()

    as_of = None
    snapshot_as_of = None
    membership_raw = None
    membership_terms_by_ticker = {}
    price_df = None
    indicator_df = pd.DataFrame()
    candidates_report = None
    n_membership_rows = None
    n_membership_unique_tickers = None
    n_membership_unique_concepts = None
    n_prices_unique_tickers = None
    data_date_max = None
    n_candidates_intersection = None
    n_candidates_after_history_filter = None
    n_candidates_scored = None
    n_theme_hit_tickers = None
    debug_data = {}
    provider_fallback = False

    input_date = parse_date(args.date)
    as_of = previous_trading_date(input_date)
    snapshot_as_of = parse_date(args.snapshot_as_of) if args.snapshot_as_of else None

    signals = load_signals(args.signals)
    theme_map = load_theme_industry_map(args.theme_map)
    extractor = DefaultThemeExtractor()
    mapper = DefaultConceptMapper()
    scorer = DefaultThemeScorer()

    core_themes = extractor.extract(signals, as_of)
    mapped_theme_map, mapper_debug, signal_theme_key_map = mapper.map(
        signals,
        theme_map,
        core_themes,
        theme_map_path=Path(args.theme_map),
    )
    signals_for_scoring = signals
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
        cached_top = int(report.get("top_n", args.top))
        fallback_used = bool(report.get("meta", {}).get("fallback_used", False))
        provider_fallback = bool(report.get("meta", {}).get("provider_fallback", False))
        warnings = report.get("debug", {}).get("warnings", [])
        if not isinstance(warnings, list):
            warnings = []
        issue_list, warnings = compute_issue_lists(
            report,
            cached_top,
            fallback_used=fallback_used,
            provider_fallback=provider_fallback,
            warnings=warnings,
        )
        report.setdefault("meta", {})["issue_list"] = issue_list
        report["issues"] = int(len(issue_list))
        report.setdefault("debug", {})["warnings"] = warnings
    else:
        try:
            provider = build_provider(args.provider, as_of=as_of, snapshot_as_of=snapshot_as_of)
        except Exception:
            if args.no_fallback:
                raise
            provider = build_provider("mock", as_of=as_of)
            provider_fallback = True

        debug = {
            "n_prices_tickers": 0,
            "n_membership_tickers": 0,
            "n_candidates_from_theme": 0,
            "n_candidates_final": 0,
            "candidate_source": "theme",
        }
        fallback_all_universe = False
        if provider.name == "snapshot":
            snapshot_date = snapshot_as_of or as_of
            snapshot_dir = Path("data/snapshots") / snapshot_date.strftime("%Y-%m-%d")
            membership_path = snapshot_dir / "concept_membership.csv"
            if not membership_path.exists():
                available = ", ".join(sorted([p.name for p in snapshot_dir.parent.iterdir() if p.is_dir()]))
                raise FileNotFoundError(
                    f"Missing concept_membership.csv under {snapshot_dir}. Available snapshots: {available}"
                )
            membership_raw = pd.read_csv(membership_path, dtype={"ticker": str})
            membership_raw["ticker"] = membership_raw["ticker"].map(normalize_ticker)
            for col in ("concept", "industry", "description", "name"):
                if col not in membership_raw.columns:
                    membership_raw[col] = ""
                membership_raw[col] = membership_raw[col].astype(str).str.strip()

            membership_terms_by_ticker = {}
            for ticker, group in membership_raw.groupby("ticker"):
                terms = set()
                for col in ("concept", "industry"):
                    if col not in group.columns:
                        continue
                    for value in group[col].dropna().astype(str):
                        term = value.strip()
                        if not term or term.lower() == "nan":
                            continue
                        if term in {"对应行业/概念", "关键词", "主题名称"}:
                            continue
                        terms.add(term)
                membership_terms_by_ticker[ticker] = sorted(terms)

            candidates, debug, candidate_source, membership = build_snapshot_candidates(
                mapped_theme_map,
                snapshot_dir,
                membership_terms_by_ticker=membership_terms_by_ticker,
                theme_map_path=Path(args.theme_map),
            )
            fallback_all_universe = candidate_source == "universe_fallback"
            membership = membership_raw.copy()
            membership = membership.sort_values(["ticker", "concept"])
            membership_first = membership.drop_duplicates(subset=["ticker"], keep="first")
            membership_lookup = membership_first.set_index("ticker").to_dict(orient="index")
            stocks = []
            for ticker in candidates:
                info = membership_lookup.get(ticker, {})
                if fallback_all_universe:
                    concept = ""
                    industry = ""
                    description = ""
                else:
                    concept = str(info.get("concept", ""))
                    industry = str(info.get("industry", ""))
                    description = str(info.get("description", ""))
                name = str(info.get("name", "")) if info.get("name", "") else f"STOCK_{ticker}"
                stocks.append(
                    StockInfo(
                        ticker=ticker,
                        name=name,
                        industry=industry,
                        concept=concept,
                        description=description,
                    )
                )
        else:
            stocks = provider.get_stock_universe(industries)
        seed = provider_seed(args.date, signals_hash)
        price_df = provider.get_price_history(stocks, as_of, lookback_days=130, seed=seed)
        if price_df.empty:
            n_prices_unique_tickers = 0
            data_date_max = None
            price_tickers = set()
        else:
            n_prices_unique_tickers = int(price_df["ticker"].nunique())
            max_date = price_df["date"].max()
            if hasattr(max_date, "strftime"):
                data_date_max = max_date.strftime("%Y-%m-%d")
            else:
                data_date_max = str(max_date)
            price_tickers = set(price_df["ticker"].dropna().astype(str).str.strip().tolist())
        if membership_raw is not None:
            n_membership_rows = int(len(membership_raw))
            n_membership_unique_tickers = int(membership_raw["ticker"].nunique())
            if "concept" in membership_raw.columns:
                n_membership_unique_concepts = int(membership_raw["concept"].nunique())
            else:
                n_membership_unique_concepts = 0
            membership_tickers = set(
                membership_raw["ticker"].dropna().astype(str).str.strip().tolist()
            )
            n_candidates_intersection = int(len(membership_tickers & price_tickers))

        history_counts = price_df[price_df["date"] <= as_of].groupby("ticker").size()
        min_history = 61
        valid_tickers = history_counts[history_counts >= min_history].index
        insufficient_history_60 = int((history_counts < 61).sum())
        insufficient_history_min = int((history_counts < min_history).sum())
        price_df = price_df[price_df["ticker"].isin(valid_tickers)]
        n_candidates_after_history_filter = int(len(valid_tickers))

        indicator_df = compute_indicators(price_df, as_of)
        n_candidates_scored = int(indicator_df.shape[0])
        if fallback_all_universe and not indicator_df.empty:
            indicator_df["concept"] = ""
            indicator_df["industry"] = ""
            indicator_df["description"] = ""
        issue_list = []
        warnings = []
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
            scored_df, hit_map = scorer.score(indicator_df, signals_for_scoring, mapped_theme_map)
            n_theme_hit_tickers = int(len(hit_map))
            if args.theme_weight == 0:
                scored_df = scored_df.copy()
                scored_df["theme_score"] = 0.0
                scored_df["final_score"] = scored_df["technical_score"]
            primary = scored_df.sort_values("final_score", ascending=False)
            selected = primary.head(args.top)
            if len(selected) < args.top and len(scored_df) >= args.top:
                remaining = scored_df[~scored_df["ticker"].isin(selected["ticker"])]
                fallback = remaining.sort_values("technical_score", ascending=False).head(
                    args.top - len(selected)
                )
                selected = pd.concat([selected, fallback], ignore_index=True)
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
            candidates_report = build_report(
                scored_df,
                signals,
                hit_map,
                as_of,
                int(len(scored_df)),
                themes_used=core_themes,
                provider=args.provider,
                snapshot_as_of=args.snapshot_as_of,
            )
            report["data_date"] = as_of.strftime("%Y-%m-%d")
            candidates_report["data_date"] = as_of.strftime("%Y-%m-%d")

        report["meta"] = report.get("meta", {})
        report["meta"]["excluded"] = {"insufficient_history_60": insufficient_history_60}
        if min_history != 61:
            report["meta"]["excluded"]["insufficient_history_min"] = insufficient_history_min
        report["meta"]["min_history"] = min_history
        report["meta"]["universe_count"] = len(stocks)
        report["meta"]["scored_count"] = int(indicator_df.shape[0])
        if indicator_df.empty:
            warnings.append("no_candidates_after_filters")
        if fallback_used:
            report["meta"]["fallback_used"] = True
            report["meta"]["fallback_reason"] = "theme_insufficient"
        if fallback_all_universe:
            warnings.append("fallback_all_universe_no_theme_hits")
        report["meta"]["provider_fallback"] = provider_fallback
        issue_list, warnings = compute_issue_lists(
            report,
            args.top,
            fallback_used=fallback_used,
            provider_fallback=provider_fallback,
            warnings=warnings,
        )
        report["meta"]["issue_list"] = issue_list
        report["issues"] = int(len(issue_list))
        report.setdefault("debug", {})["warnings"] = warnings
        results = report.get("results", [])
        top5_terms_found = []
        top5_terms_sample = []
        if membership_terms_by_ticker:
            for row in results[:5]:
                ticker = normalize_ticker(row.get("ticker", ""))
                terms = membership_terms_by_ticker.get(ticker, [])
                top5_terms_found.append(int(len(terms)))
                top5_terms_sample.append(terms[:5])
        debug_data = {
            "snapshot_as_of": args.snapshot_as_of,
            "date": args.date,
            "provider": provider.name,
            "n_membership_rows": n_membership_rows,
            "n_membership_unique_tickers": n_membership_unique_tickers,
            "n_membership_unique_concepts": n_membership_unique_concepts,
            "n_prices_unique_tickers": n_prices_unique_tickers,
            "data_date_max": data_date_max,
            "n_candidates_intersection": n_candidates_intersection,
            "n_candidates_after_history_filter": n_candidates_after_history_filter,
            "n_candidates_scored": n_candidates_scored,
            "n_theme_hit_tickers": n_theme_hit_tickers or 0,
            "top5_theme_totals": [
                row.get("score_breakdown", {}).get("score_theme_total") for row in results[:5]
            ],
            "top5_tickers": [row.get("ticker") for row in results[:5]],
            "top5_terms_found": top5_terms_found,
            "top5_terms_sample": top5_terms_sample,
        }
        if isinstance(debug, dict) and "matched_terms_by_ticker" in debug:
            debug_data["matched_terms_by_ticker"] = debug["matched_terms_by_ticker"]
        if isinstance(mapper_debug, dict):
            for key in (
                "signals_theme_key_sample",
                "signals_theme_key_count",
                "theme_map_theme_sample",
                "theme_key_miss_count",
                "theme_key_hit_count",
            ):
                if key in mapper_debug:
                    debug_data[key] = mapper_debug[key]
        debug_data["warnings"] = warnings
        if isinstance(debug, dict):
            for key in (
                "themes_in_map_count",
                "terms_in_map_count",
                "rows_in_map_count",
                "sample_themes",
                "sample_terms",
            ):
                if key in debug:
                    debug_data[key] = debug[key]
            if "rows_in_map_count" in debug:
                debug_data["theme_map_rows"] = debug["rows_in_map_count"]
            if "themes_in_map_count" in debug:
                debug_data["theme_map_themes"] = debug["themes_in_map_count"]
            if "terms_in_map_count" in debug:
                debug_data["theme_map_terms"] = debug["terms_in_map_count"]
            if "sample_themes" in debug:
                debug_data["theme_map_sample_themes"] = debug["sample_themes"]
            if "sample_terms" in debug:
                debug_data["theme_map_sample_terms"] = debug["sample_terms"]
        report["debug"] = debug_data
        if candidates_report:
            snapshot_id = args.snapshot_as_of or as_of.strftime("%Y-%m-%d")
            candidates_path = Path("artifacts_metrics") / "screener_candidates_latest.jsonl"
            write_candidates(
                candidates_report, _mode_label(args.theme_weight), candidates_path, snapshot_id
            )

        meta = {
            "as_of": as_of.strftime("%Y-%m-%d"),
            "provider": provider.name,
            "signals_hash": signals_hash,
            "theme_map_hash": map_hash,
        }
        save_cached(cache_key, indicator_df, report, meta)

    debug_data = report.get("debug", {})
    if not isinstance(debug_data, dict):
        debug_data = {}
    results = report.get("results", [])
    debug_data.setdefault("snapshot_as_of", args.snapshot_as_of)
    debug_data.setdefault("date", args.date)
    debug_data.setdefault("provider", report.get("meta", {}).get("provider", args.provider))
    debug_data.setdefault("n_membership_rows", n_membership_rows)
    debug_data.setdefault("n_membership_unique_tickers", n_membership_unique_tickers)
    debug_data.setdefault("n_membership_unique_concepts", n_membership_unique_concepts)
    debug_data.setdefault("n_prices_unique_tickers", n_prices_unique_tickers)
    debug_data.setdefault("data_date_max", data_date_max)
    debug_data.setdefault("n_candidates_intersection", n_candidates_intersection)
    debug_data.setdefault("n_candidates_after_history_filter", n_candidates_after_history_filter)
    debug_data.setdefault("n_candidates_scored", n_candidates_scored)
    debug_data.setdefault("n_theme_hit_tickers", n_theme_hit_tickers)
    debug_data["top5_theme_totals"] = [
        row.get("score_breakdown", {}).get("score_theme_total") for row in results[:5]
    ]
    debug_data["top5_tickers"] = [row.get("ticker") for row in results[:5]]
    if isinstance(mapper_debug, dict):
        for key in (
            "signals_theme_key_sample",
            "signals_theme_key_count",
            "theme_map_theme_sample",
            "theme_key_miss_count",
            "theme_key_hit_count",
        ):
            if key in mapper_debug:
                debug_data.setdefault(key, mapper_debug[key])
    if "top5_terms_found" not in debug_data:
        top5_terms_found = []
        top5_terms_sample = []
        if membership_terms_by_ticker:
            for row in results[:5]:
                ticker = normalize_ticker(row.get("ticker", ""))
                terms = membership_terms_by_ticker.get(ticker, [])
                top5_terms_found.append(int(len(terms)))
                top5_terms_sample.append(terms[:5])
        debug_data["top5_terms_found"] = top5_terms_found
        debug_data["top5_terms_sample"] = top5_terms_sample
    report["debug"] = debug_data

    report["provenance"] = build_provenance(args, signals_hash, map_hash, snapshot_as_of)
    debug_data = report.get("debug", {})
    if not isinstance(debug_data, dict):
        debug_data = {}
    if isinstance(report.get("provenance", {}).get("args", {}), dict):
        debug_data["theme_map_path"] = report["provenance"]["args"].get("theme_map")
    report["debug"] = debug_data
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
