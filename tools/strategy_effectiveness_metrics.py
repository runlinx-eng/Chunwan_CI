#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

EPS = 1e-12


def _git_rev(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return out
    except Exception:
        return ""


def _to_float(value: Any, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric value for {field_name}: {value!r}") from exc


def _compound_and_drawdown(returns: List[float]) -> Tuple[float, float]:
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for ret in returns:
        equity *= 1.0 + ret
        if equity > peak:
            peak = equity
        if peak > EPS:
            drawdown = 1.0 - (equity / peak)
            if drawdown > max_drawdown:
                max_drawdown = drawdown
    return equity - 1.0, max_drawdown


def _resolve_horizons(report: Dict[str, Any]) -> List[str]:
    config = report.get("config", {})
    raw_horizons = config.get("horizons", []) if isinstance(config, dict) else []
    horizons: List[str] = []
    if isinstance(raw_horizons, list):
        for item in raw_horizons:
            if item is None:
                continue
            horizons.append(str(item))
    if horizons:
        return sorted(set(horizons), key=lambda x: int(x))

    results = report.get("results", [])
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            horizons_map = first.get("horizons", {})
            if isinstance(horizons_map, dict):
                keys = [str(k) for k in horizons_map.keys()]
                return sorted(set(keys), key=lambda x: int(x))
    return []


def _summarize_horizon(results: List[Dict[str, Any]], horizon: str) -> Dict[str, Any]:
    baseline_returns: List[float] = []
    enhanced_returns: List[float] = []
    dates: List[str] = []

    for row in results:
        horizons = row.get("horizons", {})
        if not isinstance(horizons, dict):
            raise ValueError(f"row.horizons missing for horizon={horizon}")
        data = horizons.get(horizon)
        if not isinstance(data, dict):
            raise ValueError(f"missing horizon={horizon} in one result row")
        baseline = _to_float(data.get("baseline_return"), f"h{horizon}.baseline_return")
        enhanced = _to_float(data.get("enhanced_return"), f"h{horizon}.enhanced_return")
        baseline_returns.append(baseline)
        enhanced_returns.append(enhanced)
        dates.append(str(row.get("date", "")))

    if not baseline_returns or not enhanced_returns:
        raise ValueError(f"empty returns for horizon={horizon}")

    excess_returns = [enh - base for base, enh in zip(baseline_returns, enhanced_returns)]

    cumulative_baseline, max_drawdown_baseline = _compound_and_drawdown(baseline_returns)
    cumulative_enhanced, max_drawdown_enhanced = _compound_and_drawdown(enhanced_returns)
    cumulative_excess_curve, max_drawdown_excess = _compound_and_drawdown(excess_returns)

    n = len(excess_returns)
    win_rate_excess = sum(1 for x in excess_returns if x > 0.0) / n
    non_negative_rate_excess = sum(1 for x in excess_returns if x >= 0.0) / n
    enhanced_hit_rate = sum(1 for x in enhanced_returns if x > 0.0) / n
    baseline_hit_rate = sum(1 for x in baseline_returns if x > 0.0) / n

    mean_baseline = statistics.fmean(baseline_returns)
    mean_enhanced = statistics.fmean(enhanced_returns)
    mean_excess = statistics.fmean(excess_returns)

    summary = {
        "horizon": int(horizon),
        "n": n,
        "dates": dates,
        "mean_baseline_return": mean_baseline,
        "mean_enhanced_return": mean_enhanced,
        "mean_excess_return": mean_excess,
        "median_excess_return": statistics.median(excess_returns),
        "std_excess_return": statistics.pstdev(excess_returns),
        "baseline_hit_rate": baseline_hit_rate,
        "enhanced_hit_rate": enhanced_hit_rate,
        "excess_win_rate": win_rate_excess,
        "excess_non_negative_rate": non_negative_rate_excess,
        "cumulative_baseline_return": cumulative_baseline,
        "cumulative_enhanced_return": cumulative_enhanced,
        "cumulative_spread": cumulative_enhanced - cumulative_baseline,
        "cumulative_excess_curve_return": cumulative_excess_curve,
        "max_drawdown_baseline": max_drawdown_baseline,
        "max_drawdown_enhanced": max_drawdown_enhanced,
        "max_drawdown_excess_curve": max_drawdown_excess,
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute strategy effectiveness metrics from backtest_regression output"
    )
    parser.add_argument(
        "--input",
        default="outputs/backtest_regression_2026-01-20.json",
        help="path to backtest_regression output json",
    )
    parser.add_argument(
        "--out",
        default="artifacts_metrics/strategy_effectiveness_latest.json",
        help="output metrics path",
    )
    parser.add_argument(
        "--write-history",
        action="store_true",
        help="also write timestamped copy to artifacts_metrics",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    input_path = (repo_root / args.input).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"missing input: {input_path}")

    report = json.loads(input_path.read_text(encoding="utf-8"))
    results = report.get("results", [])
    if not isinstance(results, list) or not results:
        raise ValueError("invalid backtest report: results is empty")

    horizons = _resolve_horizons(report)
    if not horizons:
        raise ValueError("invalid backtest report: missing horizons")

    per_horizon: Dict[str, Dict[str, Any]] = {}
    for horizon in horizons:
        per_horizon[horizon] = _summarize_horizon(results, horizon)

    mean_excess_values = [float(m["mean_excess_return"]) for m in per_horizon.values()]
    mean_enhanced_values = [float(m["mean_enhanced_return"]) for m in per_horizon.values()]
    cumulative_spreads = [float(m["cumulative_spread"]) for m in per_horizon.values()]
    enhanced_drawdowns = [float(m["max_drawdown_enhanced"]) for m in per_horizon.values()]
    excess_win_rates = [float(m["excess_win_rate"]) for m in per_horizon.values()]

    overall = {
        "dates_count": len(results),
        "horizons_count": len(per_horizon),
        "horizons_with_non_negative_mean_excess": sum(1 for x in mean_excess_values if x >= 0.0),
        "horizons_with_positive_excess_win_rate": sum(1 for x in excess_win_rates if x > 0.0),
        "mean_excess_return_avg": statistics.fmean(mean_excess_values),
        "worst_mean_excess_return": min(mean_excess_values),
        "worst_cumulative_spread": min(cumulative_spreads),
        "worst_max_drawdown_enhanced": max(enhanced_drawdowns),
        "all_horizons_enhanced_mean_positive": all(x > 0.0 for x in mean_enhanced_values),
    }

    finished_at = datetime.now(timezone.utc)
    payload = {
        "created_at": finished_at.isoformat().replace("+00:00", "Z"),
        "git_rev": _git_rev(repo_root),
        "snapshot_as_of": report.get("snapshot_as_of"),
        "source_path": str(input_path),
        "per_horizon": per_horizon,
        "overall": overall,
    }

    out_path = (repo_root / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.write_history:
        ts = finished_at.strftime("%Y%m%d_%H%M%S")
        history_path = out_path.parent / f"strategy_effectiveness_{ts}.json"
        history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[strategy_effectiveness] dates={dates} horizons={horizons} worst_mean_excess={excess:.6f} out={out}".format(
            dates=overall["dates_count"],
            horizons=overall["horizons_count"],
            excess=overall["worst_mean_excess_return"],
            out=out_path,
        )
    )


if __name__ == "__main__":
    main()
