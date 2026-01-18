import json
import math
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing metrics: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[int(idx)]
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def _extract_reports(metrics: Dict[str, Any], category: str) -> List[Dict[str, Any]]:
    reports = metrics.get("reports", [])
    if not isinstance(reports, list):
        return []
    return [r for r in reports if r.get("category") == category]


def _unique_themes(reports: List[Dict[str, Any]]) -> List[str]:
    themes = []
    for report in reports:
        themes_used = report.get("themes_used", {})
        if isinstance(themes_used, dict):
            for theme in themes_used.get("unique", []) or []:
                theme_str = str(theme)
                if theme_str and theme_str not in themes:
                    themes.append(theme_str)
    return themes


def _theme_totals(reports: List[Dict[str, Any]]) -> List[float]:
    avgs = []
    for report in reports:
        totals = report.get("theme_total", {})
        if isinstance(totals, dict) and totals.get("count", 0) > 0:
            avgs.append(float(totals.get("avg", 0.0)))
    return avgs


def _positive_counts(reports: List[Dict[str, Any]]) -> Tuple[int, int]:
    positive = 0
    total = 0
    for report in reports:
        totals = report.get("theme_total", {})
        if not isinstance(totals, dict):
            continue
        positive += int(totals.get("positive", 0))
        total += int(totals.get("count", 0))
    return positive, total


def _techonly_violations(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    violations = []
    for report in reports:
        totals = report.get("theme_total", {})
        if not isinstance(totals, dict):
            continue
        max_val = totals.get("max")
        min_val = totals.get("min")
        if max_val is None or min_val is None:
            violations.append({"path": report.get("path"), "max": max_val, "min": min_val})
            continue
        if abs(float(max_val)) > 1e-9 or abs(float(min_val)) > 1e-9:
            violations.append({"path": report.get("path"), "max": max_val, "min": min_val})
    return violations


def _top_offenders(reports: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    offenders = []
    for report in reports:
        totals = report.get("theme_total", {})
        avg_val = float(totals.get("avg", 0.0)) if isinstance(totals, dict) else 0.0
        offenders.append(
            {
                "path": report.get("path"),
                "avg": avg_val,
                "positive": totals.get("positive") if isinstance(totals, dict) else None,
                "count": totals.get("count") if isinstance(totals, dict) else None,
            }
        )
    offenders.sort(key=lambda x: (x["avg"], x["path"] or ""))
    return offenders[:limit]


def _load_config(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {
            "delta_p50": 0,
            "delta_p95": 0,
            "delta_p99": 0,
            "delta_unique_count": 0,
            "min_concept_hits_unique_set_enhanced": 1,
            "min_theme_total_range_enhanced": 0,
            "min_theme_total_unique_enhanced": 1,
            "min_theme_hit_unique_set_enhanced": 2,
            "min_theme_hit_unique_set_all": 2,
            "min_concept_hits_unique_set_all": 1,
        }
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        "delta_p50": float(raw.get("delta_p50", 0)),
        "delta_p95": float(raw.get("delta_p95", 0)),
        "delta_p99": float(raw.get("delta_p99", 0)),
        "delta_unique_count": float(raw.get("delta_unique_count", 0)),
        "min_concept_hits_unique_set_enhanced": float(
            raw.get("min_concept_hits_unique_set_enhanced", 1)
        ),
        "min_theme_total_range_enhanced": float(raw.get("min_theme_total_range_enhanced", 0)),
        "min_theme_total_unique_enhanced": float(raw.get("min_theme_total_unique_enhanced", 1)),
        "min_theme_hit_unique_set_enhanced": float(raw.get("min_theme_hit_unique_set_enhanced", 2)),
        "min_theme_hit_unique_set_all": float(raw.get("min_theme_hit_unique_set_all", 2)),
        "min_concept_hits_unique_set_all": float(raw.get("min_concept_hits_unique_set_all", 1)),
    }


def _result_bucket(metrics: Dict[str, Any], category: str) -> Dict[str, Any]:
    result_level = metrics.get("result_level", {})
    if not isinstance(result_level, dict):
        return {}
    bucket = result_level.get(category, {})
    return bucket if isinstance(bucket, dict) else {}


def _result_metric(bucket: Dict[str, Any], metric: str) -> Dict[str, Any]:
    stats = bucket.get(metric, {})
    return stats if isinstance(stats, dict) else {}


def _stat_value(stats: Dict[str, Any], key: str) -> Optional[float]:
    value = stats.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _summarize_result_level(metrics: Dict[str, Any]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    summary: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for category in ("all", "enhanced", "tech_only"):
        bucket = _result_bucket(metrics, category)
        metric_summary: Dict[str, Dict[str, Any]] = {}
        for metric in ("theme_total", "themes_used", "concept_hits"):
            stats = _result_metric(bucket, metric)
            metric_summary[metric] = {
                "min": stats.get("min"),
                "p50": stats.get("p50"),
                "p95": stats.get("p95"),
                "p99": stats.get("p99"),
                "max": stats.get("max"),
                "unique_value_count": stats.get("unique_value_count"),
                "unique_set_count": stats.get("unique_set_count"),
            }
        summary[category] = metric_summary
    return summary


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    baseline_path = repo_root / "artifacts_metrics" / "theme_precision_baseline.json"
    latest_path = repo_root / "artifacts_metrics" / "theme_precision_latest.json"
    config_path = Path(__file__).resolve().parent / "config.json"

    baseline_missing = not baseline_path.exists()
    require_baseline = str(os.environ.get("THEME_PRECISION_REQUIRE_BASELINE", "")).lower() in {
        "1",
        "true",
        "yes",
    }
    ci_env = str(os.environ.get("CI", "")).lower() in {"1", "true", "yes"}
    if baseline_missing:
        if ci_env or require_baseline:
            raise FileNotFoundError(
                f"missing baseline: {baseline_path}. "
                "Generate with: python tools/update_theme_precision_baseline.sh"
            )
        if not latest_path.exists():
            raise FileNotFoundError(
                f"missing latest metrics: {latest_path}. "
                "Run verify workflow to generate latest first."
            )
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(latest_path, baseline_path)
        print(f"[theme_precision] baseline missing; bootstrapped from latest: {baseline_path}")

    baseline = _load_json(baseline_path)
    latest = _load_json(latest_path)
    config = _load_config(config_path)

    enhanced_base = _extract_reports(baseline, "enhanced")
    enhanced_latest = _extract_reports(latest, "enhanced")
    tech_latest = _extract_reports(latest, "tech_only")
    result_summary = _summarize_result_level(latest)
    enhanced_result = _result_bucket(latest, "enhanced")
    tech_result = _result_bucket(latest, "tech_only")

    if not enhanced_base:
        raise AssertionError("baseline enhanced reports missing")
    if not enhanced_latest:
        raise AssertionError("latest enhanced reports missing")
    if not tech_latest:
        raise AssertionError("latest tech_only reports missing")
    if not enhanced_result:
        raise AssertionError("latest result_level enhanced missing")
    if not tech_result:
        raise AssertionError("latest result_level tech_only missing")

    tech_violations = _techonly_violations(tech_latest)
    if tech_violations:
        raise AssertionError(f"tech_only score_theme_total not zero: {tech_violations}")

    base_positive, _ = _positive_counts(enhanced_base)
    latest_positive, _ = _positive_counts(enhanced_latest)
    if base_positive > 0 and latest_positive < base_positive * 0.9:
        raise AssertionError(
            f"enhanced positive count dropped: {latest_positive} < {base_positive} * 0.9"
        )

    base_values = _theme_totals(enhanced_base)
    latest_values = _theme_totals(enhanced_latest)
    base_unique = len(_unique_themes(enhanced_base))
    latest_unique = len(_unique_themes(enhanced_latest))

    metrics_base = {
        "N": len(base_values),
        "p50": _percentile(base_values, 0.5),
        "p95": _percentile(base_values, 0.95),
        "p99": _percentile(base_values, 0.99),
        "unique_count": base_unique,
    }
    metrics_latest = {
        "N": len(latest_values),
        "p50": _percentile(latest_values, 0.5),
        "p95": _percentile(latest_values, 0.95),
        "p99": _percentile(latest_values, 0.99),
        "unique_count": latest_unique,
    }

    failures = []
    if metrics_latest["p50"] is None or metrics_base["p50"] is None:
        failures.append("p50 missing")
    elif metrics_latest["p50"] < metrics_base["p50"] + config["delta_p50"]:
        failures.append("p50 below baseline")
    if metrics_latest["p95"] is None or metrics_base["p95"] is None:
        failures.append("p95 missing")
    elif metrics_latest["p95"] < metrics_base["p95"] + config["delta_p95"]:
        failures.append("p95 below baseline")
    if metrics_latest["p99"] is None or metrics_base["p99"] is None:
        failures.append("p99 missing")
    elif metrics_latest["p99"] < metrics_base["p99"] + config["delta_p99"]:
        failures.append("p99 below baseline")
    if metrics_latest["unique_count"] < metrics_base["unique_count"] + config["delta_unique_count"]:
        failures.append("unique_count below baseline")

    if failures:
        offenders = _top_offenders(enhanced_latest)
        raise AssertionError(
            "theme_precision failures: "
            f"{failures}; latest={metrics_latest}; baseline={metrics_base}; "
            f"top_offenders={offenders}; result_level={json.dumps(result_summary, ensure_ascii=False, sort_keys=True)}"
        )

    non_deg_failures = []
    enhanced_theme_total = _result_metric(enhanced_result, "theme_total")
    enhanced_themes_used = _result_metric(enhanced_result, "themes_used")
    enhanced_concept_hits = _result_metric(enhanced_result, "concept_hits")
    tech_theme_total = _result_metric(tech_result, "theme_total")

    concept_hits_unique_set = _stat_value(enhanced_concept_hits, "unique_set_count")
    if concept_hits_unique_set is not None and concept_hits_unique_set < config[
        "min_concept_hits_unique_set_enhanced"
    ]:
        non_deg_failures.append("concept_hits unique_set_count below minimum")

    theme_total_min = _stat_value(enhanced_theme_total, "min")
    theme_total_max = _stat_value(enhanced_theme_total, "max")
    if theme_total_min is None or theme_total_max is None:
        non_deg_failures.append("theme_total range missing")
    else:
        theme_total_range = theme_total_max - theme_total_min
        if theme_total_range < config["min_theme_total_range_enhanced"]:
            non_deg_failures.append("theme_total range below minimum")

    theme_total_unique = _stat_value(enhanced_theme_total, "unique_value_count")
    if theme_total_unique is None:
        non_deg_failures.append("theme_total unique_value_count missing")
    elif theme_total_unique < config["min_theme_total_unique_enhanced"]:
        non_deg_failures.append("theme_total unique_value_count below minimum")

    theme_hit_unique_set = _stat_value(enhanced_theme_total, "unique_set_count")
    if theme_hit_unique_set is None:
        non_deg_failures.append("theme_total unique_set_count missing")
    elif theme_hit_unique_set < config["min_theme_hit_unique_set_enhanced"]:
        non_deg_failures.append("theme_total unique_set_count below minimum")

    all_theme_total = _result_metric(_result_bucket(latest, "all"), "theme_total")
    theme_hit_unique_set_all = _stat_value(all_theme_total, "unique_set_count")
    if theme_hit_unique_set_all is None:
        non_deg_failures.append("theme_total unique_set_count (all) missing")
    elif theme_hit_unique_set_all < config["min_theme_hit_unique_set_all"]:
        non_deg_failures.append("theme_total unique_set_count (all) below minimum")

    all_concept_hits = _result_metric(_result_bucket(latest, "all"), "concept_hits")
    concept_hits_unique_set_all = _stat_value(all_concept_hits, "unique_set_count")
    if concept_hits_unique_set_all is None:
        non_deg_failures.append("concept_hits unique_set_count (all) missing")
    elif concept_hits_unique_set_all < config["min_concept_hits_unique_set_all"]:
        non_deg_failures.append("concept_hits unique_set_count (all) below minimum")

    tech_theme_max = _stat_value(tech_theme_total, "max")
    if tech_theme_max is None:
        non_deg_failures.append("tech_only theme_total max missing")
    elif abs(tech_theme_max) > 1e-9:
        non_deg_failures.append("tech_only theme_total max not zero")

    if non_deg_failures:
        raise AssertionError(
            "theme_precision non-degeneracy failures: "
            f"{non_deg_failures}; result_level={json.dumps(result_summary, ensure_ascii=False, sort_keys=True)}"
        )

    print(
        "[theme_precision] ok: "
        f"latest={metrics_latest}; baseline={metrics_base}; "
        f"positive_latest={latest_positive}; positive_baseline={base_positive}; "
        f"result_level={json.dumps(result_summary, ensure_ascii=False, sort_keys=True)}"
    )


if __name__ == "__main__":
    main()
