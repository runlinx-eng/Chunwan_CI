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
        return {"delta_p50": 0, "delta_p95": 0, "delta_p99": 0, "delta_unique_count": 0}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        "delta_p50": float(raw.get("delta_p50", 0)),
        "delta_p95": float(raw.get("delta_p95", 0)),
        "delta_p99": float(raw.get("delta_p99", 0)),
        "delta_unique_count": float(raw.get("delta_unique_count", 0)),
    }


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

    if not enhanced_base:
        raise AssertionError("baseline enhanced reports missing")
    if not enhanced_latest:
        raise AssertionError("latest enhanced reports missing")
    if not tech_latest:
        raise AssertionError("latest tech_only reports missing")

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
            f"top_offenders={offenders}"
        )

    print(
        "[theme_precision] ok: "
        f"latest={metrics_latest}; baseline={metrics_base}; "
        f"positive_latest={latest_positive}; positive_baseline={base_positive}"
    )


if __name__ == "__main__":
    main()
