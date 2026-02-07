#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing json: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_report_path(outputs_dir: Path) -> Path:
    candidates = sorted(outputs_dir.glob("report_*_top*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"no report found under: {outputs_dir}")
    return candidates[0]


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value) or math.isinf(value)
    return False


def _git_rev(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ""


def _to_int(raw: Any, default: int = 0) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _to_float(raw: Any, default: float = 0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _missing_ratios(
    rows: List[Dict[str, Any]],
    object_key: str,
    required_fields: List[str],
) -> Dict[str, float]:
    total = len(rows)
    ratios: Dict[str, float] = {}
    for field in required_fields:
        missing = 0
        for row in rows:
            obj = row.get(object_key)
            if not isinstance(obj, dict):
                missing += 1
                continue
            if field not in obj or _is_missing(obj.get(field)):
                missing += 1
        ratios[field] = float(missing) / float(total) if total else 1.0
    return ratios


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = Path(__file__).resolve().parent / "config.json"
    out_path = repo_root / "artifacts_metrics" / "real_pool_feature_health_latest.json"
    gate_path = repo_root / "artifacts_metrics" / "real_pool_feature_health_gate_latest.json"
    report_path_override = None
    env_report = os.environ.get("REAL_POOL_HEALTH_REPORT_PATH")
    if env_report:
        candidate = Path(env_report)
        report_path_override = candidate if candidate.is_absolute() else repo_root / candidate

    config = _load_json(config_path)
    outputs_dir = repo_root / "outputs"
    report_path = report_path_override or _latest_report_path(outputs_dir)
    report = _load_json(report_path)

    results = report.get("results", [])
    if not isinstance(results, list):
        results = []
    meta = report.get("meta", {})
    if not isinstance(meta, dict):
        meta = {}

    required_indicator_fields = list(config.get("required_indicator_fields", []))
    required_score_breakdown_fields = list(config.get("required_score_breakdown_fields", []))
    min_universe_count = _to_int(config.get("min_universe_count"), 0)
    min_scored_count = _to_int(config.get("min_scored_count"), 0)
    min_coverage_ratio = _to_float(config.get("min_coverage_ratio"), 0.0)
    max_feature_missing_ratio = _to_float(config.get("max_feature_missing_ratio"), 0.0)

    universe_count = _to_int(meta.get("universe_count"), 0)
    scored_count = _to_int(meta.get("scored_count"), 0)
    coverage_ratio = (
        float(scored_count) / float(universe_count) if universe_count > 0 else 0.0
    )

    indicator_missing_ratio = _missing_ratios(results, "indicators", required_indicator_fields)
    score_breakdown_missing_ratio = _missing_ratios(
        results, "score_breakdown", required_score_breakdown_fields
    )

    errors: List[str] = []
    warnings: List[str] = []

    if len(results) == 0:
        errors.append("results is empty")
    if universe_count < min_universe_count:
        errors.append(
            f"universe_count below threshold: {universe_count} < {min_universe_count}"
        )
    if scored_count < min_scored_count:
        errors.append(f"scored_count below threshold: {scored_count} < {min_scored_count}")
    if coverage_ratio < min_coverage_ratio:
        errors.append(
            f"coverage_ratio below threshold: {coverage_ratio:.4f} < {min_coverage_ratio:.4f}"
        )

    for field, ratio in indicator_missing_ratio.items():
        if ratio > max_feature_missing_ratio:
            errors.append(
                f"indicator missing ratio too high: {field}={ratio:.4f} > {max_feature_missing_ratio:.4f}"
            )
    for field, ratio in score_breakdown_missing_ratio.items():
        if ratio > max_feature_missing_ratio:
            errors.append(
                f"score_breakdown missing ratio too high: {field}={ratio:.4f} > {max_feature_missing_ratio:.4f}"
            )

    if scored_count > 0 and len(results) < min(scored_count, 5):
        warnings.append(
            f"topn results smaller than expected sample: results={len(results)} scored_count={scored_count}"
        )

    payload: Dict[str, Any] = {
        "status": "passed" if not errors else "failed",
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "report_path": str(report_path),
        "as_of": report.get("as_of"),
        "provider": meta.get("provider"),
        "universe_count": universe_count,
        "scored_count": scored_count,
        "coverage_ratio": coverage_ratio,
        "results_count": len(results),
        "indicator_missing_ratio": indicator_missing_ratio,
        "score_breakdown_missing_ratio": score_breakdown_missing_ratio,
        "git_rev": _git_rev(repo_root),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    gate_path.write_text(
        json.dumps(
            {
                "status": payload["status"],
                "error_count": payload["error_count"],
                "warning_count": payload["warning_count"],
                "errors": payload["errors"],
                "warnings": payload["warnings"],
                "source": str(out_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        "[real_pool_feature_health] status={status} errors={errors} warnings={warnings} out={out}".format(
            status=payload["status"],
            errors=payload["error_count"],
            warnings=payload["warning_count"],
            out=out_path,
        )
    )
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
