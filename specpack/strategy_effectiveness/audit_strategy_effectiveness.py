#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing json: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _to_float(value: Any, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric value for {field_name}: {value!r}") from exc


def _to_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid integer value for {field_name}: {value!r}") from exc


def _append_error(errors: List[str], msg: str) -> None:
    errors.append(msg)


def _append_warning(warnings: List[str], msg: str) -> None:
    warnings.append(msg)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = Path(__file__).resolve().parent / "config.json"
    metrics_path = repo_root / "artifacts_metrics" / "strategy_effectiveness_latest.json"
    gate_path = repo_root / "artifacts_metrics" / "strategy_effectiveness_gate_latest.json"

    config = _load_json(config_path)
    metrics = _load_json(metrics_path)

    overall = metrics.get("overall", {})
    if not isinstance(overall, dict):
        raise AssertionError("metrics.overall missing")
    per_horizon = metrics.get("per_horizon", {})
    if not isinstance(per_horizon, dict) or not per_horizon:
        raise AssertionError("metrics.per_horizon missing")

    errors: List[str] = []
    warnings: List[str] = []

    dates_count = _to_int(overall.get("dates_count"), "overall.dates_count")
    horizons_count = _to_int(overall.get("horizons_count"), "overall.horizons_count")
    min_dates = _to_int(config.get("min_dates", 0), "config.min_dates")
    min_horizons = _to_int(config.get("min_horizons", 0), "config.min_horizons")
    if dates_count < min_dates:
        _append_error(errors, f"dates_count below threshold: {dates_count} < {min_dates}")
    if horizons_count < min_horizons:
        _append_error(errors, f"horizons_count below threshold: {horizons_count} < {min_horizons}")

    hard_min_enhanced_mean = _to_float(
        config.get("min_enhanced_mean_return", 0.0), "config.min_enhanced_mean_return"
    )
    hard_max_enhanced_dd = _to_float(
        config.get("max_enhanced_max_drawdown", 1.0), "config.max_enhanced_max_drawdown"
    )
    hard_min_excess_mean = _to_float(
        config.get("min_excess_mean_return_hard", -1.0), "config.min_excess_mean_return_hard"
    )
    hard_min_excess_win_rate = _to_float(
        config.get("min_excess_win_rate_hard", 0.0), "config.min_excess_win_rate_hard"
    )
    hard_min_cumulative_spread = _to_float(
        config.get("min_cumulative_spread_hard", -1.0), "config.min_cumulative_spread_hard"
    )
    hard_min_horizons_non_negative = _to_int(
        config.get("min_horizons_with_non_negative_mean_excess_hard", 0),
        "config.min_horizons_with_non_negative_mean_excess_hard",
    )

    target_min_excess_mean = _to_float(
        config.get("min_excess_mean_return_target", 0.0), "config.min_excess_mean_return_target"
    )
    target_min_excess_win_rate = _to_float(
        config.get("min_excess_win_rate_target", 0.0), "config.min_excess_win_rate_target"
    )
    target_min_cumulative_spread = _to_float(
        config.get("min_cumulative_spread_target", 0.0), "config.min_cumulative_spread_target"
    )
    target_min_horizons_non_negative = _to_int(
        config.get("min_horizons_with_non_negative_mean_excess_target", 0),
        "config.min_horizons_with_non_negative_mean_excess_target",
    )

    horizons_with_non_negative_mean_excess = 0
    for horizon_key in sorted(per_horizon.keys(), key=lambda x: int(str(x))):
        row = per_horizon[horizon_key]
        if not isinstance(row, dict):
            _append_error(errors, f"horizon[{horizon_key}] invalid object")
            continue

        mean_enhanced = _to_float(
            row.get("mean_enhanced_return"), f"horizon[{horizon_key}].mean_enhanced_return"
        )
        mean_excess = _to_float(
            row.get("mean_excess_return"), f"horizon[{horizon_key}].mean_excess_return"
        )
        win_rate_excess = _to_float(
            row.get("excess_win_rate"), f"horizon[{horizon_key}].excess_win_rate"
        )
        cumulative_spread = _to_float(
            row.get("cumulative_spread"), f"horizon[{horizon_key}].cumulative_spread"
        )
        max_drawdown_enhanced = _to_float(
            row.get("max_drawdown_enhanced"), f"horizon[{horizon_key}].max_drawdown_enhanced"
        )

        if mean_excess >= 0.0:
            horizons_with_non_negative_mean_excess += 1

        if mean_enhanced < hard_min_enhanced_mean:
            _append_error(
                errors,
                f"h{horizon_key} mean_enhanced_return below threshold: "
                f"{mean_enhanced:.6f} < {hard_min_enhanced_mean:.6f}",
            )
        if max_drawdown_enhanced > hard_max_enhanced_dd:
            _append_error(
                errors,
                f"h{horizon_key} max_drawdown_enhanced above threshold: "
                f"{max_drawdown_enhanced:.6f} > {hard_max_enhanced_dd:.6f}",
            )
        if mean_excess < hard_min_excess_mean:
            _append_error(
                errors,
                f"h{horizon_key} mean_excess_return below hard threshold: "
                f"{mean_excess:.6f} < {hard_min_excess_mean:.6f}",
            )
        if win_rate_excess < hard_min_excess_win_rate:
            _append_error(
                errors,
                f"h{horizon_key} excess_win_rate below hard threshold: "
                f"{win_rate_excess:.4f} < {hard_min_excess_win_rate:.4f}",
            )
        if cumulative_spread < hard_min_cumulative_spread:
            _append_error(
                errors,
                f"h{horizon_key} cumulative_spread below hard threshold: "
                f"{cumulative_spread:.6f} < {hard_min_cumulative_spread:.6f}",
            )

        if mean_excess < target_min_excess_mean:
            _append_warning(
                warnings,
                f"h{horizon_key} mean_excess_return below target: "
                f"{mean_excess:.6f} < {target_min_excess_mean:.6f}",
            )
        if win_rate_excess < target_min_excess_win_rate:
            _append_warning(
                warnings,
                f"h{horizon_key} excess_win_rate below target: "
                f"{win_rate_excess:.4f} < {target_min_excess_win_rate:.4f}",
            )
        if cumulative_spread < target_min_cumulative_spread:
            _append_warning(
                warnings,
                f"h{horizon_key} cumulative_spread below target: "
                f"{cumulative_spread:.6f} < {target_min_cumulative_spread:.6f}",
            )

    if horizons_with_non_negative_mean_excess < hard_min_horizons_non_negative:
        _append_error(
            errors,
            "horizons_with_non_negative_mean_excess below hard threshold: "
            f"{horizons_with_non_negative_mean_excess} < {hard_min_horizons_non_negative}",
        )

    if horizons_with_non_negative_mean_excess < target_min_horizons_non_negative:
        _append_warning(
            warnings,
            "horizons_with_non_negative_mean_excess below target: "
            f"{horizons_with_non_negative_mean_excess} < {target_min_horizons_non_negative}",
        )

    gate_payload = {
        "status": "passed" if not errors else "failed",
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "horizons_with_non_negative_mean_excess": horizons_with_non_negative_mean_excess,
        "metrics_path": str(metrics_path),
    }
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(json.dumps(gate_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[strategy_effectiveness_gate] status={status} errors={errors} warnings={warnings} out={out}".format(
            status=gate_payload["status"],
            errors=gate_payload["error_count"],
            warnings=gate_payload["warning_count"],
            out=gate_path,
        )
    )
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
