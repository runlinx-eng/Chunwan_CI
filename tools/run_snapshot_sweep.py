import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_default_theme_map(repo_root: Path) -> Path:
    run_py = repo_root / "src" / "run.py"
    if run_py.exists():
        text = run_py.read_text(encoding="utf-8")
        match = re.search(r"--theme-map\".*?default=[\"']([^\"']+)[\"']", text, re.S)
        if match:
            value = match.group(1).strip()
            candidate = Path(value)
            return candidate if candidate.is_absolute() else repo_root / candidate
    return repo_root / "theme_to_industry_em_2026-01-20.csv"


def _resolve_theme_map(repo_root: Path, snapshot_id: str) -> Tuple[Path, bool]:
    snapshot_map = repo_root / f"theme_to_industry_em_{snapshot_id}.csv"
    if snapshot_map.exists():
        return snapshot_map, False
    legacy_map = repo_root / f"theme_to_industry_{snapshot_id}.csv"
    if legacy_map.exists():
        return legacy_map, False
    return _read_default_theme_map(repo_root), True


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_input_pool(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"input pool not found: {path}")
    suffix = path.suffix.lower()
    row_count = 0
    id_field = ""
    if suffix == ".jsonl":
        for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            row_count += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid jsonl at line {idx}: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"jsonl line {idx} must be an object")
            if "item_id" in row:
                id_field = "item_id"
            elif "ticker" in row:
                id_field = "ticker"
            else:
                raise ValueError(f"jsonl line {idx} missing item_id or ticker")
    elif suffix == ".csv":
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            if "item_id" in fieldnames:
                id_field = "item_id"
            elif "ticker" in fieldnames:
                id_field = "ticker"
            else:
                raise ValueError(
                    f"input pool csv missing item_id or ticker columns; found {fieldnames}"
                )
            for idx, row in enumerate(reader, start=2):
                row_count += 1
                value = row.get(id_field, "")
                if not value:
                    raise ValueError(f"csv row {idx} missing {id_field}")
    else:
        raise ValueError("unsupported input pool format; use .jsonl or .csv")

    if row_count == 0:
        raise ValueError(f"input pool has no rows: {path}")

    return {
        "path": str(path.resolve()),
        "rows": row_count,
        "sha256": _sha256_file(path),
        "id_field": id_field,
    }


def _read_snapshots(args: argparse.Namespace) -> List[str]:
    if args.discover_latest:
        snapshots_dir = REPO_ROOT / "data" / "snapshots"
        if not snapshots_dir.exists():
            raise FileNotFoundError(
                "snapshots directory not found; use --snapshots or --snapshots-file"
            )
        candidates = []
        for path in snapshots_dir.iterdir():
            if not path.is_dir():
                continue
            name = path.name
            if re.match(r"^\d{4}-\d{2}-\d{2}$", name):
                candidates.append(name)
        if not candidates:
            raise ValueError(
                "no snapshot directories found; use --snapshots or --snapshots-file"
            )
        candidates = sorted(candidates, reverse=True)
        return candidates[: args.discover_latest]
    if args.snapshots:
        return [item.strip() for item in args.snapshots.split(",") if item.strip()]
    if args.snapshots_file:
        path = Path(args.snapshots_file)
        if not path.is_absolute():
            path = REPO_ROOT / path
        if not path.exists():
            raise FileNotFoundError(f"snapshots file not found: {path}")
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    raise ValueError("provide --snapshots or --snapshots-file")


def _run(cmd: List[str], env: Dict[str, str]) -> None:
    subprocess.check_call(cmd, cwd=REPO_ROOT, env=env)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing json: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_theme_precision_thresholds(repo_root: Path) -> Dict[str, float]:
    config_path = repo_root / "specpack" / "theme_precision" / "config.json"
    if not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    thresholds = {}
    for key in (
        "min_theme_total_unique_value_ratio_enhanced",
        "min_theme_total_unique_value_ratio_all",
    ):
        if key in payload:
            try:
                thresholds[key] = float(payload[key])
            except (TypeError, ValueError):
                continue
    return thresholds


def main() -> int:
    parser = argparse.ArgumentParser(description="Run snapshot sweep regression matrix")
    parser.add_argument("--snapshots", default="", help="comma-separated snapshot ids")
    parser.add_argument("--snapshots-file", default="", help="file with snapshot ids per line")
    parser.add_argument("--discover-latest", type=int, default=0, help="auto-discover latest N")
    parser.add_argument("--input-pool", default="", help="optional input pool path")
    parser.add_argument("--top-n", type=int, default=10, help="top n for export")
    parser.add_argument("--sort-key", default="final_score", help="sort key for export")
    parser.add_argument("--gate", action="store_true", help="enable regression gate")
    parser.add_argument(
        "--min-theme-unique-ratio-enhanced",
        type=float,
        default=None,
        help="minimum enhanced theme unique ratio",
    )
    parser.add_argument(
        "--min-theme-unique-ratio-all",
        type=float,
        default=None,
        help="minimum all theme unique ratio",
    )
    parser.add_argument(
        "--out-path",
        default="artifacts_metrics/regression_matrix_timeseries_latest.json",
        help="output path",
    )
    args = parser.parse_args()

    snapshots = _read_snapshots(args)
    if not snapshots:
        raise ValueError("no snapshots provided")

    pool_path: Optional[Path] = None
    if args.input_pool:
        pool_path = Path(args.input_pool)
        if not pool_path.is_absolute():
            pool_path = REPO_ROOT / pool_path
        pool_meta = _load_input_pool(pool_path)
    else:
        pool_meta = {"strategy": "snapshot_universe"}

    thresholds = _load_theme_precision_thresholds(REPO_ROOT)
    min_ratio_enhanced = args.min_theme_unique_ratio_enhanced
    if min_ratio_enhanced is None:
        min_ratio_enhanced = thresholds.get("min_theme_total_unique_value_ratio_enhanced", 0.02)
    min_ratio_all = args.min_theme_unique_ratio_all
    if min_ratio_all is None:
        min_ratio_all = thresholds.get("min_theme_total_unique_value_ratio_all", 0.02)

    out_path = Path(args.out_path)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []
    snapshots_requested = list(snapshots)
    failures = 0
    snapshots_skipped = 0
    regressions: List[Dict[str, Any]] = []
    last_active_sha: Optional[str] = None

    for snapshot_id in snapshots:
        entry: Dict[str, Any] = {"snapshot_id": snapshot_id, "warnings": [], "errors": []}
        try:
            theme_map_path, fallback = _resolve_theme_map(REPO_ROOT, snapshot_id)
            if not theme_map_path.exists():
                raise FileNotFoundError(f"theme map not found: {theme_map_path}")
            theme_map_sha = _sha256_file(theme_map_path)
            entry["default_theme_map_path"] = str(theme_map_path)
            entry["default_theme_map_sha256"] = theme_map_sha
            entry["theme_map_fallback"] = fallback

            env = os.environ.copy()
            env["THEME_MAP"] = str(theme_map_path)

            build_cmd = [
                sys.executable,
                "tools/build_screener_candidates.py",
                "--snapshot-id",
                snapshot_id,
            ]
            if pool_path is not None:
                build_cmd += ["--input-pool", str(pool_path), "--on-empty-pool", "skip"]
            _run(build_cmd, env)

            candidates_meta_path = (
                REPO_ROOT / "artifacts_metrics" / "screener_candidates_latest_meta.json"
            )
            candidates_meta = _load_json(candidates_meta_path)
            pool_output = candidates_meta.get("output", {}) if isinstance(candidates_meta, dict) else {}
            pool_summary = {
                "rows": pool_output.get("rows"),
                "mode_distribution": pool_output.get("mode_distribution", {}),
            }
            entry["pool_coverage_summary"] = pool_summary
            if pool_output.get("rows") == 0 or candidates_meta.get("reason") == "empty_pool":
                entry["warnings"].append("empty_pool_for_snapshot")
                snapshots_skipped += 1
                results.append(entry)
                continue

            _run(
                [
                    sys.executable,
                    "tools/export_screener_topn.py",
                    "--top-n",
                    str(args.top_n),
                    "--sort-key",
                    args.sort_key,
                    "--modes",
                    "all,enhanced,tech_only",
                    "--source-path",
                    "artifacts_metrics/screener_candidates_latest.jsonl",
                ],
                env,
            )
            _run(
                [
                    sys.executable,
                    "tools/build_regression_matrix.py",
                ],
                env,
            )

            regression_path = REPO_ROOT / "artifacts_metrics" / "regression_matrix_latest.json"
            regression = _load_json(regression_path)
            entry["git_rev"] = regression.get("git_rev")
            entry["latest_log_path"] = regression.get("latest_log_path")
            entry["theme_precision_summary"] = regression.get("theme_precision_summary")
            entry["global_coverage_summary"] = regression.get("screener_coverage_summary")
            entry["screener_topn_meta"] = regression.get("screener_topn_meta")

            topn_meta_path = REPO_ROOT / "artifacts_metrics" / "screener_topn_latest_meta.json"
            topn_meta = _load_json(topn_meta_path)

            entry["active_theme_map_path"] = topn_meta.get("theme_map_path")
            entry["active_theme_map_sha256"] = topn_meta.get("theme_map_sha256")
            entry["active_latest_log_path"] = topn_meta.get("latest_log_path")
            entry["active_git_rev"] = topn_meta.get("git_rev")

            source_counts = topn_meta.get("source_total_counts", {})
            if entry["active_theme_map_sha256"] != topn_meta.get("theme_map_sha256"):
                entry["errors"].append("active_theme_map_sha256_mismatch")
            if pool_summary.get("rows") != source_counts.get("all"):
                entry["errors"].append("pool_rows_mismatch_topn_source_counts")
            pool_modes = pool_summary.get("mode_distribution", {})
            for mode_key in ("enhanced", "tech_only"):
                if pool_modes.get(mode_key) != source_counts.get(mode_key):
                    entry["errors"].append("pool_mode_distribution_mismatch")
                    break

            summary = entry.get("theme_precision_summary") or {}
            enhanced_ratio = summary.get("enhanced", {}).get("unique_value_ratio")
            all_ratio = summary.get("all", {}).get("unique_value_ratio")
            if enhanced_ratio is not None and enhanced_ratio < min_ratio_enhanced:
                entry["errors"].append("theme_precision_enhanced_unique_ratio_below_min")
            if all_ratio is not None and all_ratio < min_ratio_all:
                entry["errors"].append("theme_precision_all_unique_ratio_below_min")

            if args.input_pool:
                modes = candidates_meta.get("modes") if isinstance(candidates_meta, dict) else None
                if not isinstance(modes, list) or not modes:
                    modes = ["enhanced", "tech_only"]
                expected_rows = pool_meta.get("rows", 0) * len(modes)
                pool_rows = pool_summary.get("rows")
                if isinstance(pool_rows, int) and expected_rows:
                    if pool_rows < expected_rows:
                        entry["warnings"].append("pool_rows_less_than_expected")
                    elif pool_rows > expected_rows:
                        entry["errors"].append("pool_rows_greater_than_expected")

            active_sha = entry.get("active_theme_map_sha256")
            if active_sha and last_active_sha and active_sha != last_active_sha:
                entry["warnings"].append("active_theme_map_changed")
            if not entry["errors"]:
                if active_sha:
                    last_active_sha = active_sha
        except Exception as exc:
            failures += 1
            entry["error"] = str(exc)
            entry["errors"].append(str(exc))
        results.append(entry)
        if entry["errors"]:
            regressions.append(
                {
                    "snapshot_id": snapshot_id,
                    "errors": list(entry["errors"]),
                    "active_theme_map_sha256": entry.get("active_theme_map_sha256"),
                    "active_latest_log_path": entry.get("active_latest_log_path"),
                }
            )

    failed_count = sum(1 for entry in results if entry.get("errors"))
    success_count = len(results) - failed_count - snapshots_skipped
    payload = {
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "input_pool": pool_meta,
        "snapshots_requested": snapshots_requested,
        "snapshots_succeeded": success_count,
        "snapshots_failed": failed_count,
        "snapshots_skipped": snapshots_skipped,
        "regressions": regressions,
        "snapshots": results,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if failures:
        return 1
    if args.gate and regressions:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
