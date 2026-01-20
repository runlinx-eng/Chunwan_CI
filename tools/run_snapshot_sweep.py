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
from typing import Any, Dict, List, Optional, Set, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
THEME_TOTAL_UNIQUE_COUNT_MIN_ENHANCED = 3
THEME_TOTAL_UNIQUE_COUNT_MIN_ALL = 4
THEME_HIT_SIGNATURE_UNIQUE_COUNT_MIN = 3
CONCEPT_HIT_SIGNATURE_UNIQUE_COUNT_MIN = 3
DEFAULT_ENHANCED_CONCEPT_SIG_UNIQUE_MIN = 6


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


def _is_external_path(path: Path, repo_root: Path) -> bool:
    try:
        path.resolve().relative_to(repo_root.resolve())
    except ValueError:
        return True
    return False


def _normalize_repo_path(path: Path, repo_root: Path) -> Tuple[str, str, bool]:
    if not path.is_absolute():
        path = repo_root / path
    abs_path = str(path.resolve())
    try:
        rel_path = str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return abs_path, abs_path, True
    return rel_path, abs_path, False


def _resolve_env_theme_map(repo_root: Path) -> Tuple[Optional[Path], bool]:
    env_value = os.environ.get("THEME_MAP")
    if not env_value:
        return None, False
    candidate = Path(env_value)
    if not candidate.is_absolute():
        candidate = repo_root / candidate
    return candidate, _is_external_path(candidate, repo_root)


def _resolve_active_theme_map(
    repo_root: Path,
    snapshot_id: str,
    candidates_meta: Optional[Dict[str, Any]],
    fallback_path: Optional[Path],
    env_theme_map: Optional[Path],
) -> Optional[Path]:
    if isinstance(candidates_meta, dict):
        path_value = candidates_meta.get("theme_map_path")
        if path_value:
            candidate = Path(path_value)
            if not candidate.is_absolute():
                candidate = repo_root / candidate
            if candidate.exists():
                return candidate
    if env_theme_map is not None:
        if env_theme_map.exists():
            return env_theme_map
    if fallback_path is not None:
        return fallback_path
    return _resolve_theme_map(repo_root, snapshot_id)[0]


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
        "min_enhanced_concept_hit_signature_unique_set_count",
    ):
        if key in payload:
            try:
                thresholds[key] = float(payload[key])
            except (TypeError, ValueError):
                continue
    return thresholds


def _iter_values(raw: Any) -> List[str]:
    if raw is None:
        return []
    values: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if item is None:
                continue
            value = str(item).strip()
            if value:
                values.append(value)
    else:
        value = str(raw).strip()
        if value:
            values.append(value)
    return values


def _matches_token(name: str, token: str) -> bool:
    if token.isascii():
        return token.lower() in name.lower()
    return token in name


def _detect_column(
    fieldnames: List[str],
    tokens: List[str],
    preferred_tokens: Optional[List[str]] = None,
    exclude_tokens: Optional[List[str]] = None,
) -> Optional[str]:
    best_name = None
    best_score = None
    for name in fieldnames:
        if not any(_matches_token(name, token) for token in tokens):
            continue
        score = 0
        for token in tokens:
            if _matches_token(name, token):
                score += 1
        for token in preferred_tokens or []:
            if _matches_token(name, token):
                score += 2
        for token in exclude_tokens or []:
            if _matches_token(name, token):
                score -= 2
        if best_score is None or score > best_score:
            best_score = score
            best_name = name
    return best_name


def _load_theme_map_concept_index(path: Optional[Path]) -> Dict[str, Set[str]]:
    if path is None or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        theme_col = _detect_column(
            fieldnames,
            tokens=["theme", "主题"],
            preferred_tokens=["name", "名称"],
            exclude_tokens=["id", "编号", "代码"],
        )
        concept_col = _detect_column(
            fieldnames,
            tokens=["concept", "industry", "概念", "行业"],
        )
        if not theme_col or not concept_col:
            return {}
        concept_map: Dict[str, Set[str]] = {}
        for row in reader:
            if not isinstance(row, dict):
                continue
            themes = _iter_values(row.get(theme_col))
            concepts = _iter_values(row.get(concept_col))
            if not themes or not concepts:
                continue
            for concept in concepts:
                for theme in themes:
                    concept_map.setdefault(concept, set()).add(theme)
        return concept_map


def _summarize_unique_ratio(values: List[float]) -> Dict[str, Any]:
    n_value = len(values)
    if n_value == 0:
        return {
            "N": 0,
            "unique_value_count": 0,
            "unique_value_ratio": None,
            "min": None,
            "max": None,
        }
    unique_values = set(values)
    unique_count = len(unique_values)
    return {
        "N": n_value,
        "unique_value_count": unique_count,
        "unique_value_ratio": float(unique_count) / float(n_value),
        "min": min(values),
        "max": max(values),
    }


def _default_candidate_theme_total_summary() -> Dict[str, Dict[str, Any]]:
    return {
        "all": {"N": 0, "unique_value_count": 0, "unique_value_ratio": None, "min": None, "max": None},
        "enhanced": {"N": 0, "unique_value_count": 0, "unique_value_ratio": None, "min": None, "max": None},
    }


def _default_candidate_signature_summary() -> Dict[str, Dict[str, Any]]:
    return {
        "all": {
            "theme_hit_signature_unique_set_count": 0,
            "concept_hit_signature_unique_set_count": 0,
        },
        "enhanced": {
            "theme_hit_signature_unique_set_count": 0,
            "concept_hit_signature_unique_set_count": 0,
        },
    }


def _iter_concepts(raw_hits: Any) -> List[str]:
    if not isinstance(raw_hits, list):
        return []
    concepts: List[str] = []
    for hit in raw_hits:
        raw = None
        if isinstance(hit, dict):
            raw = hit.get("concept")
            if raw is None or str(raw).strip() == "":
                raw = hit.get("industry")
        else:
            raw = hit
        concepts.extend(_iter_values(raw))
    return concepts


def _theme_hit_signature(row: Dict[str, Any], concept_to_themes: Dict[str, Set[str]]) -> List[str]:
    concepts = _iter_concepts(row.get("concept_hits"))
    hit_themes: Set[str] = set()
    for concept in concepts:
        hit_themes.update(concept_to_themes.get(concept, set()))
    return sorted(hit_themes)


def _concept_hit_signature(row: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
    concept_hits = row.get("concept_hits")
    if not isinstance(concept_hits, list):
        return None
    concepts = _iter_concepts(concept_hits)
    return tuple(sorted(set(concepts)))


def _candidate_summaries(
    path: Path,
    theme_map_path: Optional[Path],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    theme_summary = _default_candidate_theme_total_summary()
    signature_summary = _default_candidate_signature_summary()
    if not path.exists():
        return theme_summary, signature_summary
    concept_to_themes = _load_theme_map_concept_index(theme_map_path)
    enhanced_values: List[float] = []
    all_values: List[float] = []
    theme_sig_sets = {"enhanced": set(), "all": set()}
    concept_sig_sets = {"enhanced": set(), "all": set()}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            mode = row.get("mode")
            if mode not in ("enhanced", "tech_only"):
                continue
            if mode == "tech_only":
                theme_total = 0.0
                theme_signature = tuple()
            else:
                hit_signature = _theme_hit_signature(row, concept_to_themes)
                theme_total = float(len(hit_signature))
                theme_signature = tuple(hit_signature)
            if mode == "enhanced":
                enhanced_values.append(theme_total)
            all_values.append(theme_total)

            theme_sig_sets["all"].add(theme_signature)
            if mode == "enhanced":
                theme_sig_sets["enhanced"].add(theme_signature)
            concept_signature = _concept_hit_signature(row)
            if concept_signature is not None:
                concept_sig_sets["all"].add(concept_signature)
                if mode == "enhanced":
                    concept_sig_sets["enhanced"].add(concept_signature)
    theme_summary["enhanced"] = _summarize_unique_ratio(enhanced_values)
    theme_summary["all"] = _summarize_unique_ratio(all_values)
    signature_summary["enhanced"] = {
        "theme_hit_signature_unique_set_count": len(theme_sig_sets["enhanced"]),
        "concept_hit_signature_unique_set_count": len(concept_sig_sets["enhanced"]),
    }
    signature_summary["all"] = {
        "theme_hit_signature_unique_set_count": len(theme_sig_sets["all"]),
        "concept_hit_signature_unique_set_count": len(concept_sig_sets["all"]),
    }
    return theme_summary, signature_summary


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
        help="DEPRECATED: no longer affects gating (count-based gates).",
    )
    parser.add_argument(
        "--min-theme-unique-ratio-all",
        type=float,
        default=None,
        help="DEPRECATED: no longer affects gating (count-based gates).",
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

    deprecated_flags: List[Tuple[str, float]] = []
    if args.min_theme_unique_ratio_enhanced is not None:
        deprecated_flags.append(
            ("--min-theme-unique-ratio-enhanced", args.min_theme_unique_ratio_enhanced)
        )
    if args.min_theme_unique_ratio_all is not None:
        deprecated_flags.append(("--min-theme-unique-ratio-all", args.min_theme_unique_ratio_all))

    pool_path: Optional[Path] = None
    if args.input_pool:
        pool_path = Path(args.input_pool)
        if not pool_path.is_absolute():
            pool_path = REPO_ROOT / pool_path
        pool_meta = _load_input_pool(pool_path)
        pool_meta.setdefault("strategy", "fixed_pool")
    else:
        pool_meta = {"strategy": "snapshot_universe"}

    thresholds = _load_theme_precision_thresholds(REPO_ROOT)
    concept_sig_min_universe = DEFAULT_ENHANCED_CONCEPT_SIG_UNIQUE_MIN
    raw_concept_min = thresholds.get("min_enhanced_concept_hit_signature_unique_set_count")
    if raw_concept_min is not None:
        try:
            concept_sig_min_universe = int(raw_concept_min)
        except (TypeError, ValueError):
            concept_sig_min_universe = DEFAULT_ENHANCED_CONCEPT_SIG_UNIQUE_MIN

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
        entry: Dict[str, Any] = {
            "snapshot_id": snapshot_id,
            "warnings": [],
            "errors": [],
            "candidate_theme_total_summary": _default_candidate_theme_total_summary(),
            "candidate_signature_summary": _default_candidate_signature_summary(),
        }
        entry["pool_strategy"] = pool_meta.get("strategy")
        try:
            theme_map_path, fallback = _resolve_theme_map(REPO_ROOT, snapshot_id)
            if not theme_map_path.exists():
                raise FileNotFoundError(f"theme map not found: {theme_map_path}")
            theme_map_sha = _sha256_file(theme_map_path)
            default_theme_map_path, _default_abs_path, _default_external = _normalize_repo_path(
                theme_map_path, REPO_ROOT
            )
            entry["default_theme_map_path"] = default_theme_map_path
            entry["default_theme_map_sha256"] = theme_map_sha
            entry["theme_map_fallback"] = fallback

            env_theme_map_path, env_theme_map_external = _resolve_env_theme_map(REPO_ROOT)
            build_theme_map_path = env_theme_map_path or theme_map_path
            if env_theme_map_path is not None and env_theme_map_external:
                entry["warnings"].append("external_theme_map_path")
                entry["active_theme_map_path_external"] = True

            env = os.environ.copy()
            env["THEME_MAP"] = str(build_theme_map_path)

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
            summary_theme_map_path = _resolve_active_theme_map(
                REPO_ROOT, snapshot_id, candidates_meta, theme_map_path, env_theme_map_path
            )
            candidates_path = pool_output.get("path")
            candidates_summary_path: Optional[Path] = None
            if candidates_path:
                candidate_path = Path(candidates_path)
                if candidate_path.exists():
                    candidates_summary_path = candidate_path
            if candidates_summary_path:
                theme_summary, signature_summary = _candidate_summaries(
                    candidates_summary_path, summary_theme_map_path
                )
                entry["candidate_theme_total_summary"] = theme_summary
                entry["candidate_signature_summary"] = signature_summary
                enhanced_unique_count = (
                    theme_summary.get("enhanced", {}).get("unique_value_count")
                    if isinstance(theme_summary, dict)
                    else None
                )
                all_unique_count = (
                    theme_summary.get("all", {}).get("unique_value_count")
                    if isinstance(theme_summary, dict)
                    else None
                )
                enhanced_theme_sig_unique = (
                    signature_summary.get("enhanced", {}).get(
                        "theme_hit_signature_unique_set_count"
                    )
                    if isinstance(signature_summary, dict)
                    else None
                )
                enhanced_concept_sig_unique = (
                    signature_summary.get("enhanced", {}).get(
                        "concept_hit_signature_unique_set_count"
                    )
                    if isinstance(signature_summary, dict)
                    else None
                )
                entry["enhanced_unique_value_count"] = enhanced_unique_count
                entry["all_unique_value_count"] = all_unique_count
                entry["enhanced_theme_hit_sig_sets"] = enhanced_theme_sig_unique
                if "enhanced_concept_hit_sig_sets" not in entry:
                    entry["enhanced_concept_hit_sig_sets"] = enhanced_concept_sig_unique
            else:
                entry["warnings"].append("missing_candidates_path_for_summary")
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

            active_theme_map_raw = topn_meta.get("theme_map_path") or str(build_theme_map_path)
            active_path, active_abs_path, _active_external = _normalize_repo_path(
                Path(active_theme_map_raw), REPO_ROOT
            )
            entry["active_theme_map_path"] = active_path
            entry["active_theme_map_abs_path"] = active_abs_path
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

            if "missing_candidates_path_for_summary" not in entry["warnings"]:
                pool_strategy = entry.get("pool_strategy") or "snapshot_universe"
                enhanced_concept_sig_unique = entry.get("enhanced_concept_hit_sig_sets")
                if pool_strategy == "snapshot_universe":
                    if (
                        enhanced_concept_sig_unique is not None
                        and enhanced_concept_sig_unique < concept_sig_min_universe
                    ):
                        entry["errors"].append(
                            "enhanced_concept_hit_signature_unique_set_count_below_min"
                        )
                else:
                    enhanced_unique_count = entry.get("enhanced_unique_value_count")
                    all_unique_count = entry.get("all_unique_value_count")
                    if (
                        enhanced_unique_count is not None
                        and enhanced_unique_count < THEME_TOTAL_UNIQUE_COUNT_MIN_ENHANCED
                    ):
                        entry["errors"].append("enhanced_theme_total_unique_value_count_below_min")
                    if (
                        all_unique_count is not None
                        and all_unique_count < THEME_TOTAL_UNIQUE_COUNT_MIN_ALL
                    ):
                        entry["errors"].append("all_theme_total_unique_value_count_below_min")
                    enhanced_theme_sig_unique = entry.get("enhanced_theme_hit_sig_sets")
                    if (
                        enhanced_theme_sig_unique is not None
                        and enhanced_theme_sig_unique < THEME_HIT_SIGNATURE_UNIQUE_COUNT_MIN
                    ):
                        entry["errors"].append(
                            "enhanced_theme_hit_signature_unique_set_count_below_min"
                        )
                    if (
                        enhanced_concept_sig_unique is not None
                        and enhanced_concept_sig_unique < CONCEPT_HIT_SIGNATURE_UNIQUE_COUNT_MIN
                    ):
                        entry["errors"].append(
                            "enhanced_concept_hit_signature_unique_set_count_below_min"
                        )

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

    print("[snapshot_sweep] BEGIN")
    print("parser_version=2")
    print(f"created_at={payload.get('created_at')}")
    print("snapshots_requested=" + ",".join(snapshots_requested))
    print(f"snapshots_succeeded={success_count}")
    print(f"snapshots_failed={failed_count}")
    print(f"snapshots_skipped={snapshots_skipped}")
    print(f"regressions_count={len(regressions)}")
    for name, value in deprecated_flags:
        print(
            "warning=deprecated_flag_no_effect name={name} value={value}".format(
                name=name, value=value
            )
        )
    summary_map = {entry.get("snapshot_id"): entry for entry in results}
    for snapshot_id in snapshots_requested:
        entry = summary_map.get(snapshot_id, {})
        warnings = entry.get("warnings") if isinstance(entry.get("warnings"), list) else []
        errors = entry.get("errors") if isinstance(entry.get("errors"), list) else []
        status = "ok"
        if errors:
            status = "failed"
        elif "empty_pool_for_snapshot" in warnings:
            status = "skipped"
        if entry.get("active_theme_map_path_external"):
            print(
                "warning=external_theme_map_path snapshot_id={snapshot_id} path={path}".format(
                    snapshot_id=snapshot_id,
                    path=entry.get("active_theme_map_path"),
                )
            )
        pool_summary = entry.get("pool_coverage_summary", {}) if isinstance(entry.get("pool_coverage_summary"), dict) else {}
        enhanced_unique_count = entry.get("enhanced_unique_value_count")
        all_unique_count = entry.get("all_unique_value_count")
        enhanced_theme_sig_unique = entry.get("enhanced_theme_hit_sig_sets")
        enhanced_concept_sig_unique = entry.get("enhanced_concept_hit_sig_sets")
        pool_rows = pool_summary.get("rows")
        line = (
            f"snapshot_id={snapshot_id} "
            f"status={status} "
            f"active_git_rev={entry.get('active_git_rev') or 'null'} "
            f"active_theme_map_sha256={entry.get('active_theme_map_sha256') or 'null'} "
            f"pool_strategy={entry.get('pool_strategy') or 'null'} "
            f"enhanced_unique_value_count={enhanced_unique_count if enhanced_unique_count is not None else 'null'} "
            f"all_unique_value_count={all_unique_count if all_unique_count is not None else 'null'} "
            f"enhanced_theme_hit_sig_sets={enhanced_theme_sig_unique if enhanced_theme_sig_unique is not None else 'null'} "
            f"enhanced_concept_hit_sig_sets={enhanced_concept_sig_unique if enhanced_concept_sig_unique is not None else 'null'} "
            f"pool_rows={pool_rows if pool_rows is not None else 'null'} "
            f"warnings_count={len(warnings)} "
            f"errors_count={len(errors)}"
        )
        print(line)
    if regressions:
        for regression in regressions:
            snapshot_id = regression.get("snapshot_id")
            errors = regression.get("errors", [])
            if not isinstance(errors, list):
                errors = [str(errors)]
            print(
                "regression_snapshot_id={snapshot_id} errors={errors}".format(
                    snapshot_id=snapshot_id,
                    errors=",".join(errors),
                )
            )
    print("[snapshot_sweep] END")

    if failures:
        return 1
    if args.gate and regressions:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
