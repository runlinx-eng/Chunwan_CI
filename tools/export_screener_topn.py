import argparse
import csv
import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ALL_MODES = ["all", "enhanced", "tech_only"]
SUPPORTED_SOURCE_SUFFIXES = {".jsonl", ".json", ".csv"}
EXCLUDED_SOURCE_HINTS = {
    "screener_topn_latest",
    "theme_precision",
    "theme_map_",
    "theme_map_prune",
    "theme_map_sparsity",
    "regression_matrix",
    "theme_to_industry_pruned",
}


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            entries.append(row)
    return entries


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if isinstance(row, dict):
                entries.append(row)
    return entries


def _theme_weight(report: Dict[str, Any]) -> Optional[float]:
    raw = report.get("provenance", {}).get("args", {}).get("theme_weight")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _mode_from_weight(weight: Optional[float]) -> str:
    if weight is None:
        return "all"
    if abs(weight) < 1e-12:
        return "tech_only"
    return "enhanced"


def _score_from_row(row: Dict[str, Any]) -> Tuple[Optional[float], str]:
    if "final_score" in row:
        try:
            return float(row["final_score"]), "final_score"
        except (TypeError, ValueError):
            pass
    if "score_total" in row:
        try:
            return float(row["score_total"]), str(row.get("score_total_source") or "score_total")
        except (TypeError, ValueError):
            pass
    breakdown = row.get("score_breakdown")
    if isinstance(breakdown, dict):
        if "score_total" in breakdown:
            try:
                return float(breakdown["score_total"]), "score_breakdown.score_total"
            except (TypeError, ValueError):
                pass
        if "score_theme_total" in breakdown:
            try:
                return float(breakdown["score_theme_total"]), "score_breakdown.score_theme_total"
            except (TypeError, ValueError):
                pass
    return None, "unknown"


def _score_breakdown(row: Dict[str, Any]) -> Dict[str, Any]:
    breakdown = row.get("score_breakdown")
    return breakdown if isinstance(breakdown, dict) else {}


def _theme_hits(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    hits = row.get("theme_hits")
    if isinstance(hits, list):
        return [hit for hit in hits if isinstance(hit, dict)]
    return []


def _concept_hits(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    reason_struct = row.get("reason_struct")
    if isinstance(reason_struct, dict):
        hits = reason_struct.get("concept_hits")
        if isinstance(hits, list):
            normalized: List[Dict[str, Any]] = []
            for item in hits:
                if isinstance(item, dict):
                    concept = str(item.get("concept", "")).strip()
                    if concept:
                        normalized.append(item)
                else:
                    concept = str(item).strip()
                    if concept:
                        normalized.append({"concept": concept})
            return normalized
    return []


def _snapshot_id(report: Optional[Dict[str, Any]], report_path: Path, row: Dict[str, Any]) -> str:
    if "snapshot_id" in row:
        return str(row.get("snapshot_id"))
    if report:
        args = report.get("provenance", {}).get("args", {})
        snapshot = args.get("snapshot_as_of") or args.get("snapshot_asof") or args.get("snapshot")
        if snapshot:
            return str(snapshot)
    return f"source:{report_path}"


def _theme_map_from_metrics(metrics_path: Path) -> Optional[Tuple[str, str]]:
    if not metrics_path.exists():
        return None
    metrics = _load_json(metrics_path)
    path = metrics.get("theme_map_path")
    sha = metrics.get("theme_map_sha256")
    if path and sha:
        return str(path), str(sha)
    return None


def _theme_map_fallback(repo_root: Path, report: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    theme_map = None
    if report:
        args = report.get("provenance", {}).get("args", {})
        theme_map = args.get("theme_map")
    theme_map = theme_map or os.environ.get("THEME_MAP")
    if theme_map:
        path = Path(theme_map)
        theme_map_path = str(path if path.is_absolute() else repo_root / path)
    else:
        theme_map_path = str(repo_root / "theme_to_industry_em_2026-01-20.csv")
    sha = hashlib.sha256(Path(theme_map_path).read_bytes()).hexdigest()
    return theme_map_path, sha


def _latest_log(repo_root: Path) -> Optional[str]:
    logs_dir = repo_root / "artifacts_logs"
    if not logs_dir.exists():
        return None
    logs = sorted(logs_dir.glob("verify_*.txt"), key=os.path.getmtime)
    if not logs:
        return None
    return str(logs[-1])


def _parse_modes(value: str) -> List[str]:
    modes = [item.strip() for item in value.split(",") if item.strip()]
    seen: List[str] = []
    for mode in modes:
        if mode not in seen:
            seen.append(mode)
    return seen


def _entry_has_final_score(entry: Dict[str, Any]) -> bool:
    if "final_score" in entry:
        return True
    return entry.get("score_total_source") == "final_score"


def _load_entries(path: Path) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if path.suffix == ".jsonl":
        return _load_jsonl(path), None
    if path.suffix == ".json":
        data = _load_json(path)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)], None
        if isinstance(data, dict):
            results = data.get("results")
            if isinstance(results, list):
                return [item for item in results if isinstance(item, dict)], data
        return [], None
    if path.suffix == ".csv":
        return _load_csv(path), None
    return [], None


def _discover_source_in_metrics(metrics_dir: Path) -> Tuple[Path, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    best_path: Optional[Path] = None
    best_entries: List[Dict[str, Any]] = []
    best_meta: Optional[Dict[str, Any]] = None
    best_count = -1
    best_suffix = ""

    for path in sorted(metrics_dir.rglob("*")):
        if path.suffix not in {".jsonl", ".json", ".csv"}:
            continue
        path_str = str(path)
        if any(hint in path_str for hint in EXCLUDED_SOURCE_HINTS):
            continue
        entries, meta = _load_entries(path)
        if not entries:
            continue
        filtered = [row for row in entries if isinstance(row, dict) and _entry_has_final_score(row)]
        if not filtered:
            continue
        count = len(filtered)
        suffix = path.suffix
        if count > best_count:
            best_path, best_entries, best_meta = path, filtered, meta
            best_count, best_suffix = count, suffix
        elif count == best_count:
            if suffix == ".jsonl" and best_suffix != ".jsonl":
                best_path, best_entries, best_meta = path, filtered, meta
                best_suffix = suffix
            elif suffix == best_suffix and best_path and str(path) < str(best_path):
                best_path, best_entries, best_meta = path, filtered, meta

    if best_path is None:
        raise FileNotFoundError(f"no suitable source found in {metrics_dir}")
    return best_path, best_entries, best_meta


def _entry_mode(entry: Dict[str, Any], fallback_mode: Optional[str]) -> str:
    mode = entry.get("mode")
    if isinstance(mode, str) and mode in ALL_MODES:
        return mode
    if fallback_mode:
        return fallback_mode
    return "all"


def _extract_sort_value(row: Dict[str, Any], sort_key: str) -> Optional[float]:
    value: Any = row
    for part in sort_key.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = None
            break
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--sort-key", default="final_score")
    parser.add_argument("--modes", default="all,enhanced,tech_only")
    parser.add_argument("--out-dir", default="artifacts_metrics")
    parser.add_argument("--source-path", default=None)
    parser.add_argument("--latest-log", default=None)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    metrics_dir = repo_root / "artifacts_metrics"
    if args.source_path:
        if "screener_topn_latest" in str(args.source_path):
            raise ValueError("source_path cannot be screener_topn_latest* artifacts")
        source_path = Path(args.source_path)
        if source_path.suffix not in SUPPORTED_SOURCE_SUFFIXES:
            raise ValueError(
                "unsupported source_path format; use .jsonl/.json/.csv or convert to jsonl first"
            )
        if not source_path.is_absolute():
            source_path = repo_root / source_path
        entries, meta = _load_entries(source_path)
        entries = [row for row in entries if isinstance(row, dict) and _entry_has_final_score(row)]
        if not entries:
            raise ValueError(
                f"source has no qualifying entries: {source_path}; "
                "ensure rows contain final_score or convert to jsonl"
            )
    else:
        candidates_path = metrics_dir / "screener_candidates_latest.jsonl"
        if candidates_path.exists():
            source_path = candidates_path
            entries, meta = _load_entries(candidates_path)
            entries = [row for row in entries if isinstance(row, dict) and _entry_has_final_score(row)]
            if not entries:
                raise ValueError(f"source has no qualifying entries: {source_path}")
        else:
            source_path, entries, meta = _discover_source_in_metrics(metrics_dir)

    mode_list = _parse_modes(args.modes)
    for mode in mode_list:
        if mode not in ALL_MODES:
            raise ValueError(f"unsupported mode: {mode}")

    fallback_mode = _mode_from_weight(_theme_weight(meta)) if meta else None

    metrics_path = repo_root / "artifacts_metrics" / "theme_map_sparsity_latest.json"
    map_info = _theme_map_from_metrics(metrics_path)
    if map_info:
        theme_map_path, theme_map_sha = map_info
    else:
        theme_map_path, theme_map_sha = _theme_map_fallback(repo_root, meta)

    git_rev = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
    ).strip()
    latest_log = args.latest_log or _latest_log(repo_root)

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = repo_root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    source_total_counts: Dict[str, int] = {mode: 0 for mode in ALL_MODES}
    for row in entries:
        source_total_counts["all"] += 1
        row_mode = _entry_mode(row, fallback_mode)
        if row_mode in {"enhanced", "tech_only"}:
            source_total_counts[row_mode] += 1

    exported_counts: Dict[str, int] = {mode: 0 for mode in ALL_MODES}
    sort_key = args.sort_key
    snapshot_id = ""

    for mode in mode_list:
        if mode == "all":
            bucket = entries
        else:
            bucket = [row for row in entries if _entry_mode(row, fallback_mode) == mode]
        if sort_key != "final_score":
            bucket = sorted(
                bucket,
                key=lambda row: (
                    _extract_sort_value(row, sort_key) is None,
                    -float(_extract_sort_value(row, sort_key) or 0.0),
                    str(row.get("ticker") or row.get("symbol") or row.get("name") or ""),
                ),
            )
        max_items = min(args.top_n, len(bucket))
        exported_counts[mode] = max_items

        items: List[str] = []
        for idx, row in enumerate(bucket[:max_items], start=1):
            score_total, score_source = _score_from_row(row)
            if score_total is None:
                score_total = 0.0
                score_source = "fallback_zero"
            item = {
                "schema_version": 1,
                "rank": idx,
                "item_id": str(row.get("ticker") or row.get("symbol") or row.get("name") or ""),
                "mode": mode,
                "score_total": score_total,
                "score_total_source": score_source,
                "score_breakdown": _score_breakdown(row),
                "theme_hits": _theme_hits(row),
                "concept_hits": _concept_hits(row),
                "snapshot_id": _snapshot_id(meta, source_path, row),
                "theme_map_path": theme_map_path,
                "theme_map_sha256": theme_map_sha,
                "git_rev": git_rev,
                "latest_log_path": latest_log,
            }
            if mode == "tech_only":
                item["theme_hits_scoring_applied"] = False
            items.append(json.dumps(item, ensure_ascii=False))

            if not snapshot_id:
                snapshot_id = item["snapshot_id"]

        out_path = out_dir / f"screener_topn_latest_{mode}.jsonl"
        content = "\n".join(items)
        if items:
            content += "\n"
        out_path.write_text(content, encoding="utf-8")

    meta_payload = {
        "git_rev": git_rev,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "snapshot_id": snapshot_id,
        "theme_map_path": theme_map_path,
        "theme_map_sha256": theme_map_sha,
        "latest_log_path": latest_log,
        "top_n": args.top_n,
        "sort_key": sort_key,
        "modes_present": mode_list,
        "source_path": str(source_path.resolve()),
        "source_total_counts": source_total_counts,
        "exported_counts": exported_counts,
        "counts": exported_counts,
    }
    meta_path = out_dir / "screener_topn_latest_meta.json"
    meta_path.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[screener_topn] written_meta={meta} modes={modes} top_n={top_n} sort_key={sort_key}".format(
            meta=meta_path,
            modes=",".join(mode_list),
            top_n=args.top_n,
            sort_key=sort_key,
        )
    )


if __name__ == "__main__":
    main()
