import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _discover_reports(outputs_dir: Path) -> List[Path]:
    return sorted(outputs_dir.glob("report_*_top*.json"), key=lambda p: p.stat().st_mtime)


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


def _snapshot_id(report: Dict[str, Any], report_path: Path) -> str:
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


def _theme_map_fallback(repo_root: Path, report: Dict[str, Any]) -> Tuple[str, str]:
    args = report.get("provenance", {}).get("args", {})
    theme_map = args.get("theme_map") or os.environ.get("THEME_MAP")
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


def _latest_by_mode(
    entries: List[Tuple[Path, Dict[str, Any], Optional[float]]],
    mode: str,
    manual_all: Optional[Tuple[Path, Dict[str, Any], Optional[float]]],
) -> Optional[Tuple[Path, Dict[str, Any], Optional[float]]]:
    if not entries and manual_all is None:
        return None
    if mode == "all":
        return manual_all or entries[-1]

    selected = []
    for path, report, weight in entries:
        if mode == "enhanced" and weight is not None and abs(weight) >= 1e-12:
            selected.append((path, report, weight))
        if mode == "tech_only" and weight is not None and abs(weight) < 1e-12:
            selected.append((path, report, weight))
    return selected[-1] if selected else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--modes", default="all,enhanced,tech_only")
    parser.add_argument("--out-dir", default="artifacts_metrics")
    parser.add_argument("--input", default=None)
    parser.add_argument("--latest-log", default=None)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    outputs_dir = repo_root / "outputs"
    reports = _discover_reports(outputs_dir)
    if not reports and not args.input:
        raise FileNotFoundError(f"no report json found in {outputs_dir}")

    entries: List[Tuple[Path, Dict[str, Any], Optional[float]]] = []
    for path in reports:
        report = _load_json(path)
        entries.append((path, report, _theme_weight(report)))

    manual_all: Optional[Tuple[Path, Dict[str, Any], Optional[float]]] = None
    if args.input:
        report_path = Path(args.input)
        if not report_path.is_absolute():
            report_path = repo_root / report_path
        report = _load_json(report_path)
        manual_all = (report_path, report, _theme_weight(report))

    mode_list = _parse_modes(args.modes)
    for mode in mode_list:
        if mode not in {"all", "enhanced", "tech_only"}:
            raise ValueError(f"unsupported mode: {mode}")

    metrics_path = repo_root / "artifacts_metrics" / "theme_map_sparsity_latest.json"
    map_info = _theme_map_from_metrics(metrics_path)
    if map_info:
        theme_map_path, theme_map_sha = map_info
    else:
        fallback_report = manual_all[1] if manual_all else (entries[-1][1] if entries else {})
        theme_map_path, theme_map_sha = _theme_map_fallback(repo_root, fallback_report)

    git_rev = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
    ).strip()
    latest_log = args.latest_log or _latest_log(repo_root)

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = repo_root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    counts: Dict[str, int] = {}
    sort_key = "final_score"
    snapshot_id = ""
    modes_present: List[str] = []

    for mode in mode_list:
        selection = _latest_by_mode(entries, mode, manual_all)
        if selection is None:
            if mode == "all":
                continue
            raise FileNotFoundError(f"no report found for mode '{mode}'")
        report_path, report, weight = selection
        results = report.get("results", [])
        if not isinstance(results, list) or not results:
            raise ValueError(f"report has no results: {report_path}")

        max_items = min(args.top_n, len(results))
        if not snapshot_id:
            snapshot_id = _snapshot_id(report, report_path)

        mode_name = "all" if mode == "all" else _mode_from_weight(weight)
        modes_present.append(mode_name)
        items: List[str] = []
        for idx, row in enumerate(results[:max_items], start=1):
            score_total, score_source = _score_from_row(row)
            if score_total is None:
                score_total = 0.0
                score_source = "fallback_zero"
            if score_source != "final_score":
                sort_key = score_source
            item = {
                "schema_version": 1,
                "rank": idx,
                "item_id": str(row.get("ticker") or row.get("symbol") or row.get("name") or ""),
                "mode": mode_name,
                "score_total": score_total,
                "score_total_source": score_source,
                "score_breakdown": _score_breakdown(row),
                "theme_hits": _theme_hits(row),
                "concept_hits": _concept_hits(row),
                "snapshot_id": _snapshot_id(report, report_path),
                "theme_map_path": theme_map_path,
                "theme_map_sha256": theme_map_sha,
                "git_rev": git_rev,
                "latest_log_path": latest_log,
            }
            if mode_name == "tech_only":
                item["theme_hits_scoring_applied"] = False
            items.append(json.dumps(item, ensure_ascii=False))

        out_path = out_dir / f"screener_topn_latest_{mode_name}.jsonl"
        out_path.write_text("\n".join(items) + "\n", encoding="utf-8")
        counts[mode_name] = max_items

    meta = {
        "git_rev": git_rev,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "snapshot_id": snapshot_id,
        "theme_map_path": theme_map_path,
        "theme_map_sha256": theme_map_sha,
        "latest_log_path": latest_log,
        "top_n": args.top_n,
        "sort_key": sort_key,
        "modes_present": modes_present,
        "counts": counts,
    }
    meta_path = out_dir / "screener_topn_latest_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[screener_topn] written_meta={meta} modes={modes} top_n={top_n} sort_key={sort_key}".format(
            meta=meta_path,
            modes=",".join(modes_present),
            top_n=args.top_n,
            sort_key=sort_key,
        )
    )


if __name__ == "__main__":
    main()
