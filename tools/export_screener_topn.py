import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_report(outputs_dir: Path) -> Path:
    candidates = sorted(outputs_dir.glob("report_*_top*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"no report json found in {outputs_dir}")
    return candidates[-1]


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
    logs = sorted((repo_root / "artifacts_logs").glob("verify_*.txt"), key=os.path.getmtime)
    if not logs:
        return None
    return str(logs[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--out", default="artifacts_metrics/screener_topn_latest.jsonl")
    parser.add_argument("--input", default=None)
    parser.add_argument("--latest-log", default=None)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    outputs_dir = repo_root / "outputs"
    if args.input:
        report_path = Path(args.input)
        if not report_path.is_absolute():
            report_path = repo_root / report_path
    else:
        report_path = _latest_report(outputs_dir)

    report = _load_json(report_path)
    results = report.get("results", [])
    if not isinstance(results, list) or not results:
        raise ValueError(f"report has no results: {report_path}")

    weight = _theme_weight(report)
    mode = _mode_from_weight(weight)
    max_items = min(args.top_n, len(results))

    metrics_path = repo_root / "artifacts_metrics" / "theme_map_sparsity_latest.json"
    map_info = _theme_map_from_metrics(metrics_path)
    if map_info:
        theme_map_path, theme_map_sha = map_info
    else:
        theme_map_path, theme_map_sha = _theme_map_fallback(repo_root, report)

    git_rev = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
    ).strip()
    latest_log = args.latest_log or _latest_log(repo_root)

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = repo_root / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    items: List[str] = []
    sort_key = "final_score"
    for idx, row in enumerate(results[:max_items], start=1):
        score_total, score_source = _score_from_row(row)
        if score_total is None:
            score_total = 0.0
            score_source = "fallback_zero"
        if score_source != "final_score":
            sort_key = score_source
        item = {
            "rank": idx,
            "item_id": str(row.get("ticker") or row.get("symbol") or row.get("name") or ""),
            "mode": mode,
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
        items.append(json.dumps(item, ensure_ascii=False))

    out_path.write_text("\n".join(items) + "\n", encoding="utf-8")
    print(f"[screener_topn] written={out_path} top_n={max_items} sort_key={sort_key}")


if __name__ == "__main__":
    main()
