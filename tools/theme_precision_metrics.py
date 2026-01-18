import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _discover_reports(outputs_dir: Path) -> List[Path]:
    candidates = sorted(outputs_dir.glob("report_*_top*.json"))
    return [path for path in candidates if path.is_file()]


def _load_report(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _theme_weight(report: Dict[str, Any]) -> Optional[float]:
    raw = report.get("provenance", {}).get("args", {}).get("theme_weight")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _theme_category(weight: Optional[float]) -> str:
    if weight is None:
        return "unknown"
    if abs(weight) < 1e-12:
        return "tech_only"
    return "enhanced"


def _extract_themes_used(row: Dict[str, Any]) -> List[str]:
    reason_struct = row.get("reason_struct")
    if isinstance(reason_struct, dict):
        themes_used = reason_struct.get("themes_used")
        if isinstance(themes_used, list):
            return [str(x) for x in themes_used if str(x).strip()]
    themes = []
    for hit in row.get("theme_hits", []) if isinstance(row.get("theme_hits"), list) else []:
        theme = hit.get("theme")
        if theme and theme not in themes:
            themes.append(theme)
    return themes


def _extract_concept_hits(row: Dict[str, Any]) -> List[str]:
    reason_struct = row.get("reason_struct")
    if isinstance(reason_struct, dict):
        concept_hits = reason_struct.get("concept_hits")
        if isinstance(concept_hits, list):
            concepts = []
            for item in concept_hits:
                if not isinstance(item, dict):
                    continue
                concept = str(item.get("concept", "")).strip()
                if concept:
                    concepts.append(concept)
            return concepts
    concept = str(row.get("concept", "")).strip()
    if concept:
        return [concept]
    industry = str(row.get("industry", "")).strip()
    if industry:
        return [industry]
    return []


def _score_theme_total(row: Dict[str, Any]) -> Optional[float]:
    breakdown = row.get("score_breakdown")
    if isinstance(breakdown, dict) and "score_theme_total" in breakdown:
        try:
            return float(breakdown["score_theme_total"])
        except (TypeError, ValueError):
            return None
    return None


def _summarize_theme_totals(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"count": 0, "sum": 0.0, "avg": 0.0, "min": None, "max": None, "positive": 0}
    total = sum(values)
    return {
        "count": len(values),
        "sum": total,
        "avg": total / len(values),
        "min": min(values),
        "max": max(values),
        "positive": sum(1 for v in values if v > 0),
    }


def _build_metrics(report: Dict[str, Any], path: Path) -> Dict[str, Any]:
    results = report.get("results", [])
    theme_totals: List[float] = []
    themes_used: List[str] = []
    concept_hits: List[str] = []
    themes_per_row: List[int] = []
    concepts_per_row: List[int] = []

    for row in results if isinstance(results, list) else []:
        theme_total = _score_theme_total(row)
        if theme_total is not None:
            theme_totals.append(theme_total)
        row_themes = _extract_themes_used(row)
        themes_per_row.append(len(row_themes))
        for theme in row_themes:
            if theme not in themes_used:
                themes_used.append(theme)
        row_concepts = _extract_concept_hits(row)
        concepts_per_row.append(len(row_concepts))
        for concept in row_concepts:
            if concept not in concept_hits:
                concept_hits.append(concept)

    weight = _theme_weight(report)
    category = _theme_category(weight)
    return {
        "path": str(path),
        "as_of": report.get("as_of"),
        "theme_weight": weight,
        "category": category,
        "results_count": len(results) if isinstance(results, list) else 0,
        "theme_total": _summarize_theme_totals(theme_totals),
        "themes_used": {
            "unique_count": len(themes_used),
            "unique": themes_used,
            "per_result_counts": themes_per_row,
        },
        "concept_hits": {
            "unique_count": len(concept_hits),
            "unique": concept_hits,
            "per_result_counts": concepts_per_row,
        },
    }


def _aggregate_by_category(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for report in reports:
        grouped.setdefault(report["category"], []).append(report)
    summary: Dict[str, Any] = {}
    for category, items in grouped.items():
        theme_totals = []
        for item in items:
            report_totals = item["theme_total"]
            if isinstance(report_totals, dict) and report_totals.get("count", 0) > 0:
                theme_totals.append(report_totals["avg"])
        summary[category] = {
            "reports": len(items),
            "avg_theme_total_mean": sum(theme_totals) / len(theme_totals) if theme_totals else 0.0,
            "avg_theme_total_min": min(theme_totals) if theme_totals else None,
            "avg_theme_total_max": max(theme_totals) if theme_totals else None,
        }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute theme precision metrics from reports")
    parser.add_argument("--out", default="", help="Output JSON path")
    parser.add_argument("--outputs-dir", default="outputs", help="Outputs directory")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    outputs_dir = (repo_root / args.outputs_dir).resolve()
    report_paths = _discover_reports(outputs_dir)
    if not report_paths:
        raise SystemExit(f"No reports found under {outputs_dir}")

    reports_metrics = []
    for path in report_paths:
        report = _load_report(path)
        reports_metrics.append(_build_metrics(report, path))

    payload = {
        "outputs_dir": str(outputs_dir),
        "report_count": len(reports_metrics),
        "reports": reports_metrics,
        "by_category": _aggregate_by_category(reports_metrics),
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
