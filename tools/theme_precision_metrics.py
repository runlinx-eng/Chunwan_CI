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


def _extract_theme_hit_signature(row: Dict[str, Any]) -> str:
    theme_hits = row.get("theme_hits")
    if not isinstance(theme_hits, list):
        return ""
    themes: List[str] = []
    for hit in theme_hits:
        if not isinstance(hit, dict):
            continue
        raw = hit.get("theme")
        if raw is None:
            raw = hit.get("theme_id")
        if raw is None:
            raw = hit.get("signal_id")
        if isinstance(raw, list):
            for item in raw:
                item_str = str(item).strip()
                if item_str:
                    themes.append(item_str)
        elif raw is not None:
            item_str = str(raw).strip()
            if item_str:
                themes.append(item_str)
    if not themes:
        return ""
    return ",".join(sorted(set(themes)))


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
        return {
            "count": 0,
            "sum": 0.0,
            "avg": 0.0,
            "min": None,
            "max": None,
            "positive": 0,
            "per_result_values": [],
        }
    total = sum(values)
    return {
        "count": len(values),
        "sum": total,
        "avg": total / len(values),
        "min": min(values),
        "max": max(values),
        "positive": sum(1 for v in values if v > 0),
        "per_result_values": values,
    }


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def _summarize_distribution(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {
            "N": 0,
            "min": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
            "unique_value_count": 0,
        }
    ordered = sorted(values)
    return {
        "N": len(ordered),
        "min": ordered[0],
        "p50": _percentile(ordered, 0.5),
        "p95": _percentile(ordered, 0.95),
        "p99": _percentile(ordered, 0.99),
        "max": ordered[-1],
        "unique_value_count": len(set(ordered)),
    }


def _build_metrics(report: Dict[str, Any], path: Path) -> Dict[str, Any]:
    results = report.get("results", [])
    weight = _theme_weight(report)
    category = _theme_category(weight)
    theme_totals: List[float] = []
    themes_used: List[str] = []
    concept_hits: List[str] = []
    themes_per_row: List[int] = []
    concepts_per_row: List[int] = []
    themes_signature_per_row: List[str] = []
    concepts_signature_per_row: List[str] = []
    theme_hit_signature_per_row: List[str] = []
    included_rows = 0

    for row in results if isinstance(results, list) else []:
        theme_total = _score_theme_total(row)
        if theme_total is None and category == "tech_only":
            theme_total = 0.0
        if theme_total is None:
            continue
        theme_totals.append(theme_total)
        theme_hit_signature_per_row.append(_extract_theme_hit_signature(row))
        row_themes = _extract_themes_used(row)
        themes_per_row.append(len(row_themes))
        themes_signature_per_row.append(",".join(sorted(set(row_themes))) if row_themes else "")
        for theme in row_themes:
            if theme not in themes_used:
                themes_used.append(theme)
        row_concepts = _extract_concept_hits(row)
        concepts_per_row.append(len(row_concepts))
        concepts_signature_per_row.append(",".join(sorted(set(row_concepts))) if row_concepts else "")
        for concept in row_concepts:
            if concept not in concept_hits:
                concept_hits.append(concept)
        included_rows += 1
    metrics = {
        "path": str(path),
        "as_of": report.get("as_of"),
        "theme_weight": weight,
        "category": category,
        "results_count": len(results) if isinstance(results, list) else 0,
        "results_included": included_rows,
        "theme_total": _summarize_theme_totals(theme_totals),
        "themes_used": {
            "unique_count": len(themes_used),
            "unique": themes_used,
            "per_result_counts": themes_per_row,
            "per_result_signatures": themes_signature_per_row,
        },
        "concept_hits": {
            "unique_count": len(concept_hits),
            "unique": concept_hits,
            "per_result_counts": concepts_per_row,
            "per_result_signatures": concepts_signature_per_row,
        },
    }
    metrics["theme_total"]["per_result_signatures"] = theme_hit_signature_per_row
    return metrics


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


def _aggregate_result_level(reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    buckets: Dict[str, Dict[str, List[float]]] = {
        "all": {"theme_total": [], "themes_used": [], "concept_hits": []},
        "enhanced": {"theme_total": [], "themes_used": [], "concept_hits": []},
        "tech_only": {"theme_total": [], "themes_used": [], "concept_hits": []},
    }
    signatures: Dict[str, Dict[str, List[str]]] = {
        "all": {"themes_used": [], "concept_hits": [], "theme_total": []},
        "enhanced": {"themes_used": [], "concept_hits": [], "theme_total": []},
        "tech_only": {"themes_used": [], "concept_hits": [], "theme_total": []},
    }

    for report in reports:
        category = report.get("category")
        theme_totals = report.get("theme_total", {}).get("per_result_values", [])
        theme_total_signatures = report.get("theme_total", {}).get("per_result_signatures", [])
        themes_counts = report.get("themes_used", {}).get("per_result_counts", [])
        concept_counts = report.get("concept_hits", {}).get("per_result_counts", [])
        themes_signatures = report.get("themes_used", {}).get("per_result_signatures", [])
        concept_signatures = report.get("concept_hits", {}).get("per_result_signatures", [])

        if isinstance(theme_totals, list):
            buckets["all"]["theme_total"].extend([float(v) for v in theme_totals if v is not None])
            if category in ("enhanced", "tech_only"):
                buckets[category]["theme_total"].extend([float(v) for v in theme_totals if v is not None])
        if isinstance(theme_total_signatures, list):
            signatures["all"]["theme_total"].extend(
                [str(v) for v in theme_total_signatures if v is not None]
            )
            if category in ("enhanced", "tech_only"):
                signatures[category]["theme_total"].extend(
                    [str(v) for v in theme_total_signatures if v is not None]
                )
        if isinstance(themes_counts, list):
            buckets["all"]["themes_used"].extend([float(v) for v in themes_counts if v is not None])
            if category in ("enhanced", "tech_only"):
                buckets[category]["themes_used"].extend([float(v) for v in themes_counts if v is not None])
        if isinstance(concept_counts, list):
            buckets["all"]["concept_hits"].extend([float(v) for v in concept_counts if v is not None])
            if category in ("enhanced", "tech_only"):
                buckets[category]["concept_hits"].extend([float(v) for v in concept_counts if v is not None])
        if isinstance(themes_signatures, list):
            signatures["all"]["themes_used"].extend([str(v) for v in themes_signatures if v is not None])
            if category in ("enhanced", "tech_only"):
                signatures[category]["themes_used"].extend([str(v) for v in themes_signatures if v is not None])
        if isinstance(concept_signatures, list):
            signatures["all"]["concept_hits"].extend([str(v) for v in concept_signatures if v is not None])
            if category in ("enhanced", "tech_only"):
                signatures[category]["concept_hits"].extend([str(v) for v in concept_signatures if v is not None])

    summary: Dict[str, Any] = {}
    for bucket, values in buckets.items():
        theme_total_unique_sets = {sig for sig in signatures[bucket]["theme_total"] if sig}
        themes_unique_sets = {sig for sig in signatures[bucket]["themes_used"] if sig}
        concept_unique_sets = {sig for sig in signatures[bucket]["concept_hits"] if sig}
        theme_total_summary = _summarize_distribution(values["theme_total"])
        theme_total_summary["unique_set_count"] = (
            len(theme_total_unique_sets) if theme_total_unique_sets else None
        )
        theme_total_unique = theme_total_summary.get("unique_value_count")
        theme_total_n = theme_total_summary.get("N")
        if theme_total_n:
            theme_total_summary["unique_value_ratio"] = (
                float(theme_total_unique) / float(theme_total_n)
                if theme_total_unique is not None
                else None
            )
        else:
            theme_total_summary["unique_value_ratio"] = None
        themes_used_summary = _summarize_distribution(values["themes_used"])
        themes_used_summary["unique_set_count"] = (
            len(themes_unique_sets) if themes_unique_sets else None
        )
        concept_hits_summary = _summarize_distribution(values["concept_hits"])
        concept_hits_summary["unique_set_count"] = (
            len(concept_unique_sets) if concept_unique_sets else None
        )
        summary[bucket] = {
            "theme_total": theme_total_summary,
            "themes_used": themes_used_summary,
            "concept_hits": concept_hits_summary,
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
        "result_level": _aggregate_result_level(reports_metrics),
    }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
