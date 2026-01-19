import json
from pathlib import Path
from typing import Any, Dict, List


def _candidate_entry(row: Dict[str, Any], mode: str, snapshot_id: str) -> Dict[str, Any]:
    reason_struct = row.get("reason_struct", {}) if isinstance(row.get("reason_struct"), dict) else {}
    return {
        "item_id": row.get("ticker", ""),
        "ticker": row.get("ticker", ""),
        "mode": mode,
        "final_score": row.get("final_score"),
        "score_breakdown": row.get("score_breakdown", {}),
        "data_date": row.get("data_date"),
        "snapshot_id": snapshot_id,
        "theme_hits": row.get("theme_hits", []) or [],
        "concept_hits": reason_struct.get("concept_hits", []) or [],
    }


def load_candidates(path: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if not path.exists():
        return entries
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def write_candidates_entries(entries: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if entries:
        content = "\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n"
    else:
        content = ""
    output_path.write_text(content, encoding="utf-8")


def write_candidates(report: Dict[str, Any], mode: str, output_path: Path, snapshot_id: str) -> None:
    results = report.get("results", [])
    if not isinstance(results, list):
        results = []

    new_entries = [_candidate_entry(row, mode, snapshot_id) for row in results]
    existing_entries = load_candidates(output_path)

    merged = [entry for entry in existing_entries if entry.get("mode") != mode]
    merged.extend(new_entries)

    mode_order = {"enhanced": 0, "tech_only": 1, "all": 2}

    def sort_key(entry: Dict[str, Any]) -> tuple:
        mode_value = mode_order.get(entry.get("mode"), 9)
        score = entry.get("final_score")
        try:
            score_value = float(score)
        except (TypeError, ValueError):
            score_value = float("-inf")
        item_id = str(entry.get("item_id") or entry.get("ticker") or "")
        return (mode_value, -score_value, item_id)

    merged = sorted(merged, key=sort_key)
    write_candidates_entries(merged, output_path)
