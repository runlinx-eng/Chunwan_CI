import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


NONE_SIGNATURE = "__NONE__"


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iter_values(raw: Any) -> Iterable[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        for item in raw:
            if item is None:
                continue
            value = str(item).strip()
            if value:
                yield value
    else:
        value = str(raw).strip()
        if value:
            yield value


def _extract_theme_total(row: Dict[str, Any]) -> Optional[float]:
    breakdown = row.get("score_breakdown")
    if isinstance(breakdown, dict):
        for key in ("theme_total", "score_theme_total"):
            raw = breakdown.get(key)
            if raw is None:
                continue
            parsed = _coerce_float(raw)
            if parsed is not None:
                return parsed
    for key in ("theme_total", "score_theme_total"):
        if key in row:
            parsed = _coerce_float(row.get(key))
            if parsed is not None:
                return parsed
    return None


def _theme_hit_signature(row: Dict[str, Any]) -> str:
    themes = set()
    theme_hits = row.get("theme_hits")
    if isinstance(theme_hits, list):
        for hit in theme_hits:
            if not isinstance(hit, dict):
                continue
            raw = hit.get("theme")
            if raw is None:
                continue
            for value in _iter_values(raw):
                themes.add(value)
    if not themes:
        breakdown = row.get("score_breakdown")
        if isinstance(breakdown, dict):
            theme_components = breakdown.get("theme_components")
            if isinstance(theme_components, list):
                for hit in theme_components:
                    if not isinstance(hit, dict):
                        continue
                    raw = hit.get("theme")
                    if raw is None:
                        continue
                    for value in _iter_values(raw):
                        themes.add(value)
    if not themes:
        return NONE_SIGNATURE
    return "|".join(sorted(themes))


def _concept_hit_signature(row: Dict[str, Any]) -> str:
    concept_hits = row.get("concept_hits")
    if not isinstance(concept_hits, list):
        return NONE_SIGNATURE
    concepts = set()
    for hit in concept_hits:
        raw = None
        if isinstance(hit, dict):
            raw = hit.get("concept")
            if raw is None or str(raw).strip() == "":
                raw = hit.get("industry")
        else:
            raw = hit
        if raw is None:
            continue
        for value in _iter_values(raw):
            concepts.add(value)
    if not concepts:
        return NONE_SIGNATURE
    return "|".join(sorted(concepts))


def _top_k_items(counter: Counter, top_k: int) -> list:
    if top_k <= 0:
        return []
    items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [[item[0], item[1]] for item in items[:top_k]]


def _theme_total_summary(counter: Counter, n_value: int, top_k: int) -> Dict[str, Any]:
    unique_count = len(counter)
    ratio = float(unique_count) / float(n_value) if n_value > 0 else None
    return {
        "N": n_value,
        "unique_value_count": unique_count,
        "unique_value_ratio": ratio,
        "top_k_values": _top_k_items(counter, top_k),
    }


def _signature_summary(counter: Counter, n_value: int, top_k: int) -> Dict[str, Any]:
    unique_count = len(counter)
    ratio = float(unique_count) / float(n_value) if n_value > 0 else None
    return {
        "unique_set_count": unique_count,
        "unique_set_ratio": ratio,
        "top_k_signatures": _top_k_items(counter, top_k),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect candidates diversity stats")
    parser.add_argument(
        "--path",
        default="artifacts_metrics/screener_candidates_latest.jsonl",
        help="candidates jsonl path",
    )
    parser.add_argument("--top-k", type=int, default=10, help="top k values to report")
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise FileNotFoundError(f"candidates jsonl not found: {path}")

    mode_counts = {"enhanced": 0, "tech_only": 0}
    theme_total_counts = {"enhanced": Counter(), "tech_only": Counter()}
    theme_total_n = {"enhanced": 0, "tech_only": 0}
    theme_sig_counts = {"enhanced": Counter(), "all": Counter()}
    theme_sig_n = {"enhanced": 0, "all": 0}
    concept_sig_counts = {"enhanced": Counter(), "all": Counter()}
    concept_sig_n = {"enhanced": 0, "all": 0}

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

            mode_counts[mode] += 1

            if mode == "enhanced":
                theme_total = _extract_theme_total(row)
                if theme_total is not None:
                    theme_total_counts["enhanced"][theme_total] += 1
                    theme_total_n["enhanced"] += 1
            else:
                theme_total_counts["tech_only"][0.0] += 1
                theme_total_n["tech_only"] += 1

            theme_signature = _theme_hit_signature(row)
            theme_sig_counts["all"][theme_signature] += 1
            theme_sig_n["all"] += 1
            concept_signature = _concept_hit_signature(row)
            concept_sig_counts["all"][concept_signature] += 1
            concept_sig_n["all"] += 1

            if mode == "enhanced":
                theme_sig_counts["enhanced"][theme_signature] += 1
                theme_sig_n["enhanced"] += 1
                concept_sig_counts["enhanced"][concept_signature] += 1
                concept_sig_n["enhanced"] += 1

    top_k = args.top_k
    theme_total_all = theme_total_counts["enhanced"] + theme_total_counts["tech_only"]
    theme_total_all_n = theme_total_n["enhanced"] + theme_total_n["tech_only"]

    output = {
        "enhanced": {
            "mode_counts": {"enhanced": mode_counts["enhanced"]},
            "theme_total": _theme_total_summary(
                theme_total_counts["enhanced"], theme_total_n["enhanced"], top_k
            ),
            "theme_hit_signature": _signature_summary(
                theme_sig_counts["enhanced"], theme_sig_n["enhanced"], top_k
            ),
            "concept_hit_signature": _signature_summary(
                concept_sig_counts["enhanced"], concept_sig_n["enhanced"], top_k
            ),
            "top5_theme_total_values": _top_k_items(theme_total_counts["enhanced"], 5),
            "top5_theme_hit_signatures": _top_k_items(theme_sig_counts["enhanced"], 5),
        },
        "tech_only": {
            "mode_counts": {"tech_only": mode_counts["tech_only"]},
            "theme_total": _theme_total_summary(
                theme_total_counts["tech_only"], theme_total_n["tech_only"], top_k
            ),
        },
        "all": {
            "mode_counts": {
                "enhanced": mode_counts["enhanced"],
                "tech_only": mode_counts["tech_only"],
            },
            "theme_total": _theme_total_summary(theme_total_all, theme_total_all_n, top_k),
            "theme_hit_signature": _signature_summary(
                theme_sig_counts["all"], theme_sig_n["all"], top_k
            ),
            "concept_hit_signature": _signature_summary(
                concept_sig_counts["all"], concept_sig_n["all"], top_k
            ),
        },
    }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
