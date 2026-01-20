import argparse
import json
import math
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional


def _extract_theme_total(row: Dict[str, Any]) -> Optional[float]:
    score_breakdown = row.get("score_breakdown")
    if isinstance(score_breakdown, dict):
        if "theme_total" in score_breakdown:
            value = score_breakdown.get("theme_total")
            return _normalize_number(value)
    if "theme_total" in row:
        return _normalize_number(row.get("theme_total"))
    if isinstance(score_breakdown, dict) and "score_theme_total" in score_breakdown:
        return _normalize_number(score_breakdown.get("score_theme_total"))
    if "score_theme_total" in row:
        return _normalize_number(row.get("score_theme_total"))
    return None


def _normalize_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num):
        return None
    return num


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate screener candidates health")
    parser.add_argument(
        "--path",
        default="artifacts_metrics/screener_candidates_latest.jsonl",
        help="candidates jsonl path",
    )
    args = parser.parse_args()

    path = Path(args.path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    if not path.exists():
        print(f"error=missing_candidates path={path}", file=sys.stderr)
        return 1

    enhanced_rows = 0
    concept_nonempty = 0
    theme_totals: Counter[Optional[float]] = Counter()

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("mode") != "enhanced":
            continue
        enhanced_rows += 1
        concept_hits = row.get("concept_hits")
        if isinstance(concept_hits, list) and concept_hits:
            concept_nonempty += 1
        theme_totals[_extract_theme_total(row)] += 1

    concept_nonempty_ratio = concept_nonempty / enhanced_rows if enhanced_rows else 0.0
    theme_total_unique_value_count = len(theme_totals)
    top1_value_count = theme_totals.most_common(1)[0][1] if theme_totals else 0

    print(
        "enhanced_rows={rows} concept_nonempty_ratio={ratio} "
        "theme_total_unique_value_count={unique_count} top1_value_count={top1}".format(
            rows=enhanced_rows,
            ratio=concept_nonempty_ratio,
            unique_count=theme_total_unique_value_count,
            top1=top1_value_count,
        )
    )

    if enhanced_rows > 0 and theme_total_unique_value_count == 1:
        top_value = theme_totals.most_common(1)[0][0]
        print(f"warning=theme_total_constant value={top_value}")

    if concept_nonempty_ratio <= 0:
        print(
            "error=concept_hits_nonempty_ratio_zero ratio={ratio}".format(
                ratio=concept_nonempty_ratio
            ),
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
