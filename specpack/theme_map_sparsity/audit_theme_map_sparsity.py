import csv
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


TERM_SPLIT_RE = re.compile(r"[,\uFF0C;\uFF1B、|\s]+")


def _split_terms(value: str) -> List[str]:
    return [item.strip() for item in TERM_SPLIT_RE.split(str(value)) if item.strip()]


def _percentile(values: List[int], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return float(min(values))
    if q >= 1:
        return float(max(values))
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return float(ordered[lo])
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def _match_column(
    headers: List[str], exact_names: List[str], keywords: List[str]
) -> Optional[str]:
    for name in exact_names:
        for header in headers:
            if header.strip().lower() == name.lower():
                return header
    for header in headers:
        lowered = header.lower()
        for key in keywords:
            if key in lowered or key in header:
                return header
    return None


def _resolve_theme_map(repo_root: Path) -> Path:
    raw = os.environ.get("THEME_MAP")
    if raw:
        path = Path(raw)
        return path if path.is_absolute() else repo_root / path
    return repo_root / "theme_to_industry_em_2026-01-20.csv"


def _read_theme_map(path: Path) -> Tuple[Dict[str, Set[str]], int]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("theme map has no header row")
        headers = [field.strip().lstrip("\ufeff") for field in reader.fieldnames]
        reader.fieldnames = headers
        theme_col = _match_column(headers, ["theme", "主题名称"], ["theme", "主题"])
        concept_col = _match_column(headers, ["concept", "概念", "对应行业/概念"], ["concept", "概念"])
        if concept_col is None:
            concept_col = _match_column(headers, ["industry", "行业"], ["industry", "行业"])
        if theme_col is None or concept_col is None:
            raise ValueError(f"missing required columns: headers={headers}")

        theme_terms: Dict[str, Set[str]] = {}
        rows = 0
        for row in reader:
            rows += 1
            theme = str(row.get(theme_col, "")).strip()
            if not theme:
                continue
            raw_term = str(row.get(concept_col, "") or "").strip()
            if not raw_term or raw_term.lower() == "nan":
                continue
            for term in _split_terms(raw_term):
                if not term:
                    continue
                theme_terms.setdefault(theme, set()).add(term)

    return theme_terms, rows


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    theme_map_path = _resolve_theme_map(repo_root)
    if not theme_map_path.exists():
        raise FileNotFoundError(f"theme map missing: {theme_map_path}")

    theme_terms, rows = _read_theme_map(theme_map_path)
    theme_counts = {theme: len(terms) for theme, terms in theme_terms.items()}
    concepts_per_theme = list(theme_counts.values())
    unique_themes = len(theme_terms)
    unique_concepts = len({term for terms in theme_terms.values() for term in terms})

    if concepts_per_theme:
        min_val = min(concepts_per_theme)
        max_val = max(concepts_per_theme)
        p50 = _percentile(concepts_per_theme, 0.5)
        p95 = _percentile(concepts_per_theme, 0.95)
    else:
        min_val = 0
        max_val = 0
        p50 = 0.0
        p95 = 0.0

    max_theme = ""
    if theme_counts:
        max_themes = [theme for theme, count in theme_counts.items() if count == max_val]
        max_theme = sorted(max_themes)[0]

    metrics_path = repo_root / "artifacts_metrics" / "theme_map_sparsity_latest.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics = {
        "theme_map_path": str(theme_map_path),
        "theme_map_sha256": _sha256(theme_map_path),
        "rows": rows,
        "unique_themes": unique_themes,
        "unique_concepts": unique_concepts,
        "concepts_per_theme": {
            "min": min_val,
            "p50": p50,
            "p95": p95,
            "max": max_val,
        },
    }
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[theme_map_sparsity] rows={rows} unique_themes={themes} unique_concepts={concepts} "
        "concepts_per_theme_p95={p95} max={max_val}".format(
            rows=rows,
            themes=unique_themes,
            concepts=unique_concepts,
            p95=p95,
            max_val=max_val,
        )
    )

    if max_val > 3:
        raise AssertionError(
            f"max_concepts_per_theme {max_val} > 3 for theme '{max_theme}'"
        )


if __name__ == "__main__":
    main()
