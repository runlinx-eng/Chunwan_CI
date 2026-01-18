import argparse
import csv
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


TERM_SPLIT_RE = re.compile(r"[,\uFF0C;\uFF1B、|\s]+")
WEIGHT_COLUMNS = [
    "weight",
    "权重",
    "frequency",
    "频次",
    "count",
    "出现次数",
    "hit_count",
    "coverage",
    "score",
]


def _split_terms(raw: str) -> List[str]:
    return [item.strip() for item in TERM_SPLIT_RE.split(str(raw)) if item.strip()]


def _read_runpy_default(repo_root: Path) -> Optional[str]:
    run_py = repo_root / "src" / "run.py"
    if not run_py.exists():
        return None
    text = run_py.read_text(encoding="utf-8")
    match = re.search(r"--theme-map\".*?default=[\"']([^\"']+)[\"']", text, re.S)
    if not match:
        return None
    return match.group(1).strip()


def _read_latest_report_theme_map(repo_root: Path) -> Optional[str]:
    outputs_dir = repo_root / "outputs"
    if not outputs_dir.exists():
        return None
    reports = sorted(outputs_dir.glob("report_*_top*.json"), key=os.path.getmtime, reverse=True)
    for path in reports:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            theme_map = report.get("provenance", {}).get("args", {}).get("theme_map")
            if theme_map:
                return str(theme_map)
        except Exception:
            continue
    return None


def _resolve_theme_map_path(repo_root: Path, override: Optional[str]) -> Path:
    if override:
        path = Path(override)
        return path if path.is_absolute() else repo_root / path
    default_path = _read_runpy_default(repo_root)
    if default_path:
        candidate = Path(default_path)
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        if candidate.exists():
            return candidate
    report_path = _read_latest_report_theme_map(repo_root)
    if report_path:
        candidate = Path(report_path)
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        if candidate.exists():
            return candidate
    return repo_root / "theme_to_industry.csv"


def _load_hit_counts(repo_root: Path) -> Dict[str, int]:
    outputs_dir = repo_root / "outputs"
    if not outputs_dir.exists():
        return {}
    counts: Dict[str, int] = {}
    for path in outputs_dir.glob("report_*_top*.json"):
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in report.get("results", []) if isinstance(report.get("results"), list) else []:
            for hit in row.get("theme_hits", []) if isinstance(row.get("theme_hits"), list) else []:
                for term in hit.get("matched_terms", []) if isinstance(hit.get("matched_terms"), list) else []:
                    key = str(term).strip()
                    if not key:
                        continue
                    counts[key] = counts.get(key, 0) + 1
    return counts


def _detect_schema(header: List[str]) -> Tuple[str, str, List[str]]:
    if "主题名称" in header:
        return "cn", "主题名称", ["关键词", "对应行业/概念"]
    if "theme" in header:
        term_cols = []
        if "concept" in header:
            term_cols.append("concept")
        if "industry" in header:
            term_cols.append("industry")
        if "map_values" in header:
            term_cols.append("map_values")
        if not term_cols:
            raise SystemExit(f"Unsupported theme map header: {header}")
        return "en", "theme", term_cols
    raise SystemExit(f"Unsupported theme map header: {header}")


def _get_term_raw(row: Dict[str, Any], term_cols: List[str], schema: str) -> str:
    if schema == "cn":
        raw = str(row.get("关键词", "")).strip()
        if not raw or raw.lower() == "nan":
            raw = str(row.get("对应行业/概念", "")).strip()
        return raw
    for col in term_cols:
        raw = str(row.get(col, "")).strip()
        if raw and raw.lower() != "nan":
            return raw
    return ""


def _parse_weight(row: Dict[str, Any]) -> Optional[float]:
    for col in WEIGHT_COLUMNS:
        if col not in row:
            continue
        raw = str(row.get(col, "")).strip()
        if not raw or raw.lower() == "nan":
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def _build_candidates(
    path: Path,
) -> Tuple[List[str], str, List[str], List[Dict[str, Any]], bool, int, int, int]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        has_weight_column = any(col in header for col in WEIGHT_COLUMNS)
        schema, theme_col, term_cols = _detect_schema(header)
        rows: List[Dict[str, Any]] = []
        row_count = 0
        unique_themes: set = set()
        unique_terms: set = set()
        for idx, row in enumerate(reader):
            row_count += 1
            theme = str(row.get(theme_col, "")).strip()
            if not theme:
                continue
            unique_themes.add(theme)
            term_raw = _get_term_raw(row, term_cols, schema)
            if not term_raw or term_raw.lower() == "nan":
                continue
            terms = _split_terms(term_raw)
            if not terms:
                continue
            weight = _parse_weight(row)
            for term in terms:
                unique_terms.add(term)
                rows.append(
                    {
                        "row_index": idx,
                        "theme": theme,
                        "term": term,
                        "row": row,
                        "weight": weight,
                    }
                )
        return header, theme_col, term_cols, rows, has_weight_column, row_count, len(unique_themes), len(unique_terms)


def _select_terms(
    candidates: List[Dict[str, Any]],
    has_weight_column: bool,
    min_concepts: int,
    min_score: float,
    lambda_penalty: float,
) -> Tuple[Dict[str, List[Dict[str, Any]]], str]:
    theme_candidates: Dict[str, Dict[str, Dict[str, Any]]] = {}
    theme_order: List[str] = []

    for item in candidates:
        theme = item["theme"]
        term = item["term"]
        if has_weight_column:
            local_support = float(item.get("weight") or 0.0)
        else:
            local_support = 1.0
        entry = {
            "term": term,
            "row": item["row"],
            "row_index": item["row_index"],
            "local_support": float(local_support),
        }
        if theme not in theme_candidates:
            theme_candidates[theme] = {}
            theme_order.append(theme)
        existing = theme_candidates[theme].get(term)
        if existing is None:
            theme_candidates[theme][term] = entry
        else:
            if entry["local_support"] > existing["local_support"] or (
                entry["local_support"] == existing["local_support"]
                and entry["row_index"] < existing["row_index"]
            ):
                theme_candidates[theme][term] = entry

    term_theme_counts: Dict[str, int] = {}
    for theme, term_map in theme_candidates.items():
        for term in term_map:
            term_theme_counts[term] = term_theme_counts.get(term, 0) + 1
    num_themes = len(theme_candidates)

    term_hash = {
        term: int(hashlib.md5(term.encode("utf-8")).hexdigest()[:8], 16)
        for term in term_theme_counts
    }

    selected: Dict[str, List[Dict[str, Any]]] = {theme: [] for theme in theme_candidates}
    selected_terms_global: Dict[str, int] = {}
    max_per_theme = 3

    theme_index = {theme: idx for idx, theme in enumerate(theme_order)}
    blocked_themes: set = set()

    for _ in range(max_per_theme):
        for theme in theme_order:
            if theme in blocked_themes:
                continue
            if len(selected[theme]) >= max_per_theme:
                continue
            term_map = theme_candidates[theme]
            already = {item["term"] for item in selected[theme]}
            best_entry = None
            best_key = None
            best_score = None
            for term, entry in term_map.items():
                if term in already:
                    continue
                base_freq = term_theme_counts.get(term, 1)
                selected_count = selected_terms_global.get(term, 0)
                penalty = lambda_penalty * (base_freq / max(1, num_themes))
                score = entry["local_support"] - penalty
                bias = (term_hash.get(term, 0) + theme_index.get(theme, 0)) % 1000000
                key = (
                    -score,
                    -entry["local_support"],
                    selected_count,
                    base_freq,
                    bias,
                    entry["row_index"],
                    term,
                )
                if best_key is None or key < best_key:
                    best_key = key
                    best_entry = entry
                    best_score = score
            if best_entry is None:
                break
            if len(selected[theme]) >= min_concepts and best_score is not None and best_score < min_score:
                blocked_themes.add(theme)
                continue
            selected[theme].append(best_entry)
            selected_terms_global[best_entry["term"]] = selected_terms_global.get(best_entry["term"], 0) + 1

    strategy = "diversity_penalty"
    return selected, strategy


def _write_pruned(
    header: List[str],
    schema: str,
    theme_col: str,
    term_cols: List[str],
    selected: Dict[str, List[Dict[str, Any]]],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for theme, entries in selected.items():
            for entry in entries:
                term = entry["term"]
                row = dict(entry["row"])
                row[theme_col] = theme
                if schema == "cn":
                    row["关键词"] = term
                    row["对应行业/概念"] = term
                else:
                    if term_cols:
                        row[term_cols[0]] = term
                        if len(term_cols) > 1:
                            row[term_cols[1]] = term
                writer.writerow({col: row.get(col, "") for col in header})


def _build_summary(
    theme_map_path: Path,
    out_path: Path,
    candidates: List[Dict[str, Any]],
    selected: Dict[str, List[Dict[str, Any]]],
    strategy: str,
) -> Dict[str, Any]:
    all_terms = {}
    for item in candidates:
        all_terms.setdefault(item["theme"], set()).add(item["term"])

    summary: Dict[str, Any] = {
        "theme_map_path": str(theme_map_path),
        "output_path": str(out_path),
        "selection_strategy": strategy,
        "themes": {},
        "distribution": {},
    }

    kept_terms_all: List[str] = []
    for theme, terms_set in all_terms.items():
        kept = [item["term"] for item in selected.get(theme, [])]
        removed = sorted(set(terms_set) - set(kept))
        kept_terms_all.extend(kept)
        summary["themes"][theme] = {
            "kept": kept,
            "removed": removed,
            "kept_count": len(kept),
            "removed_count": len(removed),
        }
        summary["distribution"].setdefault(str(len(kept)), 0)
        summary["distribution"][str(len(kept))] += 1

    unique_terms = len(set(kept_terms_all))
    total_terms = len(kept_terms_all)
    duplicate_terms = max(total_terms - unique_terms, 0)
    term_counts: Dict[str, int] = {}
    for term in kept_terms_all:
        term_counts[term] = term_counts.get(term, 0) + 1
    top_repeated = sorted(
        [{"concept": term, "themes": count} for term, count in term_counts.items()],
        key=lambda x: (-x["themes"], x["concept"]),
    )[:20]
    unique_triplets = {
        tuple(sorted(item["term"] for item in selected.get(theme, [])))
        for theme in selected
    }
    summary["global"] = {
        "total_terms": total_terms,
        "unique_terms": unique_terms,
        "duplicate_terms": duplicate_terms,
        "duplicate_ratio": (duplicate_terms / total_terms) if total_terms else 0.0,
        "top_repeated_terms": top_repeated,
    }
    summary["concepts_per_theme_histogram"] = dict(summary["distribution"])
    summary["top_repeated_concepts"] = top_repeated
    summary["unique_triplets_count"] = len(unique_triplets)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Prune theme map to max 3 concepts per theme")
    parser.add_argument("--in", dest="input_path", default="", help="Input theme map path")
    parser.add_argument("--verbose", action="store_true", help="Print inspect stats before/after pruning")
    parser.add_argument(
        "--inspect",
        action="store_true",
        dest="verbose",
        help="Alias for --verbose",
    )
    parser.add_argument(
        "--min-concepts",
        type=int,
        default=1,
        help="Minimum concepts per theme before applying stop rule",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.0,
        help="Stop selecting additional concepts when best score < min-score",
    )
    parser.add_argument(
        "--lambda",
        dest="lambda_penalty",
        type=float,
        default=0.5,
        help="Penalty multiplier for global theme frequency",
    )
    parser.add_argument(
        "--out",
        default="artifacts_metrics/theme_to_industry_pruned.csv",
        help="Output theme map path",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    theme_map_path = _resolve_theme_map_path(repo_root, args.input_path or None)
    if not theme_map_path.exists():
        raise SystemExit(f"Theme map not found: {theme_map_path}")

    if args.verbose:
        print(f"input_path={theme_map_path.resolve()}")
        try:
            lines = theme_map_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            lines = []
        header_line = lines[0] if lines else ""
        print(f"input_header_line: {header_line}")
        for idx, line in enumerate(lines[1:6], start=1):
            print(f"input_data_line_{idx}: {line}")

    (
        header,
        theme_col,
        term_cols,
        candidates,
        has_weight_column,
        row_count,
        unique_theme_count,
        unique_term_count,
    ) = _build_candidates(theme_map_path)
    if args.verbose:
        print(f"rows_read={row_count}")
        print(f"unique_themes={unique_theme_count}")
        print(f"unique_concepts={unique_term_count}")
        print(f"has_weight_columns={has_weight_column}")
    selected, strategy = _select_terms(
        candidates,
        has_weight_column,
        max(args.min_concepts, 1),
        args.min_score,
        args.lambda_penalty,
    )

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = repo_root / out_path
    schema = "cn" if "主题名称" in header else "en"
    _write_pruned(header, schema, theme_col, term_cols, selected, out_path)

    summary = _build_summary(theme_map_path, out_path, candidates, selected, strategy)
    metrics_dir = repo_root / "artifacts_metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    summary_path = metrics_dir / "theme_map_prune_latest.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"theme_map_path={theme_map_path}")
    print(f"output_path={out_path}")
    print(f"summary_path={summary_path}")
    if args.verbose:
        concept_freq: Dict[str, int] = {}
        theme_terms: Dict[str, set] = {}
        for theme, entries in selected.items():
            theme_terms.setdefault(theme, set())
            for entry in entries:
                term = entry["term"]
                theme_terms[theme].add(term)
        for terms in theme_terms.values():
            for term in terms:
                concept_freq[term] = concept_freq.get(term, 0) + 1
        histogram: Dict[str, int] = {}
        for terms in theme_terms.values():
            key = str(len(terms))
            histogram[key] = histogram.get(key, 0) + 1
        top_repeated = sorted(
            [{"concept": term, "themes": count} for term, count in concept_freq.items()],
            key=lambda x: (-x["themes"], x["concept"]),
        )[:30]
        unique_triplets = {
            tuple(sorted(terms)) for terms in theme_terms.values()
        }
        print(f"concepts_per_theme_histogram={histogram}")
        print(f"top_repeated_concepts={top_repeated}")
        print(f"unique_triplets_count={len(unique_triplets)}")


if __name__ == "__main__":
    main()
