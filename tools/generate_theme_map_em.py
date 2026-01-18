import argparse
import csv
import sys
from pathlib import Path
from typing import List, Set, Tuple

import yaml


def _parse_concepts(text: str) -> List[str]:
    tokens: List[str] = []
    for line in text.splitlines():
        parts = [line]
        for sep in ("、", ",", "，", ";", "；"):
            split_parts = []
            for item in parts:
                split_parts.extend(item.split(sep))
            parts = split_parts
        for item in parts:
            cleaned = item.strip()
            if cleaned:
                tokens.append(cleaned)
    return tokens


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _read_base_header(path: Path) -> List[str]:
    if not path.exists():
        print(f"Base mapping file not found: {path}", file=sys.stderr)
        raise SystemExit(1)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    return [col.strip() for col in header if col.strip()]


def _determine_output_columns(header: List[str]) -> Tuple[str, List[str]]:
    lowered = [col.lower() for col in header]
    if len(header) == 2 and set(lowered) == {"theme", "industry"}:
        return "theme_industry", ["theme", "industry"]
    if len(header) == 2 and set(lowered) == {"theme", "concept"}:
        return "theme_concept", ["theme", "concept"]
    if "主题ID" in header and "对应行业/概念" in header:
        return "legacy_cn", header
    print(f"Unsupported base header: {header}", file=sys.stderr)
    raise SystemExit(1)


def _load_core_themes(signals_path: Path) -> List[str]:
    if not signals_path.exists():
        print(f"Signals file not found: {signals_path}", file=sys.stderr)
        raise SystemExit(1)
    raw = yaml.safe_load(signals_path.read_text(encoding="utf-8"))
    themes: List[str] = []
    for item in raw.get("signals", []):
        theme = item.get("core_theme") or item.get("theme")
        if theme and theme not in themes:
            themes.append(theme)
    return themes


def _row_from_columns(columns: List[str], theme: str, concept: str) -> dict:
    row = {}
    for col in columns:
        if col in ("theme", "主题名称"):
            row[col] = theme
        elif col in ("industry", "concept", "对应行业/概念", "关键词"):
            row[col] = concept
        elif col == "主题ID":
            row[col] = ""
        else:
            row[col] = ""
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate EM theme map CSV from concept list")
    parser.add_argument("--as-of", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--concepts-file", required=True, help="Concept list file path")
    parser.add_argument("--out", required=True, help="Output CSV path")
    args = parser.parse_args()

    base_header = _read_base_header(Path("theme_to_industry.csv"))
    mode, columns = _determine_output_columns(base_header)

    signals_themes = _load_core_themes(Path("signals.yaml"))
    if len(signals_themes) < 3:
        print("Insufficient core themes in signals.yaml (need >= 3).", file=sys.stderr)
        raise SystemExit(1)
    selected_themes = signals_themes[:4]
    if len(selected_themes) < 3:
        print("Selected themes fewer than 3.", file=sys.stderr)
        raise SystemExit(1)

    concepts_path = Path(args.concepts_file)
    if not concepts_path.exists():
        print(f"Concepts file not found: {concepts_path}", file=sys.stderr)
        raise SystemExit(1)

    raw_concepts = _parse_concepts(concepts_path.read_text(encoding="utf-8"))
    concepts = _dedupe_keep_order(raw_concepts)
    if not concepts:
        print("No concepts found in concepts file.", file=sys.stderr)
        raise SystemExit(1)

    rows = []
    for theme in selected_themes:
        for concept in concepts:
            rows.append(_row_from_columns(columns, theme, concept))

    if mode not in ("theme_industry", "theme_concept", "legacy_cn"):
        print(f"Unsupported output mode: {mode}", file=sys.stderr)
        raise SystemExit(1)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"selected_themes={selected_themes}")
    print(f"concepts_count={len(concepts)}")
    print(f"rows_written={len(rows)}")


if __name__ == "__main__":
    main()
