import argparse
import csv
import re
import sys
from pathlib import Path
from typing import List, Set


EXPECTED_HEADER = ["主题ID", "主题名称", "关键词", "对应行业/概念"]


def _read_base_header(path: Path) -> List[str]:
    if not path.exists():
        print(f"Base mapping file not found: {path}", file=sys.stderr)
        raise SystemExit(1)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    return [col.strip() for col in header if col.strip()]


def _load_themes(signals_path: Path) -> List[str]:
    if not signals_path.exists():
        print(f"Signals file not found: {signals_path}", file=sys.stderr)
        raise SystemExit(1)
    text = signals_path.read_text(encoding="utf-8")
    themes: List[str] = []
    for line in text.splitlines():
        match = re.match(r"^\s*theme\s*:\s*(.*)$", line)
        if not match:
            continue
        value = match.group(1).strip()
        if "#" in value:
            value = value.split("#", 1)[0].strip()
        if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
            value = value[1:-1].strip()
        if value and value not in themes:
            themes.append(value)
    return themes


def _load_concepts(path: Path) -> List[str]:
    if not path.exists():
        print(f"concept_membership.csv not found: {path}", file=sys.stderr)
        raise SystemExit(1)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        if "concept" not in header:
            print(f"Missing concept column: {header}", file=sys.stderr)
            raise SystemExit(1)
        concepts: List[str] = []
        seen: Set[str] = set()
        for row in reader:
            concept = str(row.get("concept", "")).strip()
            if not concept or concept in seen:
                continue
            seen.add(concept)
            concepts.append(concept)
    if not concepts:
        print("No concepts found in concept_membership.csv", file=sys.stderr)
        raise SystemExit(1)
    return concepts


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate CN EM theme map from snapshot concepts")
    parser.add_argument("--snapshot-as-of", required=True, help="Snapshot as-of date YYYY-MM-DD")
    parser.add_argument("--snapshot-dir", required=True, help="Snapshot directory path")
    parser.add_argument("--out", required=True, help="Output CSV path")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    base_header = _read_base_header(repo_root / "theme_to_industry.csv")
    if base_header != EXPECTED_HEADER:
        print(f"Unsupported base header: {base_header}", file=sys.stderr)
        raise SystemExit(1)

    selected_themes = _load_themes(repo_root / "signals.yaml")[:4]

    if len(selected_themes) < 3:
        print(f"Selected themes fewer than 3: {selected_themes}", file=sys.stderr)
        raise SystemExit(1)

    concepts = _load_concepts(Path(args.snapshot_dir) / "concept_membership.csv")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPECTED_HEADER)
        writer.writeheader()
        for idx, theme in enumerate(selected_themes, start=1):
            theme_id = f"em_{idx:02d}"
            for concept in concepts:
                writer.writerow(
                    {
                        "主题ID": theme_id,
                        "主题名称": theme,
                        "关键词": concept,
                        "对应行业/概念": concept,
                    }
                )
                rows_written += 1

    print(f"selected_themes={selected_themes}")
    print(f"concepts_count={len(concepts)}")
    print(f"rows_written={rows_written}")


if __name__ == "__main__":
    main()
