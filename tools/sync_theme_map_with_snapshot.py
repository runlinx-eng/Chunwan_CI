import argparse
import csv
import sys
from pathlib import Path
from typing import Any, List, Set

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _collect_theme_keys(obj: Any, values: List[str]) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "theme" and isinstance(value, str):
                theme = value.strip()
                if theme and theme not in values:
                    values.append(theme)
            _collect_theme_keys(value, values)
    elif isinstance(obj, list):
        for item in obj:
            _collect_theme_keys(item, values)


def _load_themes(signals_path: Path) -> List[str]:
    if yaml is None:
        print("PyYAML not available. Install with: pip install pyyaml", file=sys.stderr)
        raise SystemExit(1)
    if not signals_path.exists():
        print(f"signals.yaml not found: {signals_path}", file=sys.stderr)
        raise SystemExit(1)
    raw = yaml.safe_load(signals_path.read_text(encoding="utf-8"))
    themes: List[str] = []
    _collect_theme_keys(raw, themes)
    return themes


def _load_terms(snapshot_dir: Path) -> List[str]:
    membership_path = snapshot_dir / "concept_membership.csv"
    if not membership_path.exists():
        print(f"Missing concept_membership.csv: {membership_path}", file=sys.stderr)
        raise SystemExit(1)
    terms: List[str] = []
    seen: Set[str] = set()
    bad_terms = {"对应行业/概念", "关键词", "主题名称", "主题ID", "concept", "industry"}
    with membership_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        if "concept" not in header or "industry" not in header:
            print(f"Missing concept/industry columns: {header}", file=sys.stderr)
            raise SystemExit(1)
        for row in reader:
            for key in ("concept", "industry"):
                term = str(row.get(key, "")).strip()
                if not term or term in bad_terms:
                    continue
                if term not in seen:
                    seen.add(term)
                    terms.append(term)
    return terms


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync theme map with snapshot concepts")
    parser.add_argument("--snapshot-as-of", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--out", required=True, help="Output CSV path")
    parser.add_argument("--n-themes", type=int, default=4, help="Number of themes to include")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    snapshot_dir = repo_root / "data" / "snapshots" / args.snapshot_as_of

    themes = _load_themes(repo_root / "signals.yaml")
    selected_themes = themes[: args.n_themes]
    if not selected_themes:
        print("No themes found in signals.yaml", file=sys.stderr)
        raise SystemExit(1)

    terms = _load_terms(snapshot_dir)
    if not terms:
        print("No terms found in concept_membership.csv", file=sys.stderr)
        raise SystemExit(1)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["主题ID", "主题名称", "关键词", "对应行业/概念"],
        )
        writer.writeheader()
        for idx, theme in enumerate(selected_themes, start=1):
            theme_id = f"auto_{idx:02d}"
            for term in terms:
                writer.writerow(
                    {
                        "主题ID": theme_id,
                        "主题名称": theme,
                        "关键词": term,
                        "对应行业/概念": term,
                    }
                )

    print(f"as_of={args.snapshot_as_of}")
    print(f"selected_themes={selected_themes}")
    print(f"n_terms={len(terms)}")
    print(f"out_path={out_path}")


if __name__ == "__main__":
    main()
