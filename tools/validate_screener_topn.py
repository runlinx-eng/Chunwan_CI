import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


REQUIRED_FIELDS = [
    "rank",
    "mode",
    "score_total",
    "score_total_source",
    "score_breakdown",
    "theme_hits",
    "concept_hits",
    "snapshot_id",
    "theme_map_path",
    "theme_map_sha256",
    "git_rev",
    "latest_log_path",
    "schema_version",
]


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid jsonl line {idx}: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"jsonl line {idx} must be an object")
        entries.append(row)
    return entries


def _missing_fields(row: Dict[str, Any]) -> List[str]:
    missing = [field for field in REQUIRED_FIELDS if field not in row]
    if "item_id" not in row and "ticker" not in row:
        missing.append("item_id_or_ticker")
    return missing


def _validate_path(path: Path) -> int:
    entries = _load_jsonl(path)
    if not entries:
        raise ValueError(f"topn jsonl has no entries: {path}")
    for row in entries:
        missing = _missing_fields(row)
        if missing:
            raise AssertionError(
                "missing fields {missing} in record: {sample}".format(
                    missing=missing,
                    sample=json.dumps(row, ensure_ascii=False),
                )
            )
    print(f"[validate_screener_topn] ok: path={path} rows={len(entries)}")
    return len(entries)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate screener topn jsonl schema")
    parser.add_argument(
        "--path",
        help="jsonl path to validate (if omitted, validate default three buckets)",
    )
    args = parser.parse_args()
    if args.path:
        path = Path(args.path)
        if not path.exists():
            raise FileNotFoundError(f"topn jsonl not found: {path}")
        _validate_path(path)
        return

    default_paths = [
        Path("artifacts_metrics/screener_topn_latest_all.jsonl"),
        Path("artifacts_metrics/screener_topn_latest_enhanced.jsonl"),
        Path("artifacts_metrics/screener_topn_latest_tech_only.jsonl"),
    ]
    missing = [str(path) for path in default_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "missing topn jsonl files: {paths}".format(paths=", ".join(missing))
        )
    for path in default_paths:
        _validate_path(path)


if __name__ == "__main__":
    main()
