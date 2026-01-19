import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read_default_theme_map(repo_root: Path) -> Path:
    run_py = repo_root / "src" / "run.py"
    if run_py.exists():
        text = run_py.read_text(encoding="utf-8")
        match = re.search(r"--theme-map\".*?default=[\"']([^\"']+)[\"']", text, re.S)
        if match:
            value = match.group(1).strip()
            candidate = Path(value)
            return candidate if candidate.is_absolute() else repo_root / candidate
    return repo_root / "theme_to_industry_em_2026-01-20.csv"


def _resolve_theme_map(repo_root: Path, snapshot_id: str) -> Tuple[Path, bool]:
    snapshot_map = repo_root / f"theme_to_industry_em_{snapshot_id}.csv"
    if snapshot_map.exists():
        return snapshot_map, False
    legacy_map = repo_root / f"theme_to_industry_{snapshot_id}.csv"
    if legacy_map.exists():
        return legacy_map, False
    return _read_default_theme_map(repo_root), True


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_input_pool(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"input pool not found: {path}")
    suffix = path.suffix.lower()
    row_count = 0
    id_field = ""
    if suffix == ".jsonl":
        for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            row_count += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid jsonl at line {idx}: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"jsonl line {idx} must be an object")
            if "item_id" in row:
                id_field = "item_id"
            elif "ticker" in row:
                id_field = "ticker"
            else:
                raise ValueError(f"jsonl line {idx} missing item_id or ticker")
    elif suffix == ".csv":
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            if "item_id" in fieldnames:
                id_field = "item_id"
            elif "ticker" in fieldnames:
                id_field = "ticker"
            else:
                raise ValueError(
                    f"input pool csv missing item_id or ticker columns; found {fieldnames}"
                )
            for idx, row in enumerate(reader, start=2):
                row_count += 1
                value = row.get(id_field, "")
                if not value:
                    raise ValueError(f"csv row {idx} missing {id_field}")
    else:
        raise ValueError("unsupported input pool format; use .jsonl or .csv")

    if row_count == 0:
        raise ValueError(f"input pool has no rows: {path}")

    return {
        "path": str(path.resolve()),
        "rows": row_count,
        "sha256": _sha256_file(path),
        "id_field": id_field,
    }


def _read_snapshots(args: argparse.Namespace) -> List[str]:
    if args.snapshots:
        return [item.strip() for item in args.snapshots.split(",") if item.strip()]
    if args.snapshots_file:
        path = Path(args.snapshots_file)
        if not path.is_absolute():
            path = REPO_ROOT / path
        if not path.exists():
            raise FileNotFoundError(f"snapshots file not found: {path}")
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    raise ValueError("provide --snapshots or --snapshots-file")


def _run(cmd: List[str], env: Dict[str, str]) -> None:
    subprocess.check_call(cmd, cwd=REPO_ROOT, env=env)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run snapshot sweep regression matrix")
    parser.add_argument("--snapshots", default="", help="comma-separated snapshot ids")
    parser.add_argument("--snapshots-file", default="", help="file with snapshot ids per line")
    parser.add_argument("--input-pool", required=True, help="input pool path for comparability")
    parser.add_argument("--top-n", type=int, default=10, help="top n for export")
    parser.add_argument("--sort-key", default="final_score", help="sort key for export")
    parser.add_argument(
        "--out-path",
        default="artifacts_metrics/regression_matrix_timeseries_latest.json",
        help="output path",
    )
    args = parser.parse_args()

    snapshots = _read_snapshots(args)
    if not snapshots:
        raise ValueError("no snapshots provided")

    pool_path = Path(args.input_pool)
    if not pool_path.is_absolute():
        pool_path = REPO_ROOT / pool_path
    pool_meta = _load_input_pool(pool_path)

    out_path = Path(args.out_path)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []
    failures = 0

    for snapshot_id in snapshots:
        entry: Dict[str, Any] = {"snapshot_id": snapshot_id}
        try:
            theme_map_path, fallback = _resolve_theme_map(REPO_ROOT, snapshot_id)
            if not theme_map_path.exists():
                raise FileNotFoundError(f"theme map not found: {theme_map_path}")
            theme_map_sha = _sha256_file(theme_map_path)
            entry["theme_map_path"] = str(theme_map_path)
            entry["theme_map_sha256"] = theme_map_sha
            entry["theme_map_fallback"] = fallback

            env = os.environ.copy()
            env["THEME_MAP"] = str(theme_map_path)

            _run(
                [
                    sys.executable,
                    "tools/build_screener_candidates.py",
                    "--snapshot-id",
                    snapshot_id,
                    "--input-pool",
                    str(pool_path),
                ],
                env,
            )
            _run(
                [
                    sys.executable,
                    "tools/export_screener_topn.py",
                    "--top-n",
                    str(args.top_n),
                    "--sort-key",
                    args.sort_key,
                    "--modes",
                    "all,enhanced,tech_only",
                    "--source-path",
                    "artifacts_metrics/screener_candidates_latest.jsonl",
                ],
                env,
            )
            _run(
                [
                    sys.executable,
                    "tools/build_regression_matrix.py",
                ],
                env,
            )

            regression_path = REPO_ROOT / "artifacts_metrics" / "regression_matrix_latest.json"
            regression = json.loads(regression_path.read_text(encoding="utf-8"))
            entry["git_rev"] = regression.get("git_rev")
            entry["latest_log_path"] = regression.get("latest_log_path")
            entry["theme_precision_summary"] = regression.get("theme_precision_summary")
            entry["screener_coverage_summary"] = regression.get("screener_coverage_summary")
            entry["screener_topn_meta"] = regression.get("screener_topn_meta")
        except Exception as exc:
            failures += 1
            entry["error"] = str(exc)
        results.append(entry)

    payload = {
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "input_pool": pool_meta,
        "snapshots": results,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
