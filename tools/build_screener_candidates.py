import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ALL_MODES = ["enhanced", "tech_only"]
DEFAULT_SNAPSHOT_ID = "2026-01-20"

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from src.candidates import load_candidates, write_candidates_entries  # noqa: E402


def _mode_distribution(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {mode: 0 for mode in ALL_MODES}
    for row in entries:
        mode = row.get("mode")
        if isinstance(mode, str) and mode in counts:
            counts[mode] += 1
    return counts


def _theme_map_info(repo_root: Path, override: Optional[str]) -> Tuple[Path, str]:
    env_map = os.environ.get("THEME_MAP")
    if env_map:
        path = Path(env_map)
        theme_map_path = path if path.is_absolute() else repo_root / path
    elif override:
        path = Path(override)
        theme_map_path = path if path.is_absolute() else repo_root / path
    else:
        theme_map_path = repo_root / "theme_to_industry_em_2026-01-20.csv"
    sha = hashlib.sha256(theme_map_path.read_bytes()).hexdigest()
    return theme_map_path, sha


def _latest_log(repo_root: Path) -> Optional[str]:
    logs_dir = repo_root / "artifacts_logs"
    if not logs_dir.exists():
        return None
    logs = sorted(logs_dir.glob("verify_*.txt"), key=os.path.getmtime)
    if not logs:
        return None
    return str(logs[-1])


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_input_pool(path: Path) -> Tuple[List[str], int, str]:
    if not path.exists():
        raise FileNotFoundError(f"input pool not found: {path}")
    suffix = path.suffix.lower()
    ids: List[str] = []
    row_count = 0
    has_item_id = False
    has_ticker = False

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
            value = row.get("item_id")
            if value:
                has_item_id = True
            else:
                value = row.get("ticker")
                if value:
                    has_ticker = True
            if not value:
                raise ValueError(f"jsonl line {idx} missing item_id or ticker")
            ids.append(str(value).strip())
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
            if id_field == "item_id":
                has_item_id = True
            else:
                has_ticker = True
            for idx, row in enumerate(reader, start=2):
                row_count += 1
                value = row.get(id_field, "")
                if not value:
                    raise ValueError(f"csv row {idx} missing {id_field}")
                ids.append(str(value).strip())
    else:
        raise ValueError("unsupported input pool format; use .jsonl or .csv")

    if row_count == 0:
        raise ValueError(f"input pool has no rows: {path}")

    id_field = "item_id" if has_item_id else "ticker"
    return ids, row_count, id_field


def _validate_modes(modes: List[str]) -> None:
    invalid_modes = [mode for mode in modes if mode not in ALL_MODES]
    if invalid_modes:
        raise ValueError(f"unsupported modes {invalid_modes}; expected subset of {ALL_MODES}")
    if not modes:
        raise ValueError("no modes specified")


def _run_snapshot(
    repo_root: Path,
    snapshot_id: str,
    theme_map_path: Path,
    mode: str,
) -> None:
    weight = 0.0 if mode == "tech_only" else 1.0
    cmd = [
        sys.executable,
        "-m",
        "src.run",
        "--date",
        snapshot_id,
        "--top",
        "5",
        "--provider",
        "snapshot",
        "--no-fallback",
        "--snapshot-as-of",
        snapshot_id,
        "--theme-map",
        str(theme_map_path),
        "--theme-weight",
        str(weight),
        "--no-cache",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    subprocess.check_call(cmd, cwd=repo_root, env=env)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build screener candidates artifact")
    parser.add_argument(
        "--out-path",
        default="artifacts_metrics/screener_candidates_latest.jsonl",
        help="output candidates jsonl path",
    )
    parser.add_argument(
        "--snapshot-id",
        default=DEFAULT_SNAPSHOT_ID,
        help="snapshot/as_of id for snapshot provider",
    )
    parser.add_argument("--modes", default="enhanced,tech_only", help="modes to include")
    parser.add_argument("--theme-map", default="", help="theme map path override")
    parser.add_argument("--input-pool", default="", help="optional input pool path")
    args = parser.parse_args()

    out_path = Path(args.out_path)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path

    modes = [item.strip() for item in args.modes.split(",") if item.strip()]
    _validate_modes(modes)

    theme_map_path, theme_map_sha256 = _theme_map_info(REPO_ROOT, args.theme_map or None)
    snapshot_id = args.snapshot_id

    default_candidates = REPO_ROOT / "artifacts_metrics" / "screener_candidates_latest.jsonl"
    if default_candidates.exists():
        default_candidates.unlink()

    for mode in modes:
        _run_snapshot(REPO_ROOT, snapshot_id, theme_map_path, mode)

    entries = load_candidates(default_candidates)
    if not entries:
        raise ValueError(f"candidates file has no entries: {default_candidates}")

    filtered = [row for row in entries if row.get("mode") in modes]
    if not filtered:
        raise ValueError(f"no candidates remain after filtering modes {modes}")

    input_pool_meta: Optional[Dict[str, Any]] = None
    if args.input_pool:
        pool_path = Path(args.input_pool)
        if not pool_path.is_absolute():
            pool_path = REPO_ROOT / pool_path
        pool_ids, pool_rows, pool_id_field = _load_input_pool(pool_path)
        pool_set = {value for value in pool_ids if value}
        if not pool_set:
            raise ValueError(f"input pool has no usable ids: {pool_path}")
        filtered = [
            row
            for row in filtered
            if str(row.get("item_id") or row.get("ticker") or "").strip() in pool_set
        ]
        if not filtered:
            raise ValueError(f"no candidates match input pool: {pool_path}")
        input_pool_meta = {
            "path": str(pool_path.resolve()),
            "rows": pool_rows,
            "sha256": _sha256_file(pool_path),
            "id_field": pool_id_field,
        }

    write_candidates_entries(filtered, out_path)

    git_rev = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()
    latest_log_path = _latest_log(REPO_ROOT)

    output_meta = {
        "git_rev": git_rev,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "snapshot_id": snapshot_id,
        "theme_map_path": str(theme_map_path),
        "theme_map_sha256": theme_map_sha256,
        "latest_log_path": latest_log_path,
        "source": {
            "provider": "snapshot",
            "snapshot_id": snapshot_id,
        },
        "input_pool": input_pool_meta,
        "output": {
            "path": str(out_path.resolve()),
            "rows": len(filtered),
            "mode_distribution": _mode_distribution(filtered),
        },
        "modes": modes,
    }
    meta_path = REPO_ROOT / "artifacts_metrics" / "screener_candidates_latest_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(output_meta, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
