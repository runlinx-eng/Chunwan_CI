import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ALL_MODES = ["enhanced", "tech_only"]


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            entries.append(row)
    return entries


def _mode_distribution(entries: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {mode: 0 for mode in ALL_MODES}
    for row in entries:
        mode = row.get("mode")
        if isinstance(mode, str) and mode in counts:
            counts[mode] += 1
    return counts


def _theme_map_info(repo_root: Path) -> Tuple[str, str]:
    metrics_path = repo_root / "artifacts_metrics" / "theme_map_sparsity_latest.json"
    if metrics_path.exists():
        metrics = _load_json(metrics_path)
        path = metrics.get("theme_map_path")
        sha = metrics.get("theme_map_sha256")
        if path and sha:
            return str(path), str(sha)
    theme_map = os.environ.get("THEME_MAP")
    if theme_map:
        path = Path(theme_map)
        theme_map_path = str(path if path.is_absolute() else repo_root / path)
    else:
        theme_map_path = str(repo_root / "theme_to_industry_em_2026-01-20.csv")
    sha = hashlib.sha256(Path(theme_map_path).read_bytes()).hexdigest()
    return theme_map_path, sha


def _latest_log(repo_root: Path) -> Optional[str]:
    logs_dir = repo_root / "artifacts_logs"
    if not logs_dir.exists():
        return None
    logs = sorted(logs_dir.glob("verify_*.txt"), key=os.path.getmtime)
    if not logs:
        return None
    return str(logs[-1])


def _snapshot_id(entries: List[Dict[str, Any]], fallback: Optional[str]) -> str:
    for row in entries:
        snapshot = row.get("snapshot_id")
        if snapshot:
            return str(snapshot)
        data_date = row.get("data_date") or row.get("date")
        if data_date:
            return str(data_date)
    return fallback or ""


def _validate_modes(entries: List[Dict[str, Any]], modes: List[str]) -> None:
    missing = [row for row in entries if "mode" not in row]
    if missing:
        raise ValueError(f"missing mode in entries; expected {ALL_MODES}")
    invalid = {row.get("mode") for row in entries if row.get("mode") not in ALL_MODES}
    if invalid:
        raise ValueError(f"invalid mode values {sorted(invalid)}; expected {ALL_MODES}")
    allowed = set(modes)
    invalid_modes = [mode for mode in modes if mode not in ALL_MODES]
    if invalid_modes:
        raise ValueError(f"unsupported modes {invalid_modes}; expected subset of {ALL_MODES}")
    if not allowed:
        raise ValueError("no modes specified")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build screener candidates artifact")
    parser.add_argument(
        "--out-path",
        default="artifacts_metrics/screener_candidates_latest.jsonl",
        help="output candidates jsonl path",
    )
    parser.add_argument("--snapshot-id", default="", help="snapshot/as_of id for meta")
    parser.add_argument("--modes", default="enhanced,tech_only", help="modes to include")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_path = Path(args.out_path)
    if not out_path.is_absolute():
        out_path = repo_root / out_path
    if not out_path.exists():
        raise FileNotFoundError(
            f"candidates file missing: {out_path}. "
            "Run phase10 verify or src.run to generate candidates first."
        )

    entries = _load_jsonl(out_path)
    if not entries:
        raise ValueError(f"candidates file has no entries: {out_path}")

    modes = [item.strip() for item in args.modes.split(",") if item.strip()]
    _validate_modes(entries, modes)

    filtered = [row for row in entries if row.get("mode") in modes]
    if not filtered:
        raise ValueError(f"no candidates remain after filtering modes {modes}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(json.dumps(row, ensure_ascii=False) for row in filtered) + "\n"
    out_path.write_text(content, encoding="utf-8")

    theme_map_path, theme_map_sha256 = _theme_map_info(repo_root)
    git_rev = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()
    latest_log_path = _latest_log(repo_root)
    snapshot_id = _snapshot_id(filtered, args.snapshot_id)

    meta = {
        "git_rev": git_rev,
        "snapshot_id": snapshot_id,
        "theme_map_path": theme_map_path,
        "theme_map_sha256": theme_map_sha256,
        "latest_log_path": latest_log_path,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "input": {
            "path": str(out_path.resolve()),
            "rows": len(entries),
            "mode_distribution": _mode_distribution(entries),
        },
        "output": {
            "path": str(out_path.resolve()),
            "rows": len(filtered),
            "mode_distribution": _mode_distribution(filtered),
        },
        "modes": modes,
    }
    meta_path = repo_root / "artifacts_metrics" / "screener_candidates_latest_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
