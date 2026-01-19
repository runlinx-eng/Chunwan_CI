import argparse
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
    if override:
        path = Path(override)
        theme_map_path = path if path.is_absolute() else repo_root / path
    else:
        env_map = os.environ.get("THEME_MAP")
        if env_map:
            path = Path(env_map)
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
