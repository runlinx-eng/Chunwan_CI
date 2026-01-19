import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


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


def _git_rev(repo_root: Path) -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()


def _snapshot_id(entries: List[Dict[str, Any]]) -> str:
    for row in entries:
        snapshot = row.get("snapshot_id")
        if snapshot:
            return str(snapshot)
    return ""


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    default_path = repo_root / "artifacts_metrics" / "screener_candidates_latest.jsonl"
    candidates_path = Path(os.environ.get("SCREENER_CANDIDATES_PATH", default_path))
    if not candidates_path.is_absolute():
        candidates_path = repo_root / candidates_path
    if not candidates_path.exists():
        raise FileNotFoundError(f"missing candidates: {candidates_path}")

    entries = _load_jsonl(candidates_path)
    total = len(entries)
    enhanced_count = sum(1 for row in entries if row.get("mode") == "enhanced")
    tech_only_count = sum(1 for row in entries if row.get("mode") == "tech_only")
    all_count = sum(1 for row in entries if row.get("mode") == "all")

    print(
        f"[screener_coverage] total={total} enhanced={enhanced_count} "
        f"tech_only={tech_only_count} all={all_count}"
    )

    if enhanced_count <= 0:
        raise AssertionError("enhanced_count must be > 0")
    if tech_only_count <= 0:
        raise AssertionError("tech_only_count must be > 0")
    if all_count != enhanced_count + tech_only_count:
        raise AssertionError("all_count must equal enhanced_count + tech_only_count")

    metrics = {
        "counts": {
            "total": total,
            "enhanced": enhanced_count,
            "tech_only": tech_only_count,
            "all": all_count,
        },
        "git_rev": _git_rev(repo_root),
        "snapshot_id": _snapshot_id(entries),
        "source_path": str(candidates_path),
    }
    metrics_path = repo_root / "artifacts_metrics" / "screener_coverage_latest.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
