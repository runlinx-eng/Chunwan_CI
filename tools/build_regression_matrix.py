import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing metrics: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _git_rev(repo_root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
    ).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default="artifacts_metrics/regression_matrix_latest.json",
        help="output path for regression matrix metrics",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = repo_root / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    metrics_dir = repo_root / "artifacts_metrics"
    theme_precision = _load_json(metrics_dir / "theme_precision_latest.json")
    theme_map_sparsity = _load_json(metrics_dir / "theme_map_sparsity_latest.json")
    theme_map_prune_path = metrics_dir / "theme_map_prune_latest.json"
    theme_map_prune = (
        json.loads(theme_map_prune_path.read_text(encoding="utf-8"))
        if theme_map_prune_path.exists()
        else None
    )

    payload = {
        "git_rev": _git_rev(repo_root),
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "theme_precision": theme_precision,
        "theme_map_sparsity": theme_map_sparsity,
    }
    if theme_map_prune is not None:
        payload["theme_map_prune"] = theme_map_prune

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[regression_matrix] written={out_path}")


if __name__ == "__main__":
    main()
