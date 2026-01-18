import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict


def _load_report(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing report: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    outputs_dir = repo_root / "outputs"
    report_path = outputs_dir / "report_2026-01-16_top5.json"

    if report_path.exists():
        report_path.unlink()

    cmd = [
        sys.executable,
        "-m",
        "src.run",
        "--date",
        "2026-01-16",
        "--top",
        "5",
        "--provider",
        "snapshot",
        "--no-fallback",
        "--snapshot-as-of",
        "2026-01-16",
        "--theme-map",
        "theme_to_industry_em_2026-01-16.csv",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root) + (
        os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env and env["PYTHONPATH"] else ""
    )
    subprocess.check_call(cmd, cwd=repo_root, env=env)

    report = _load_report(report_path)
    args = report.get("provenance", {}).get("args", {})
    snapshot_as_of = args.get("snapshot_as_of")
    date_arg = args.get("date")
    theme_map = str(args.get("theme_map", ""))
    if snapshot_as_of != "2026-01-16":
        raise AssertionError(
            "snapshot_as_of mismatch: "
            f"{json.dumps(args, ensure_ascii=False, sort_keys=True)}"
        )
    if date_arg != "2026-01-16":
        raise AssertionError(
            "date mismatch: " f"{json.dumps(args, ensure_ascii=False, sort_keys=True)}"
        )
    if "theme_to_industry_em_2026-01-16.csv" not in theme_map:
        raise AssertionError(
            "theme_map mismatch: "
            f"{json.dumps(args, ensure_ascii=False, sort_keys=True)}"
        )
    results = report.get("results", [])
    if len(results) != 5:
        raise AssertionError("results_len != 5")

    debug = report.get("debug")
    if not isinstance(debug, dict):
        raise AssertionError("missing debug")
    if int(debug.get("n_candidates_scored", 0)) <= 0:
        raise AssertionError("debug.n_candidates_scored <= 0")
    if int(debug.get("theme_key_hit_count", 0)) <= 0:
        raise AssertionError("debug.theme_key_hit_count <= 0")
    if int(debug.get("n_theme_hit_tickers", 0)) <= 0:
        raise AssertionError("debug.n_theme_hit_tickers <= 0")
    top5_theme_totals = debug.get("top5_theme_totals", [])
    if not isinstance(top5_theme_totals, list) or not any(
        isinstance(value, (int, float)) and value > 0 for value in top5_theme_totals
    ):
        raise AssertionError("debug.top5_theme_totals has no positive value")

    for row in results:
        for key in ("ticker", "name", "final_score", "data_date", "score_breakdown"):
            if key not in row:
                raise AssertionError(f"missing {key}")

    print(
        "[real_theme_effectiveness] "
        f"n_theme_hit_tickers={debug.get('n_theme_hit_tickers')} "
        f"top5_theme_totals={debug.get('top5_theme_totals')}"
    )


if __name__ == "__main__":
    main()
