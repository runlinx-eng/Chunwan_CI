import json
import re
import subprocess
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    base_dir = Path("data/snapshots/2026-01-16")
    prices_path = base_dir / "prices.csv"
    membership_path = base_dir / "concept_membership.csv"
    manifest_path = base_dir / "manifest.json"

    for path in (prices_path, membership_path, manifest_path):
        if not path.exists():
            raise FileNotFoundError(f"missing file: {path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    stats = manifest.get("stats", {})
    if int(stats.get("unique_concepts", 0)) < 8:
        raise AssertionError("unique_concepts < 8")
    if int(stats.get("min_concept_members", 0)) < 50:
        raise AssertionError("min_concept_members < 50")
    if int(stats.get("min_price_bars", 0)) < 160:
        raise AssertionError("min_price_bars < 160")

    run_py = repo_root / "src" / "run.py"
    if run_py.exists():
        text = run_py.read_text(encoding="utf-8")
        match = re.search(r"--theme-map\".*?default=[\"']([^\"']+)[\"']", text, re.S)
        default_map = match.group(1).strip() if match else "theme_to_industry.csv"
    else:
        default_map = "theme_to_industry.csv"
    # This pack replays snapshot 2026-01-16; pin theme map to the same snapshot to keep gate semantics stable.
    theme_map = "theme_to_industry_em_2026-01-16.csv" or default_map

    cmd = (
        "python3 -m src.run --date 2026-01-16 --top 5 --provider snapshot "
        f"--no-fallback --snapshot-as-of 2026-01-16 --theme-map {theme_map}"
    )
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        raise SystemExit(ret)

    report_path = Path("outputs/report_2026-01-16_top5.json")
    if not report_path.exists():
        raise FileNotFoundError(f"missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    results = report.get("results", [])
    if len(results) != 5:
        raise AssertionError("results length != 5")

    for row in results:
        for key in ("ticker", "name", "final_score", "data_date"):
            if key not in row:
                raise AssertionError(f"missing field: {key}")


if __name__ == "__main__":
    main()
