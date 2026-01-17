import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _theme_weight_from_report(report: Dict[str, Any]) -> Optional[float]:
    try:
        raw = report["provenance"]["args"]["theme_weight"]
    except Exception:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _theme_scores(report: Dict[str, Any]) -> List[float]:
    scores: List[float] = []
    for row in report.get("results", []):
        breakdown = row.get("score_breakdown")
        if isinstance(breakdown, dict) and "score_theme_total" in breakdown:
            try:
                scores.append(float(breakdown["score_theme_total"]))
            except (TypeError, ValueError):
                continue
    return scores


def _debug_payload(report: Dict[str, Any], repo_root: Path, base_report: Path) -> str:
    return (
        f"python={sys.executable}; theme_weight={_theme_weight_from_report(report)}; "
        f"score_theme_total={_theme_scores(report)}; repo_root={repo_root}; "
        f"base_report={base_report}; cwd={os.getcwd()}"
    )


def run_and_load(
    cmd: List[str],
    base_report: Path,
    copy_path: Path,
    repo_root: Path,
    expected_weight: float,
    label: str,
) -> Dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root) + (
        os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env and env["PYTHONPATH"] else ""
    )
    shutil.rmtree(repo_root / ".cache", ignore_errors=True)
    subprocess.check_call(cmd, cwd=repo_root, env=env)
    if not base_report.exists():
        raise FileNotFoundError(f"{label}: missing report: {base_report}")
    report = json.loads(base_report.read_text(encoding="utf-8"))
    results = report.get("results", [])
    if len(results) != 5:
        raise AssertionError(
            f"{label}: results_len != 5; {_debug_payload(report, repo_root, base_report)}"
        )
    actual_weight = _theme_weight_from_report(report)
    if actual_weight is None:
        raise AssertionError(
            f"{label}: missing provenance.args.theme_weight; "
            f"{_debug_payload(report, repo_root, base_report)}"
        )
    if actual_weight != expected_weight:
        raise AssertionError(
            f"{label}: theme_weight mismatch (expected {expected_weight}); "
            f"{_debug_payload(report, repo_root, base_report)}"
        )
    shutil.copyfile(base_report, copy_path)
    return report


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    date_str = "2026-01-20"
    enhanced_cmd = [
        sys.executable,
        "-m",
        "src.run",
        "--date",
        date_str,
        "--top",
        "5",
        "--provider",
        "snapshot",
        "--no-fallback",
        "--snapshot-as-of",
        date_str,
        "--theme-map",
        "theme_to_industry.csv",
        "--theme-weight",
        "1",
        "--no-cache",
    ]
    tech_only_cmd = [
        sys.executable,
        "-m",
        "src.run",
        "--date",
        date_str,
        "--top",
        "5",
        "--provider",
        "snapshot",
        "--no-fallback",
        "--snapshot-as-of",
        date_str,
        "--theme-map",
        "theme_to_industry.csv",
        "--theme-weight",
        "0",
        "--no-cache",
    ]

    outputs_dir = repo_root / "outputs"
    base_report = outputs_dir / f"report_{date_str}_top5.json"
    enhanced_copy = outputs_dir / f"report_{date_str}_top5_enhanced.json"
    tech_only_copy = outputs_dir / f"report_{date_str}_top5_techonly.json"

    for path in (base_report, enhanced_copy, tech_only_copy):
        if path.exists():
            path.unlink()

    enhanced_report = run_and_load(
        enhanced_cmd,
        base_report,
        enhanced_copy,
        repo_root,
        expected_weight=1.0,
        label="enhanced",
    )

    if base_report.exists():
        base_report.unlink()

    tech_report = run_and_load(
        tech_only_cmd,
        base_report,
        tech_only_copy,
        repo_root,
        expected_weight=0.0,
        label="tech_only",
    )

    enhanced_report = json.loads(enhanced_copy.read_text(encoding="utf-8"))
    tech_report = json.loads(tech_only_copy.read_text(encoding="utf-8"))

    enhanced_results = enhanced_report.get("results", [])
    tech_results = tech_report.get("results", [])
    if len(enhanced_results) != 5:
        raise AssertionError("enhanced results_len != 5")
    if len(tech_results) != 5:
        raise AssertionError("tech_only results_len != 5")

    for report in (enhanced_report, tech_report):
        for row in report.get("results", []):
            breakdown = row.get("score_breakdown")
            if breakdown is None:
                raise AssertionError(
                    f"missing score_breakdown; {_debug_payload(report, repo_root, base_report)}"
                )
            if "score_total" not in breakdown:
                raise AssertionError(
                    f"missing score_total; {_debug_payload(report, repo_root, base_report)}"
                )
            if "score_tech_total" not in breakdown:
                raise AssertionError(
                    f"missing score_tech_total; {_debug_payload(report, repo_root, base_report)}"
                )
            if "score_theme_total" not in breakdown:
                raise AssertionError(
                    f"missing score_theme_total; {_debug_payload(report, repo_root, base_report)}"
                )

    for row in tech_results:
        breakdown = row.get("score_breakdown")
        score_theme_total = float(breakdown["score_theme_total"])
        if abs(score_theme_total) > 1e-9:
            raise AssertionError(
                "tech_only score_theme_total not zero; "
                f"{_debug_payload(tech_report, repo_root, base_report)}"
            )

    enhanced_theme_scores = [
        float(row["score_breakdown"]["score_theme_total"]) for row in enhanced_results
    ]
    if not any(value > 0 for value in enhanced_theme_scores):
        raise AssertionError(
            "enhanced has no positive theme score; "
            f"{_debug_payload(enhanced_report, repo_root, base_report)}"
        )

    for row in enhanced_results:
        breakdown = row["score_breakdown"]
        score_total = float(breakdown["score_total"])
        score_tech_total = float(breakdown["score_tech_total"])
        score_theme_total = float(breakdown["score_theme_total"])
        if round(score_total, 8) != round(score_tech_total + score_theme_total, 8):
            raise AssertionError(
                "enhanced score_total mismatch; "
                f"{_debug_payload(enhanced_report, repo_root, base_report)}"
            )

    enhanced_tickers = [row.get("ticker") for row in enhanced_results]
    tech_tickers = [row.get("ticker") for row in tech_results]
    if enhanced_tickers == tech_tickers:
        for idx, row in enumerate(enhanced_results):
            enh_breakdown = row["score_breakdown"]
            tech_breakdown = tech_results[idx]["score_breakdown"]
            enh_score_total = float(enh_breakdown["score_total"])
            tech_score_total = float(tech_breakdown["score_total"])
            enh_theme_total = float(enh_breakdown["score_theme_total"])
            if round(enh_score_total - tech_score_total, 8) != round(enh_theme_total, 8):
                raise AssertionError(
                    "aligned score diff mismatch; "
                    f"enhanced={_debug_payload(enhanced_report, repo_root, base_report)}; "
                    f"tech_only={_debug_payload(tech_report, repo_root, base_report)}"
                )


if __name__ == "__main__":
    main()
