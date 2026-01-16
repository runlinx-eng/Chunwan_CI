import json
from pathlib import Path
import subprocess

import pandas as pd


def main() -> None:
    cmd = (
        "python3 -m src.run --date 2026-01-20 --top 5 "
        "--provider snapshot --no-fallback --snapshot-as-of 2026-01-20"
    )
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        raise SystemExit(ret)

    report_path = Path("outputs/report_2026-01-20_top5.json")
    if not report_path.exists():
        raise FileNotFoundError(f"missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    results = report.get("results", [])

    membership = pd.read_csv("data/snapshots/2026-01-20/concept_membership.csv")
    membership_concepts = set(membership["concept"].astype(str).tolist())

    k = 8
    for row in results:
        reason = row.get("reason", {})
        if isinstance(reason, dict):
            reason_obj = reason
        else:
            reason_obj = row.get("reason_struct", {}) or {}
        themes_used = reason_obj.get("themes_used", [])
        themes_used = list(dict.fromkeys(themes_used))
        if not (3 <= len(themes_used) <= 5):
            raise AssertionError("themes_used count out of range")

        breakdown = row.get("score_breakdown", {})
        for key in ("score_total", "score_tech_total", "score_theme_total"):
            if key not in breakdown:
                raise AssertionError(f"missing score_breakdown.{key}")

        score_total = float(breakdown["score_total"])
        score_tech_total = float(breakdown["score_tech_total"])
        score_theme_total = float(breakdown["score_theme_total"])
        if round(score_total, k) != round(score_tech_total + score_theme_total, k):
            raise AssertionError("score_total mismatch")

        if score_theme_total > 0:
            concept_hits = reason_obj.get("concept_hits", [])
            if not concept_hits:
                raise AssertionError("concept_hits empty with theme score")
            for hit in concept_hits:
                concept = str(hit.get("concept", ""))
                if concept not in membership_concepts:
                    raise AssertionError("concept not in membership")


if __name__ == "__main__":
    main()
