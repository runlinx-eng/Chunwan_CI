import json
import subprocess
import sys
from pathlib import Path


def test_strategy_effectiveness_emits_alpha_objective_fields(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    input_path = tmp_path / "backtest_sample.json"
    output_path = tmp_path / "strategy_effectiveness.json"

    sample = {
        "snapshot_as_of": "2026-01-20",
        "config": {
            "horizons": [1],
            "alpha_objective": {
                "excess_return_weight": 1.0,
                "drawdown_penalty_weight": 0.5,
                "turnover_penalty_weight": 0.1,
                "max_drawdown_constraint": 0.1,
            },
        },
        "results": [
            {
                "date": "2026-01-01",
                "baseline": {"turnover": None},
                "enhanced": {"turnover": None},
                "horizons": {"1": {"baseline_return": 0.01, "enhanced_return": 0.02}},
            },
            {
                "date": "2026-01-02",
                "baseline": {"turnover": 0.2},
                "enhanced": {"turnover": 0.4},
                "horizons": {"1": {"baseline_return": -0.02, "enhanced_return": -0.01}},
            },
        ],
    }
    input_path.write_text(json.dumps(sample, ensure_ascii=False), encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            "tools/strategy_effectiveness_metrics.py",
            "--input",
            str(input_path),
            "--out",
            str(output_path),
        ],
        cwd=repo_root,
        check=True,
    )

    metrics = json.loads(output_path.read_text(encoding="utf-8"))
    assert "alpha_objective" in metrics
    per_horizon = metrics["per_horizon"]["1"]
    assert "objective_alpha" in per_horizon
    assert "avg_turnover_enhanced" in per_horizon
    assert "drawdown_constraint_passed" in per_horizon
    assert per_horizon["drawdown_constraint_passed"] is True
    assert "mean_objective_alpha" in metrics["overall"]
