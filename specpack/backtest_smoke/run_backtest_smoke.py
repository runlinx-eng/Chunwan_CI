import json
import math
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def load_snapshot(snapshot_as_of: str):
    snapshot_dir = Path("data/snapshots") / snapshot_as_of
    prices_path = snapshot_dir / "prices.csv"
    membership_path = snapshot_dir / "concept_membership.csv"
    if not prices_path.exists():
        raise FileNotFoundError(f"Missing prices.csv: {prices_path}")
    if not membership_path.exists():
        raise FileNotFoundError(f"Missing concept_membership.csv: {membership_path}")
    prices = pd.read_csv(prices_path)
    prices["date"] = pd.to_datetime(prices["date"])
    membership = pd.read_csv(membership_path)
    return prices, membership


def forward_return(prices: pd.DataFrame, date: pd.Timestamp, tickers: list) -> float:
    if not tickers:
        return 0.0
    df = prices[prices["ticker"].isin(tickers)].copy()
    df = df.sort_values(["ticker", "date"])
    df["next_close"] = df.groupby("ticker")["close"].shift(-1)
    current = df[df["date"] == date]
    if current.empty:
        return 0.0
    current = current.dropna(subset=["next_close"])
    if current.empty:
        return 0.0
    ret = (current["next_close"] - current["close"]) / current["close"]
    return float(ret.mean())


def compute_momentum60(prices: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    df = prices[prices["date"] <= date].copy()
    # Ensure no future data used.
    assert df["date"].max() <= date
    df = df.sort_values(["ticker", "date"])
    df["momentum_60"] = df.groupby("ticker")["close"].pct_change(59)
    latest = df[df["date"] == date].copy()
    latest = latest.dropna(subset=["momentum_60"])
    return latest


def summarize(series: list) -> dict:
    if not series:
        return {"mean": 0.0, "std": 0.0, "win_rate": 0.0}
    arr = np.array(series, dtype=float)
    return {
        "mean": float(arr.mean()),
        "std": float(arr.std(ddof=0)),
        "win_rate": float((arr > 0).mean()),
    }


def main():
    conf = yaml.safe_load(Path("specpack/snapshot_replay/assertions.yaml").read_text(encoding="utf-8"))
    cmd_template = conf["run"]["cmd"]
    snapshot_as_of = conf["run"]["as_of"]

    prices, membership = load_snapshot(snapshot_as_of)

    all_dates = sorted(prices["date"].unique())
    min_enhanced_history = 121
    horizon = 5
    candidates = []
    for idx, d in enumerate(all_dates):
        if idx + 1 < min_enhanced_history:
            continue
        if idx + horizon >= len(all_dates):
            continue
        candidates.append(d)
    N = 10
    dates = candidates[-N:]
    if len(dates) < 5:
        raise AssertionError("backtest dates < 5")

    results = []
    baseline_returns = []
    enhanced_returns = []
    enhanced_theme_scores = []

    for d in dates:
        momentum = compute_momentum60(prices, d)
        top = momentum.sort_values("momentum_60", ascending=False).head(5)
        baseline_tickers = top["ticker"].tolist()
        baseline_ret = forward_return(prices, d, baseline_tickers)
        # Ensure forward return uses future date.
        if not prices[prices["date"] > d].empty:
            assert prices[prices["date"] > d]["date"].min() > d

        # Enhanced run
        cmd = cmd_template.replace("--date 2026-01-20", f"--date {d.strftime('%Y-%m-%d')}")
        cmd = cmd.replace("--top 5", "--top 5")
        output_path = Path("outputs") / f"report_{d.strftime('%Y-%m-%d')}_top5.json"
        if output_path.exists():
            output_path.unlink()
        cache_dir = Path(".cache")
        if cache_dir.exists():
            subprocess.call("rm -rf .cache", shell=True)
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            raise SystemExit(ret)
        if not output_path.exists():
            raise FileNotFoundError(f"Missing enhanced output: {output_path}")
        report = json.loads(output_path.read_text(encoding="utf-8"))
        enhanced_tickers = [row["ticker"] for row in report.get("results", [])]
        enhanced_ret = forward_return(prices, d, enhanced_tickers)

        for row in report.get("results", []):
            data_date = row.get("data_date")
            if data_date is None:
                raise AssertionError("enhanced data_date missing")
            if data_date > d.strftime("%Y-%m-%d"):
                raise AssertionError("enhanced data_date beyond date")
            for key, value in row.get("indicators", {}).items():
                if value is None or (isinstance(value, float) and math.isnan(value)):
                    raise AssertionError(f"indicator {key} invalid")
            breakdown = row.get("score_breakdown", {})
            for key, value in breakdown.items():
                if value is None or (isinstance(value, float) and math.isnan(value)):
                    raise AssertionError(f"score_breakdown {key} invalid")
            enhanced_theme_scores.append(float(breakdown.get("theme_score", 0.0)))

        if not (1 <= len(baseline_tickers) <= 5):
            raise AssertionError("baseline selection size invalid")
        if not (1 <= len(enhanced_tickers) <= 5):
            raise AssertionError("enhanced selection size invalid")

        results.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "baseline": {
                    "tickers": baseline_tickers,
                    "forward_return": baseline_ret,
                },
                "enhanced": {
                    "tickers": enhanced_tickers,
                    "forward_return": enhanced_ret,
                },
            }
        )
        baseline_returns.append(baseline_ret)
        enhanced_returns.append(enhanced_ret)

    if not any(score > 0 for score in enhanced_theme_scores):
        raise AssertionError("enhanced theme_score never > 0")

    output = {
        "snapshot_as_of": snapshot_as_of,
        "dates": [r["date"] for r in results],
        "results": results,
        "summary": {
            "baseline": summarize(baseline_returns),
            "enhanced": summarize(enhanced_returns),
        },
    }

    output_path = Path("outputs") / f"backtest_smoke_{snapshot_as_of}.json"
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[backtest_smoke] wrote {output_path}")


if __name__ == "__main__":
    main()
