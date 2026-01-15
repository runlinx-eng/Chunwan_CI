import json
import math
import shlex
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

N_DATES = 30
HORIZONS = [1, 5, 20]
TOP_N = 5
MIN_ENHANCED_HISTORY = 80


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
    return prices


def compute_momentum60(prices: pd.DataFrame, date: pd.Timestamp) -> pd.DataFrame:
    df = prices[prices["date"] <= date].copy()
    assert df["date"].max() <= date
    df = df.sort_values(["ticker", "date"])
    df["momentum_60"] = df.groupby("ticker")["close"].pct_change(59)
    latest = df[df["date"] == date].copy()
    latest = latest.dropna(subset=["momentum_60"])
    return latest


def weight_nonneg(values: pd.Series) -> pd.Series:
    weights = values.clip(lower=0)
    if weights.sum() <= 0:
        return pd.Series([1 / len(values)] * len(values), index=values.index)
    return weights / weights.sum()


def forward_return(prices: pd.DataFrame, date: pd.Timestamp, tickers: list, horizon: int) -> float:
    df = prices[prices["ticker"].isin(tickers)].copy()
    df = df.sort_values(["ticker", "date"])
    df["future_close"] = df.groupby("ticker")["close"].shift(-horizon)
    current = df[df["date"] == date].copy()
    if current.empty:
        return 0.0
    current = current.dropna(subset=["future_close"])
    if current.empty:
        return 0.0
    assert (df[df["date"] > date]["date"].min() > date) if not df[df["date"] > date].empty else True
    ret = (current["future_close"] - current["close"]) / current["close"]
    return float(ret.mean())


def main() -> None:
    conf = yaml.safe_load(Path("specpack/snapshot_replay/assertions.yaml").read_text(encoding="utf-8"))
    cmd_template = conf["run"]["cmd"]
    snapshot_as_of = conf["run"]["as_of"]

    prices = load_snapshot(snapshot_as_of)
    all_dates = sorted(prices["date"].unique())
    candidates = []
    for idx, d in enumerate(all_dates):
        if idx + 1 < MIN_ENHANCED_HISTORY:
            continue
        if idx + max(HORIZONS) >= len(all_dates):
            continue
        candidates.append(d)

    selected_dates = []
    failures = []
    for d in reversed(candidates):
        output_path = Path("outputs") / f"report_{d.strftime('%Y-%m-%d')}_top{TOP_N}.json"
        if output_path.exists():
            output_path.unlink()
        cache_dir = Path(".cache")
        if cache_dir.exists():
            subprocess.call("rm -rf .cache", shell=True)

        tokens = shlex.split(cmd_template)
        filtered = []
        skip_next = False
        for token in tokens:
            if skip_next:
                skip_next = False
                continue
            if token in ("--date", "--top", "--output-json"):
                skip_next = True
                continue
            filtered.append(token)
        filtered.extend(
            [
                "--date",
                d.strftime("%Y-%m-%d"),
                "--top",
                str(TOP_N),
            ]
        )
        cmd = " ".join(shlex.quote(tok) for tok in filtered)
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            raise SystemExit(ret)
        if not output_path.exists():
            failures.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "exists": False,
                    "len_results": 0,
                    "data_date": None,
                    "issues": None,
                }
            )
            continue
        report = json.loads(output_path.read_text(encoding="utf-8"))
        if len(report.get("results", [])) < TOP_N:
            first = report.get("results", [{}])[0] if report.get("results") else {}
            failures.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "exists": True,
                    "len_results": len(report.get("results", [])),
                    "data_date": first.get("data_date"),
                    "issues": report.get("issues"),
                }
            )
            continue
        selected_dates.append(d)
        if len(selected_dates) >= N_DATES:
            break

    if len(selected_dates) < N_DATES:
        summary = ", ".join(
            [
                f"{f['date']}:exists={f['exists']},len={f['len_results']},"
                f"data_date={f['data_date']},issues={f['issues']}"
                for f in failures[:3]
            ]
        )
        raise AssertionError(
            "insufficient enhanced dates: need {need}, got {got}; "
            "len(all_dates)={total}, min_history={min_history}, max_horizon={max_horizon}, "
            "candidate_count={candidate_count}, selected_count={selected_count}; "
            "failures={failures}".format(
                need=N_DATES,
                got=len(selected_dates),
                total=len(all_dates),
                min_history=MIN_ENHANCED_HISTORY,
                max_horizon=max(HORIZONS),
                candidate_count=len(candidates),
                selected_count=len(selected_dates),
                failures=summary,
            )
        )

    selected_dates = list(sorted(selected_dates))

    results = []
    for d in selected_dates:
        momentum = compute_momentum60(prices, d)
        top = momentum.sort_values("momentum_60", ascending=False).head(TOP_N)
        baseline_tickers = top["ticker"].tolist()
        baseline_weights = weight_nonneg(top["momentum_60"])

        output_path = Path("outputs") / f"report_{d.strftime('%Y-%m-%d')}_top{TOP_N}.json"
        report = json.loads(output_path.read_text(encoding="utf-8"))
        enhanced_rows = report.get("results", [])
        enhanced_tickers = [row["ticker"] for row in enhanced_rows]
        scores = pd.Series([row.get("final_score", 0.0) for row in enhanced_rows])
        enhanced_weights = weight_nonneg(scores)

        if len(baseline_tickers) != TOP_N or len(enhanced_tickers) != TOP_N:
            raise AssertionError("selection size invalid")

        horizons_data = {}
        for horizon in HORIZONS:
            base_ret = forward_return(prices, d, baseline_tickers, horizon)
            enh_ret = forward_return(prices, d, enhanced_tickers, horizon)
            horizons_data[str(horizon)] = {
                "baseline_return": round(base_ret, 8),
                "enhanced_return": round(enh_ret, 8),
            }

        results.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "baseline": {
                    "tickers": baseline_tickers,
                    "weights": [round(float(w), 8) for w in baseline_weights.tolist()],
                },
                "enhanced": {
                    "tickers": enhanced_tickers,
                    "weights": [round(float(w), 8) for w in enhanced_weights.tolist()],
                },
                "horizons": horizons_data,
            }
        )

    output = {
        "snapshot_as_of": snapshot_as_of,
        "dates": [r["date"] for r in results],
        "results": results,
        "config": {"n_dates": N_DATES, "horizons": HORIZONS, "top_n": TOP_N},
    }

    output_path = Path("outputs") / f"backtest_regression_{snapshot_as_of}.json"
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(f"[backtest_regression] wrote {output_path}")


if __name__ == "__main__":
    main()
