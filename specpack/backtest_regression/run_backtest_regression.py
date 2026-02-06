import json
import shlex
import subprocess
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import yaml

N_DATES = 30
HORIZONS = [1, 5, 20]
TOP_N = 5
MIN_ENHANCED_HISTORY = 80
ALPHA_LAYER_SHARES = (0.5, 0.3, 0.2)
ALPHA_MAX_SINGLE_WEIGHT = 0.55
ALPHA_OBJECTIVE = {
    "excess_return_weight": 1.0,
    "drawdown_penalty_weight": 0.35,
    "turnover_penalty_weight": 0.10,
    "max_drawdown_constraint": 0.25,
}


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


def _layer_ranges(n_positions: int) -> list[tuple[int, int]]:
    if n_positions <= 0:
        return []
    if n_positions == 1:
        return [(0, 1)]
    if n_positions == 2:
        return [(0, 1), (1, 2)]
    if n_positions == 3:
        return [(0, 1), (1, 2), (2, 3)]
    if n_positions == 4:
        return [(0, 1), (1, 3), (3, 4)]
    return [(0, 1), (1, 3), (3, n_positions)]


def _normalize_shares(raw_shares: Sequence[float]) -> list[float]:
    values = [float(x) for x in raw_shares if float(x) > 0]
    if not values:
        return []
    total = sum(values)
    if total <= 0:
        return []
    return [x / total for x in values]


def _apply_weight_cap(weights: pd.Series, max_single_weight: float) -> pd.Series:
    capped = weights.copy()
    for _ in range(8):
        excess = (capped - max_single_weight).clip(lower=0).sum()
        if excess <= 1e-12:
            break
        capped = capped.clip(upper=max_single_weight)
        room = (max_single_weight - capped).clip(lower=0)
        room_sum = room.sum()
        if room_sum <= 1e-12:
            break
        capped = capped + (room / room_sum) * excess
    total = capped.sum()
    if total <= 1e-12:
        return pd.Series([1.0 / len(capped)] * len(capped), index=capped.index)
    return capped / total


def layered_score_weights(scores: pd.Series) -> pd.Series:
    if len(scores) == 0:
        return scores.copy()
    nonneg_scores = scores.astype(float).clip(lower=0)
    weights = pd.Series(0.0, index=scores.index, dtype=float)
    ranges = _layer_ranges(len(scores))
    shares = _normalize_shares(ALPHA_LAYER_SHARES[: len(ranges)])
    for layer_share, (start, end) in zip(shares, ranges):
        segment = nonneg_scores.iloc[start:end]
        if segment.empty:
            continue
        if float(segment.sum()) <= 1e-12:
            local = pd.Series([1.0 / len(segment)] * len(segment), index=segment.index)
        else:
            local = segment / float(segment.sum())
        weights.loc[segment.index] = local * float(layer_share)

    if float(weights.sum()) <= 1e-12:
        return pd.Series([1.0 / len(scores)] * len(scores), index=scores.index)

    weights = _apply_weight_cap(weights, ALPHA_MAX_SINGLE_WEIGHT)
    return weights / float(weights.sum())


def forward_return(
    prices: pd.DataFrame,
    date: pd.Timestamp,
    tickers: list,
    horizon: int,
    weights: Optional[Sequence[float]] = None,
) -> float:
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
    current["forward_return"] = (current["future_close"] - current["close"]) / current["close"]

    if weights is None:
        return float(current["forward_return"].mean())

    weight_map = {}
    for ticker, weight in zip(tickers, weights):
        weight_map[str(ticker)] = float(weight)
    current["weight"] = current["ticker"].map(lambda x: weight_map.get(str(x), 0.0))
    current = current[current["weight"] > 0]
    if current.empty:
        return 0.0
    weight_sum = float(current["weight"].sum())
    if weight_sum <= 1e-12:
        return float(current["forward_return"].mean())
    return float((current["forward_return"] * current["weight"] / weight_sum).sum())


def portfolio_turnover(
    prev_tickers: Optional[Sequence[str]],
    prev_weights: Optional[Sequence[float]],
    curr_tickers: Sequence[str],
    curr_weights: Sequence[float],
) -> Optional[float]:
    if prev_tickers is None or prev_weights is None:
        return None

    prev_map = {str(t): float(w) for t, w in zip(prev_tickers, prev_weights)}
    curr_map = {str(t): float(w) for t, w in zip(curr_tickers, curr_weights)}
    all_tickers = set(prev_map.keys()) | set(curr_map.keys())
    if not all_tickers:
        return 0.0
    turnover = 0.5 * sum(abs(prev_map.get(t, 0.0) - curr_map.get(t, 0.0)) for t in all_tickers)
    return float(turnover)


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
    prev_baseline_tickers: Optional[list[str]] = None
    prev_baseline_weights: Optional[list[float]] = None
    prev_enhanced_tickers: Optional[list[str]] = None
    prev_enhanced_weights: Optional[list[float]] = None
    for d in selected_dates:
        momentum = compute_momentum60(prices, d)
        top = momentum.sort_values("momentum_60", ascending=False).head(TOP_N)
        baseline_tickers = top["ticker"].tolist()
        baseline_weights = weight_nonneg(top["momentum_60"])

        output_path = Path("outputs") / f"report_{d.strftime('%Y-%m-%d')}_top{TOP_N}.json"
        report = json.loads(output_path.read_text(encoding="utf-8"))
        enhanced_rows = report.get("results", [])
        enhanced_tickers = [row["ticker"] for row in enhanced_rows]
        scores = pd.Series(
            [float(row.get("final_score", 0.0)) for row in enhanced_rows],
            index=enhanced_tickers,
        )
        enhanced_weights = layered_score_weights(scores)

        if len(baseline_tickers) != TOP_N or len(enhanced_tickers) != TOP_N:
            raise AssertionError("selection size invalid")

        baseline_turnover = portfolio_turnover(
            prev_baseline_tickers,
            prev_baseline_weights,
            baseline_tickers,
            baseline_weights.tolist(),
        )
        enhanced_turnover = portfolio_turnover(
            prev_enhanced_tickers,
            prev_enhanced_weights,
            enhanced_tickers,
            enhanced_weights.tolist(),
        )

        horizons_data = {}
        for horizon in HORIZONS:
            base_ret = forward_return(
                prices,
                d,
                baseline_tickers,
                horizon,
                baseline_weights.tolist(),
            )
            enh_ret = forward_return(
                prices,
                d,
                enhanced_tickers,
                horizon,
                enhanced_weights.tolist(),
            )
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
                    "weighting_mode": "momentum_nonneg",
                    "turnover": (
                        round(float(baseline_turnover), 8)
                        if baseline_turnover is not None
                        else None
                    ),
                },
                "enhanced": {
                    "tickers": enhanced_tickers,
                    "weights": [round(float(w), 8) for w in enhanced_weights.tolist()],
                    "weighting_mode": "layered_score",
                    "turnover": (
                        round(float(enhanced_turnover), 8)
                        if enhanced_turnover is not None
                        else None
                    ),
                },
                "horizons": horizons_data,
            }
        )
        prev_baseline_tickers = baseline_tickers
        prev_baseline_weights = baseline_weights.tolist()
        prev_enhanced_tickers = enhanced_tickers
        prev_enhanced_weights = enhanced_weights.tolist()

    output = {
        "snapshot_as_of": snapshot_as_of,
        "dates": [r["date"] for r in results],
        "results": results,
        "config": {
            "n_dates": N_DATES,
            "horizons": HORIZONS,
            "top_n": TOP_N,
            "portfolio": {
                "baseline_weighting": "momentum_nonneg",
                "enhanced_weighting": "layered_score",
                "layer_shares": list(ALPHA_LAYER_SHARES),
                "max_single_weight": ALPHA_MAX_SINGLE_WEIGHT,
            },
            "alpha_objective": ALPHA_OBJECTIVE,
        },
    }

    output_path = Path("outputs") / f"backtest_regression_{snapshot_as_of}.json"
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    print(f"[backtest_regression] wrote {output_path}")


if __name__ == "__main__":
    main()
