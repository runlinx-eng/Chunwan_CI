#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

CONCEPT_SLEEP = 0.2
PRICE_SLEEP = 0.15
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.8


def retry_call(func, attempts: int = RETRY_ATTEMPTS, backoff: float = RETRY_BACKOFF):
    last_exc = None
    for attempt in range(attempts):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(0.5 * (backoff**attempt))
    raise last_exc


def normalize_ticker(value) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        return text.zfill(6)
    return text


def read_prices_schema(reference_path: Path) -> List[str]:
    with reference_path.open("r", encoding="utf-8") as f:
        header = f.readline().strip()
    return [col.strip() for col in header.split(",") if col.strip()]


def validate_schema(schema: List[str]) -> None:
    allowed = {"date", "ticker", "open", "close", "high", "low", "volume"}
    unknown = [col for col in schema if col not in allowed]
    if unknown:
        raise AssertionError(f"Unsupported schema columns: {unknown} (schema={schema})")


def map_price_columns(df: pd.DataFrame, schema: List[str]) -> pd.DataFrame:
    rename_map = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
    }
    df = df.rename(columns=rename_map)
    missing = [col for col in schema if col not in df.columns and col not in ("ticker",)]
    if missing:
        raise AssertionError(f"Missing columns after mapping: {missing} (schema={schema})")
    return df


def fetch_concepts(min_members: int, target_concepts: int) -> Tuple[List[str], List[Dict[str, str]]]:
    import akshare as ak

    concept_df = retry_call(lambda: ak.stock_board_concept_name_em())
    if "排名" in concept_df.columns:
        concept_df = concept_df.sort_values("排名")
    concept_df = concept_df.reset_index(drop=True)

    selected = []
    records: List[Dict[str, str]] = []
    for _, row in concept_df.iterrows():
        if len(selected) >= target_concepts:
            break
        name = str(row.get("板块名称", "")).strip()
        code = str(row.get("板块代码", "")).strip()
        if not name:
            continue

        def _fetch_by_code():
            return ak.stock_board_concept_cons_em(symbol=code)

        def _fetch_by_name():
            return ak.stock_board_concept_cons_em(symbol=name)

        try:
            cons = retry_call(_fetch_by_code)
        except Exception:
            cons = retry_call(_fetch_by_name)
        time.sleep(CONCEPT_SLEEP)

        ticker_col = None
        for col in ("代码", "code", "symbol"):
            if col in cons.columns:
                ticker_col = col
                break
        if ticker_col is None:
            continue

        tickers = [normalize_ticker(v) for v in cons[ticker_col].tolist()]
        tickers = [t for t in tickers if t]
        unique_tickers = sorted(set(tickers))
        if len(unique_tickers) < min_members:
            continue

        selected.append(name)
        for ticker in unique_tickers:
            records.append(
                {
                    "ticker": ticker,
                    "concept": name,
                    "industry": name,
                    "description": "eastmoney_concept",
                }
            )

    return selected, records


def fetch_prices(
    tickers: List[str],
    schema: List[str],
    as_of: datetime,
    min_bars: int,
    adjust: str,
) -> Tuple[pd.DataFrame, int]:
    import akshare as ak

    start_date = (as_of - timedelta(days=400)).strftime("%Y%m%d")
    end_date = as_of.strftime("%Y%m%d")
    frames = []
    dropped = 0

    for ticker in tickers:
        def _fetch():
            return ak.stock_zh_a_hist(
                symbol=ticker,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )

        df = retry_call(_fetch)
        time.sleep(PRICE_SLEEP)
        if df.empty:
            dropped += 1
            continue
        df = map_price_columns(df, schema)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df = df[df["date"] <= as_of.strftime("%Y-%m-%d")]
        df["ticker"] = ticker
        df = df[schema]
        df = df.dropna()
        if len(df) < min_bars:
            dropped += 1
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("No prices fetched")

    prices = pd.concat(frames, ignore_index=True)
    prices = prices.drop_duplicates(subset=["ticker", "date"], keep="first")
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    return prices, dropped


def main() -> None:
    parser = argparse.ArgumentParser(description="Build real snapshot from EastMoney")
    parser.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--n-concepts", type=int, default=10)
    parser.add_argument("--min-members", type=int, default=50)
    parser.add_argument("--min-bars", type=int, default=160)
    parser.add_argument("--max-tickers", type=int, default=600)
    parser.add_argument("--adjust", default="hfq", choices=["", "qfq", "hfq"])
    args = parser.parse_args()

    if args.n_concepts < 8:
        raise ValueError("n-concepts must be >= 8")

    as_of = datetime.strptime(args.as_of, "%Y-%m-%d")
    out_dir = Path(args.out_dir) if args.out_dir else Path("data/snapshots") / args.as_of
    out_dir.mkdir(parents=True, exist_ok=True)

    schema_path = Path("data/snapshots/2026-01-20/prices.csv")
    if not schema_path.exists():
        raise FileNotFoundError(f"missing schema reference: {schema_path}")
    schema = read_prices_schema(schema_path)
    validate_schema(schema)

    target_concepts = max(args.n_concepts, 8)
    selected_concepts, records = fetch_concepts(args.min_members, target_concepts)
    if len(selected_concepts) < target_concepts:
        raise RuntimeError(
            f"only {len(selected_concepts)} concepts collected (<{target_concepts}); "
            "try lower min-members or n-concepts"
        )

    concept_df = pd.DataFrame.from_records(records)
    concept_df = concept_df[["ticker", "concept", "industry", "description"]]
    concept_df = concept_df.sort_values(["concept", "ticker"]).reset_index(drop=True)

    unique_tickers = sorted(set(concept_df["ticker"].tolist()))
    if len(unique_tickers) > args.max_tickers:
        allowed = set(sorted(unique_tickers)[: args.max_tickers])
        concept_df = concept_df[concept_df["ticker"].isin(allowed)]
        unique_tickers = sorted(allowed)

    concepts_input = out_dir / "concepts_input.csv"
    concept_df.to_csv(concepts_input, index=False)

    prices, dropped = fetch_prices(unique_tickers, schema, as_of, args.min_bars, args.adjust)
    prices_path = out_dir / "prices.csv"
    prices.to_csv(prices_path, index=False)

    cmd = (
        f"python tools/ingest_concepts.py --as-of {args.as_of} "
        f"--prices-snapshot {prices_path} --concepts-input {concepts_input} "
        f"--out-dir {out_dir} --min-concept-members {args.min_members}"
    )
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        raise SystemExit(ret)

    manifest_path = out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    stats = manifest.get("stats", {})
    max_date = prices["date"].max()

    print(
        "summary: "
        f"unique_tickers={stats.get('unique_tickers')} "
        f"unique_concepts={stats.get('unique_concepts')} "
        f"min_concept_members={stats.get('min_concept_members')} "
        f"min_price_bars={stats.get('min_price_bars')} "
        f"data_date_max={max_date} "
        f"dropped_tickers={dropped}"
    )


if __name__ == "__main__":
    main()
