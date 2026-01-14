import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, List

import pandas as pd


def retry_call(func: Callable, attempts: int = 3, delay: float = 1.0, backoff: float = 1.8):
    last_exc = None
    for attempt in range(attempts):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(delay * (backoff ** attempt))
    raise last_exc


def rate_limit_sleep(seconds: float = 0.4) -> None:
    time.sleep(seconds)


def fetch_concepts_em(concept_names: List[str]) -> pd.DataFrame:
    import akshare as ak

    available = ak.stock_board_concept_name_em()
    available_names = set(available["板块名称"].tolist())
    missing = [name for name in concept_names if name not in available_names]
    if missing:
        raise ValueError(f"Concepts not found on EM: {missing}")

    rows = []
    for name in concept_names:
        df = retry_call(lambda: ak.stock_board_concept_cons_em(symbol=name))
        rate_limit_sleep()
        for _, row in df.iterrows():
            rows.append(
                {
                    "ticker": str(row.get("代码", row.get("code", ""))),
                    "name": str(row.get("名称", row.get("name", ""))),
                    "concept": name,
                    "industry": name,
                    "description": "",
                }
            )
    return pd.DataFrame(rows)


def fetch_concepts_ths(concept_names: List[str]) -> pd.DataFrame:
    import akshare as ak

    available = ak.stock_board_concept_name_ths()
    available_names = set(available["板块名称"].tolist())
    missing = [name for name in concept_names if name not in available_names]
    if missing:
        raise ValueError(f"Concepts not found on THS: {missing}")

    rows = []
    for name in concept_names:
        df = retry_call(lambda: ak.stock_board_concept_cons_ths(symbol=name))
        rate_limit_sleep()
        for _, row in df.iterrows():
            rows.append(
                {
                    "ticker": str(row.get("代码", row.get("code", ""))),
                    "name": str(row.get("名称", row.get("name", ""))),
                    "concept": name,
                    "industry": name,
                    "description": "",
                }
            )
    return pd.DataFrame(rows)


def fetch_prices(tickers: List[str], as_of: datetime) -> pd.DataFrame:
    import akshare as ak

    # Fetch longer window to ensure >=121 trading days available per ticker.
    start_date = (as_of - timedelta(days=450)).strftime("%Y%m%d")
    end_date = as_of.strftime("%Y%m%d")

    frames = []
    for ticker in tickers:
        def _fetch():
            return ak.stock_zh_a_hist(
                symbol=ticker,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="",
            )

        df = retry_call(_fetch)
        rate_limit_sleep()
        if df.empty:
            continue
        df = df.rename(columns={"日期": "date", "收盘": "close", "成交量": "volume"})
        df["ticker"] = ticker
        frames.append(df[["date", "ticker", "close", "volume"]])

    if not frames:
        return pd.DataFrame(columns=["date", "ticker", "close", "volume"])
    prices = pd.concat(frames, ignore_index=True)
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values(["ticker", "date"]).groupby("ticker").tail(200)
    return prices


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch snapshot data for concept screening")
    parser.add_argument("--as-of", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--source", choices=["em", "ths"], default="em", help="Data source")
    parser.add_argument("--concepts", nargs="+", required=True, help="Concept names to fetch")
    args = parser.parse_args()

    as_of = datetime.strptime(args.as_of, "%Y-%m-%d")
    concepts = args.concepts

    try:
        if args.source == "em":
            membership = fetch_concepts_em(concepts)
        else:
            membership = fetch_concepts_ths(concepts)
    except Exception:
        if args.source == "em":
            membership = fetch_concepts_ths(concepts)
        else:
            raise

    if membership.empty:
        raise RuntimeError("No concept members fetched")

    tickers = sorted(set(membership["ticker"].tolist()))
    prices = fetch_prices(tickers, as_of)
    if prices.empty:
        raise RuntimeError("No prices fetched")

    counts = prices.groupby("ticker").size()
    valid_tickers = counts[counts >= 121].index
    dropped = sorted(set(tickers) - set(valid_tickers))
    if dropped:
        print(f"Dropping {len(dropped)} tickers with insufficient history (need >=121).")
    membership = membership[membership["ticker"].isin(valid_tickers)]
    prices = prices[prices["ticker"].isin(valid_tickers)]

    if membership.empty or prices.empty:
        raise RuntimeError("No tickers with sufficient history (>=121 trading days).")

    snapshot_dir = Path("data/snapshots") / args.as_of
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    membership.to_csv(snapshot_dir / "concept_membership.csv", index=False)
    prices.to_csv(snapshot_dir / "prices.csv", index=False)

    print(f"Saved {len(membership)} membership rows and {len(prices)} price rows to {snapshot_dir}")


if __name__ == "__main__":
    main()
