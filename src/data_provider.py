from __future__ import annotations

from dataclasses import dataclass
import getpass
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Iterable, List, Optional

import numpy as np
import pandas as pd
import time

from .utils import stable_hash, trading_calendar


def normalize_ticker(x) -> str:
    s = str(x).strip()
    return s.zfill(6) if s.isdigit() else s


def _io_debug_exit(path: Path, exc: Exception) -> None:
    print(
        "[io] error={error} path={path}".format(error=type(exc).__name__, path=path),
        file=sys.stderr,
    )
    print(
        "[io] pwd={pwd} user={user}".format(pwd=os.getcwd(), user=getpass.getuser()),
        file=sys.stderr,
    )
    try:
        subprocess.run(
            ["ls", "-leO@", str(path)],
            check=False,
            stdout=sys.stderr,
            stderr=sys.stderr,
        )
    except Exception as ls_exc:  # noqa: BLE001
        print(f"[io] ls_failed={ls_exc}", file=sys.stderr)
    sys.exit(1)


@dataclass(frozen=True)
class StockInfo:
    ticker: str
    name: str
    industry: str
    concept: str
    description: str


class DataProvider:
    name = "base"

    def get_stock_universe(self, industries: List[str]) -> List[StockInfo]:
        raise NotImplementedError

    def get_price_history(
        self,
        stocks: Iterable[StockInfo],
        end_date: pd.Timestamp,
        lookback_days: int,
        seed: int,
    ) -> pd.DataFrame:
        raise NotImplementedError


class LocalMockProvider(DataProvider):
    name = "mock"

    def get_stock_universe(self, industries: List[str]) -> List[StockInfo]:
        # Deterministic universe sized to the industry list.
        universe = []
        total = max(60, len(industries) * 5)
        for i in range(total):
            industry = industries[i % len(industries)]
            ticker = f"A{i:04d}"
            universe.append(
                StockInfo(
                    ticker=ticker,
                    name=f"STOCK_{i:04d}",
                    industry=industry,
                    concept=industry,
                    description="",
                )
            )
        return universe

    def get_price_history(
        self,
        stocks: Iterable[StockInfo],
        end_date: pd.Timestamp,
        lookback_days: int,
        seed: int,
    ) -> pd.DataFrame:
        dates = trading_calendar(end_date, lookback_days)
        records = []
        for idx, stock in enumerate(stocks):
            rng = np.random.RandomState(seed + idx)
            base_price = 10 + rng.rand() * 50
            daily_returns = rng.normal(loc=0.0005, scale=0.02, size=len(dates))
            prices = base_price * (1 + daily_returns).cumprod()
            volume = rng.randint(1_000_000, 50_000_000, size=len(dates))
            for d, p, v in zip(dates, prices, volume):
                records.append(
                    {
                        "date": d,
                        "ticker": stock.ticker,
                        "name": stock.name,
                        "industry": stock.industry,
                        "concept": stock.concept,
                        "description": stock.description,
                        "close": float(round(p, 4)),
                        "volume": int(v),
                    }
                )
        return pd.DataFrame.from_records(records)


class AkshareProvider(DataProvider):
    name = "akshare"

    def __init__(
        self,
        cache_dir: str = ".cache/akshare",
        rate_limit: float = 0.4,
        retries: int = 3,
        backoff: float = 1.8,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.rate_limit = rate_limit
        self.retries = retries
        self.backoff = backoff

    def _sleep(self) -> None:
        time.sleep(self.rate_limit)

    def _retry(self, func: Callable):
        last_exc = None
        for attempt in range(self.retries):
            try:
                return func()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                time.sleep(self.rate_limit * (self.backoff**attempt))
        raise last_exc

    def _cache_path(self, ticker: str, as_of: pd.Timestamp) -> Path:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        key = stable_hash([ticker, as_of.strftime("%Y-%m-%d")])
        return self.cache_dir / f"{ticker}_{key}.csv"

    def get_stock_universe(self, industries: List[str]) -> List[StockInfo]:
        try:
            import akshare as ak  # noqa: F401
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("akshare not available") from exc

        if not industries:
            spot = ak.stock_zh_a_spot_em()
            universe = []
            for _, row in spot.iterrows():
                universe.append(
                    StockInfo(
                        ticker=str(row.get("代码", "")),
                        name=str(row.get("名称", "")),
                        industry="",
                        concept="",
                        description="",
                    )
                )
            return universe

        concept_names = ak.stock_board_concept_name_em()
        industry_names = ak.stock_board_industry_name_em()
        concept_set = set(concept_names["板块名称"].astype(str).tolist())
        industry_set = set(industry_names["板块名称"].astype(str).tolist())

        universe_map = {}
        for name in industries:
            if name in concept_set:
                df = self._retry(lambda: ak.stock_board_concept_cons_em(symbol=name))
                self._sleep()
                for _, row in df.iterrows():
                    ticker = str(row.get("代码", row.get("code", "")))
                    universe_map[ticker] = StockInfo(
                        ticker=ticker,
                        name=str(row.get("名称", row.get("name", ticker))),
                        industry=name,
                        concept=name,
                        description="",
                    )
            elif name in industry_set:
                df = self._retry(lambda: ak.stock_board_industry_cons_em(symbol=name))
                self._sleep()
                for _, row in df.iterrows():
                    ticker = str(row.get("代码", row.get("code", "")))
                    universe_map[ticker] = StockInfo(
                        ticker=ticker,
                        name=str(row.get("名称", row.get("name", ticker))),
                        industry=name,
                        concept=name,
                        description="",
                    )

        if universe_map:
            return list(universe_map.values())

        spot = ak.stock_zh_a_spot_em()
        keywords = [kw for kw in industries if kw]
        if keywords:
            mask = pd.Series(False, index=spot.index)
            for kw in keywords:
                mask = mask | spot["名称"].astype(str).str.contains(kw, na=False)
            spot = spot[mask]
        if spot.empty:
            spot = ak.stock_zh_a_spot_em().head(200)
        universe = []
        for _, row in spot.iterrows():
            universe.append(
                StockInfo(
                    ticker=str(row.get("代码", "")),
                    name=str(row.get("名称", "")),
                    industry="",
                    concept="",
                    description="",
                )
            )
        return universe

    def get_price_history(
        self,
        stocks: Iterable[StockInfo],
        end_date: pd.Timestamp,
        lookback_days: int,
        seed: int,
    ) -> pd.DataFrame:
        try:
            import akshare as ak  # noqa: F401
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("akshare not available") from exc

        records = []
        stock_lookup = {stock.ticker: stock for stock in stocks}
        start_date = (end_date - pd.Timedelta(days=lookback_days * 2)).strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        for ticker, stock in stock_lookup.items():
            cache_path = self._cache_path(ticker, end_date)
            if cache_path.exists():
                df = pd.read_csv(cache_path)
                df["date"] = pd.to_datetime(df["date"])
            else:
                def _fetch():
                    return ak.stock_zh_a_hist(
                        symbol=ticker,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date_str,
                        adjust="",
                    )

                df = self._retry(_fetch)
                self._sleep()
                if df.empty:
                    continue
                df = df.rename(
                    columns={
                        "日期": "date",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                    }
                )
                df = df[["date", "open", "close", "high", "low", "volume"]]
                df["date"] = pd.to_datetime(df["date"])
                df.to_csv(cache_path, index=False)

            df = df[df["date"] <= end_date].sort_values("date")
            if len(df) < lookback_days:
                continue
            df = df.tail(lookback_days)
            df = df.assign(
                ticker=ticker,
                name=stock.name,
                industry=stock.industry,
                concept=stock.concept,
                description=stock.description,
            )
            records.append(df)

        if not records:
            return pd.DataFrame(
                columns=[
                    "date",
                    "ticker",
                    "name",
                    "industry",
                    "concept",
                    "description",
                    "open",
                    "close",
                    "high",
                    "low",
                    "volume",
                ]
            )
        merged = pd.concat(records, ignore_index=True)
        return merged


class SnapshotProvider(DataProvider):
    name = "snapshot"

    def __init__(
        self,
        as_of: Optional[pd.Timestamp] = None,
        snapshot_as_of: Optional[pd.Timestamp] = None,
        base_dir: str = "data/snapshots",
    ) -> None:
        self.as_of = as_of
        self.snapshot_as_of = snapshot_as_of
        self.base_dir = Path(base_dir)

    def _available_snapshots(self) -> List[str]:
        if not self.base_dir.exists():
            return []
        return sorted([p.name for p in self.base_dir.iterdir() if p.is_dir()])

    def _snapshot_dir(self, as_of: pd.Timestamp) -> Path:
        return self.base_dir / as_of.strftime("%Y-%m-%d")

    def _load_membership(self, as_of: pd.Timestamp) -> pd.DataFrame:
        snapshot_dir = self._snapshot_dir(as_of)
        membership_path = snapshot_dir / "concept_membership.csv"
        if not membership_path.exists():
            _io_debug_exit(
                membership_path,
                FileNotFoundError(f"Missing concept_membership.csv: {membership_path}"),
            )
        try:
            df = pd.read_csv(
                membership_path,
                dtype={"ticker": str, "concept": str, "industry": str},
            )
        except (FileNotFoundError, PermissionError) as exc:
            _io_debug_exit(membership_path, exc)
        if len(df) == 0:
            raise ValueError(f"membership has 0 rows: {membership_path}")
        if "ticker" not in df.columns:
            raise ValueError(
                "membership missing join key column(s) ['ticker']; "
                f"columns={list(df.columns)}"
            )
        df["ticker"] = df["ticker"].map(normalize_ticker)
        df["concept"] = df.get("concept", "").astype(str).str.strip()
        df["industry"] = df.get("industry", df["concept"]).astype(str).str.strip()
        df["description"] = df.get("description", "").astype(str).str.strip()
        return df

    def _load_prices(self, as_of: pd.Timestamp) -> pd.DataFrame:
        snapshot_dir = self._snapshot_dir(as_of)
        path: Optional[Path] = None
        suffix = None
        for candidate in ("csv", "parquet"):
            candidate_path = snapshot_dir / f"prices.{candidate}"
            if candidate_path.exists():
                path = candidate_path
                suffix = candidate
                break
        if path is None or suffix is None:
            _io_debug_exit(
                snapshot_dir / "prices.csv",
                FileNotFoundError(f"Missing prices.csv or prices.parquet: {snapshot_dir}"),
            )
        try:
            if suffix == "csv":
                df = pd.read_csv(path, dtype={"ticker": str})
            else:
                df = pd.read_parquet(path)
        except (FileNotFoundError, PermissionError) as exc:
            _io_debug_exit(path, exc)
        if len(df) == 0:
            raise ValueError(f"prices has 0 rows: {path}")
        if "ticker" not in df.columns:
            raise ValueError(
                "prices missing join key column(s) ['ticker']; "
                f"columns={list(df.columns)}"
            )
        df["ticker"] = df["ticker"].map(normalize_ticker)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def get_stock_universe(self, industries: List[str]) -> List[StockInfo]:
        snapshot_date = self.snapshot_as_of or self.as_of
        if snapshot_date is None:
            raise ValueError("SnapshotProvider requires as_of date")
        membership = self._load_membership(snapshot_date)
        if industries:
            membership = membership[membership["concept"].isin(industries)]
        universe = []
        for _, row in membership.iterrows():
            universe.append(
                StockInfo(
                    ticker=str(row["ticker"]),
                    name=str(row.get("name", row["ticker"])),
                    industry=str(row.get("industry", row.get("concept", ""))),
                    concept=str(row.get("concept", "")),
                    description=str(row.get("description", "")),
                )
            )
        return universe

    def get_price_history(
        self,
        stocks: Iterable[StockInfo],
        end_date: pd.Timestamp,
        lookback_days: int,
        seed: int,
    ) -> pd.DataFrame:
        snapshot_date = self.snapshot_as_of or end_date
        prices = self._load_prices(snapshot_date)
        membership = self._load_membership(snapshot_date)
        tickers = {normalize_ticker(stock.ticker) for stock in stocks}
        prices = prices[prices["ticker"].isin(tickers)]
        prices = prices[prices["date"] <= end_date].sort_values(["ticker", "date"])
        prices = prices.groupby("ticker").tail(lookback_days)
        merged = prices.merge(membership, on="ticker", how="left")
        merged["name"] = merged.get("name", merged["ticker"])
        merged["industry"] = merged.get("industry", merged.get("concept", ""))
        merged["concept"] = merged.get("concept", merged["industry"])
        merged["description"] = merged.get("description", "")
        merged = merged[["date", "ticker", "name", "industry", "concept", "description", "close", "volume"]]
        return merged


def build_provider(
    name: str,
    as_of: Optional[pd.Timestamp] = None,
    snapshot_as_of: Optional[pd.Timestamp] = None,
) -> DataProvider:
    if name == "mock":
        return LocalMockProvider()
    if name == "akshare":
        return AkshareProvider()
    if name == "snapshot":
        return SnapshotProvider(as_of=as_of, snapshot_as_of=snapshot_as_of)
    raise ValueError(f"Unknown provider: {name}")


def provider_seed(date_str: str, signals_hash: str) -> int:
    return int(stable_hash([date_str, signals_hash])[:8], 16)
