from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import getpass
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set

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

    @contextmanager
    def _akshare_network_context(self):
        """By default bypass system proxies to avoid host-level proxy injection on macOS."""
        if os.getenv("AKSHARE_USE_SYSTEM_PROXY", "0") == "1":
            yield
            return

        keys = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "NO_PROXY", "no_proxy")
        old = {k: os.environ.get(k) for k in keys}
        try:
            for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
                os.environ.pop(k, None)
            os.environ["NO_PROXY"] = "*"
            os.environ["no_proxy"] = "*"
            yield
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    @staticmethod
    def _normalize_akshare_symbol(raw: object) -> str:
        text = str(raw or "").strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 6:
            return digits[-6:]
        return normalize_ticker(text)

    @staticmethod
    def _normalize_snapshot_bridge_ticker(raw: object, valid_codes: Optional[Set[str]]) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        if text.isdigit():
            code = text.zfill(6)
            if valid_codes is None or code in valid_codes:
                return code
            return ""
        if len(text) >= 2 and text[0].isalpha() and text[1:].isdigit():
            code = text[1:].zfill(6)
            if valid_codes is None or code in valid_codes:
                return code
            return ""
        return ""

    @staticmethod
    def _build_code_name_map(codes_df: Optional[pd.DataFrame]) -> Dict[str, str]:
        if codes_df is None or codes_df.empty:
            return {}
        mapping: Dict[str, str] = {}
        for _, row in codes_df.iterrows():
            ticker = AkshareProvider._normalize_akshare_symbol(row.get("代码", row.get("code", "")))
            name = str(row.get("名称", row.get("name", ""))).strip()
            if not ticker:
                continue
            if not name or name.lower() in {"nan", "none", "null"}:
                continue
            mapping[ticker] = name
        return mapping

    @staticmethod
    def _is_placeholder_name(name: str, ticker: str) -> bool:
        text = str(name or "").strip()
        if not text:
            return True
        if text.lower() in {"nan", "none", "null"}:
            return True
        if text.upper().startswith("STOCK_"):
            return True
        if text == str(ticker or "").strip():
            return True
        return False

    def _resolve_name(self, row: pd.Series, ticker: str, code_name_map: Dict[str, str]) -> str:
        for key in ("名称", "name", "股票简称"):
            candidate = str(row.get(key, "")).strip()
            if not self._is_placeholder_name(candidate, ticker):
                return candidate
        fallback = str(code_name_map.get(ticker, "")).strip()
        if fallback:
            return fallback
        for key in ("名称", "name", "股票简称"):
            candidate = str(row.get(key, "")).strip()
            if candidate and candidate.lower() not in {"nan", "none", "null"}:
                return candidate
        return ticker

    @staticmethod
    def _to_daily_symbol(symbol: str) -> str:
        if symbol.startswith(("4", "8", "9")):
            return f"bj{symbol}"
        if symbol.startswith(("5", "6", "9")):
            return f"sh{symbol}"
        return f"sz{symbol}"

    def _fetch_spot(self, ak):
        try:
            return self._retry(lambda: ak.stock_zh_a_spot_em())
        except Exception:
            return self._retry(lambda: ak.stock_zh_a_spot())

    def _fetch_code_name(self, ak):
        df = self._retry(lambda: ak.stock_info_a_code_name())
        rename_map = {}
        if "code" in df.columns:
            rename_map["code"] = "代码"
        if "name" in df.columns:
            rename_map["name"] = "名称"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _snapshot_membership_universe(
        self,
        industries: List[str],
        valid_codes: Optional[Set[str]] = None,
        code_name_map: Optional[Dict[str, str]] = None,
    ) -> List[StockInfo]:
        name_map = code_name_map or {}
        root_dir = Path(__file__).resolve().parent.parent
        snapshots_root = root_dir / "data" / "snapshots"
        if not snapshots_root.exists():
            return []
        membership_paths = sorted(snapshots_root.glob("*/concept_membership.csv"), reverse=True)
        if not membership_paths:
            return []
        best_df = None
        best_source = ""
        for path in membership_paths:
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            required = {"ticker", "name", "concept", "industry"}
            if not required.issubset(df.columns):
                continue
            df = df.copy()
            df["ticker"] = df["ticker"].map(
                lambda x: self._normalize_snapshot_bridge_ticker(x, valid_codes)
            )
            df["concept"] = df["concept"].astype(str).fillna("")
            df["industry"] = df["industry"].astype(str).fillna("")
            df["name"] = df["name"].astype(str).fillna("")
            df = df[df["ticker"].astype(str).str.fullmatch(r"\d{6}")]
            if valid_codes is not None:
                df = df[df["ticker"].isin(valid_codes)]
            if industries:
                allowed = set(str(x) for x in industries if x)
                df = df[df["concept"].isin(allowed) | df["industry"].isin(allowed)]
            if df.empty:
                continue
            df = df.drop_duplicates(subset=["ticker"], keep="first")
            if best_df is None or len(df) > len(best_df):
                best_df = df
                best_source = path.parent.name

        if best_df is None or best_df.empty:
            return []
        try:
            limit = int(os.getenv("AKSHARE_UNIVERSE_LIMIT", "80"))
        except ValueError:
            limit = 80
        if limit > 0 and len(best_df) > limit:
            best_df = best_df.head(limit)
        universe: List[StockInfo] = []
        for _, row in best_df.iterrows():
            ticker = str(row["ticker"])
            name = self._resolve_name(row, ticker, name_map)
            universe.append(
                StockInfo(
                    ticker=ticker,
                    name=name,
                    industry=str(row.get("industry", "")),
                    concept=str(row.get("concept", "")),
                    description=f"snapshot_membership_bridge:{best_source}",
                )
            )
        return universe

    def get_stock_universe(self, industries: List[str]) -> List[StockInfo]:
        try:
            import akshare as ak  # noqa: F401
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("akshare not available") from exc

        with self._akshare_network_context():
            valid_codes: Optional[Set[str]] = None
            codes_df: Optional[pd.DataFrame] = None
            code_name_map: Dict[str, str] = {}
            try:
                codes_df = self._fetch_code_name(ak)
                code_name_map = self._build_code_name_map(codes_df)
                if "代码" in codes_df.columns:
                    valid_codes = set(codes_df["代码"].astype(str).str.zfill(6).tolist())
            except Exception:
                valid_codes = None
                code_name_map = {}

            if not industries:
                try:
                    spot = self._fetch_spot(ak)
                except Exception:
                    spot = codes_df if codes_df is not None else self._fetch_code_name(ak)
                universe = []
                for _, row in spot.iterrows():
                    ticker = self._normalize_akshare_symbol(
                        row.get("代码", row.get("code", row.get("symbol", row.get("证券代码", ""))))
                    )
                    if not ticker:
                        continue
                    universe.append(
                        StockInfo(
                            ticker=ticker,
                            name=self._resolve_name(row, ticker, code_name_map),
                            industry="",
                            concept="",
                            description="",
                        )
                    )
                return universe

            concept_set = set()
            industry_set = set()
            concept_catalog_available = False
            try:
                concept_names = ak.stock_board_concept_name_em()
                industry_names = ak.stock_board_industry_name_em()
                concept_set = set(concept_names["板块名称"].astype(str).tolist())
                industry_set = set(industry_names["板块名称"].astype(str).tolist())
                concept_catalog_available = True
            except Exception:
                # In some networks, EastMoney concept endpoints are blocked while quotes/history still work.
                concept_set = set()
                industry_set = set()
                concept_catalog_available = False

            if not concept_catalog_available:
                snapshot_universe = self._snapshot_membership_universe(
                    industries, valid_codes=valid_codes, code_name_map=code_name_map
                )
                if snapshot_universe:
                    return snapshot_universe

            universe_map = {}
            for name in industries:
                if name in concept_set:
                    df = self._retry(lambda: ak.stock_board_concept_cons_em(symbol=name))
                    self._sleep()
                    for _, row in df.iterrows():
                        ticker = self._normalize_akshare_symbol(row.get("代码", row.get("code", "")))
                        if not ticker:
                            continue
                        universe_map[ticker] = StockInfo(
                            ticker=ticker,
                            name=self._resolve_name(row, ticker, code_name_map),
                            industry=name,
                            concept=name,
                            description="",
                        )
                elif name in industry_set:
                    df = self._retry(lambda: ak.stock_board_industry_cons_em(symbol=name))
                    self._sleep()
                    for _, row in df.iterrows():
                        ticker = self._normalize_akshare_symbol(row.get("代码", row.get("code", "")))
                        if not ticker:
                            continue
                        universe_map[ticker] = StockInfo(
                            ticker=ticker,
                            name=self._resolve_name(row, ticker, code_name_map),
                            industry=name,
                            concept=name,
                            description="",
                        )

            if universe_map:
                return list(universe_map.values())

            snapshot_universe = self._snapshot_membership_universe(
                industries, valid_codes=valid_codes, code_name_map=code_name_map
            )
            if snapshot_universe:
                return snapshot_universe

            try:
                spot = self._fetch_spot(ak)
            except Exception:
                spot = codes_df if codes_df is not None else self._fetch_code_name(ak)
            keywords = [kw for kw in industries if kw]
            if keywords:
                mask = pd.Series(False, index=spot.index)
                for kw in keywords:
                    mask = mask | spot["名称"].astype(str).str.contains(kw, na=False)
                spot = spot[mask]
            if spot.empty:
                try:
                    spot = self._fetch_spot(ak).head(200)
                except Exception:
                    spot = self._fetch_code_name(ak).head(200)
            universe = []
            for _, row in spot.iterrows():
                ticker = self._normalize_akshare_symbol(
                    row.get("代码", row.get("code", row.get("symbol", row.get("证券代码", ""))))
                )
                if not ticker:
                    continue
                universe.append(
                    StockInfo(
                        ticker=ticker,
                        name=self._resolve_name(row, ticker, code_name_map),
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

        with self._akshare_network_context():
            for ticker, stock in stock_lookup.items():
                symbol = self._normalize_akshare_symbol(ticker)
                cache_path = self._cache_path(symbol, end_date)
                if cache_path.exists():
                    df = pd.read_csv(cache_path)
                    df["date"] = pd.to_datetime(df["date"])
                else:

                    def _fetch():
                        return ak.stock_zh_a_hist(
                            symbol=symbol,
                            period="daily",
                            start_date=start_date,
                            end_date=end_date_str,
                            adjust="",
                        )

                    try:
                        df = self._retry(_fetch)
                        self._sleep()
                    except Exception:
                        df = pd.DataFrame()

                    if not df.empty:
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
                    else:
                        try:
                            daily_symbol = self._to_daily_symbol(symbol)
                            df = self._retry(lambda: ak.stock_zh_a_daily(symbol=daily_symbol))
                            self._sleep()
                            if df.empty:
                                continue
                            needed = ["date", "open", "close", "high", "low", "volume"]
                            if not set(needed).issubset(df.columns):
                                continue
                            df = df[needed]
                        except Exception:
                            continue

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
