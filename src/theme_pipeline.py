from dataclasses import dataclass
import getpass
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import re

import pandas as pd

from .data_provider import normalize_ticker
from .scoring import score_stocks
from .signals import Signal

ThemeMap = Dict[str, List[Dict[str, List[str]]]]
TERM_SPLIT_RE = re.compile(r"[,\uFF0C;\uFF1B、|\s]+")


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


def _split_terms(value: str) -> List[str]:
    return [item.strip() for item in TERM_SPLIT_RE.split(str(value)) if item.strip()]


def _terms_from_theme_map(theme_map: ThemeMap) -> Dict[str, Set[str]]:
    theme_terms: Dict[str, Set[str]] = {}
    for key, entries in theme_map.items():
        term_set = theme_terms.setdefault(str(key), set())
        for entry in entries:
            for raw in entry.get("values", []):
                for term in _split_terms(raw):
                    if term:
                        term_set.add(term)
    return theme_terms


def _terms_from_theme_map_csv(path: Path) -> Dict[str, Set[str]]:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if "主题名称" not in df.columns:
        return {}
    theme_terms: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        theme = str(row.get("主题名称", "")).strip()
        if not theme:
            continue
        raw_term = str(row.get("关键词", "")).strip()
        if not raw_term or raw_term.lower() == "nan":
            raw_term = str(row.get("对应行业/概念", "")).strip()
        if not raw_term or raw_term.lower() == "nan":
            continue
        terms = _split_terms(raw_term)
        if not terms:
            continue
        for term in terms:
            theme_terms.setdefault(theme, set()).add(term)
    return theme_terms


def _theme_map_stats_from_csv(path: Path) -> Tuple[Dict[str, Set[str]], Dict[str, object]]:
    if not path.exists():
        return {}, {
            "themes_in_map_count": 0,
            "terms_in_map_count": 0,
            "rows_in_map_count": 0,
            "sample_themes": [],
            "sample_terms": [],
        }
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}, {
            "themes_in_map_count": 0,
            "terms_in_map_count": 0,
            "rows_in_map_count": 0,
            "sample_themes": [],
            "sample_terms": [],
        }
    rows_in_map_count = int(len(df))
    if "主题名称" not in df.columns:
        return {}, {
            "themes_in_map_count": 0,
            "terms_in_map_count": 0,
            "rows_in_map_count": rows_in_map_count,
            "sample_themes": [],
            "sample_terms": [],
        }

    theme_terms: Dict[str, Set[str]] = {}
    themes_seen: List[str] = []
    terms_seen: List[str] = []
    themes_set: Set[str] = set()
    terms_set: Set[str] = set()

    for _, row in df.iterrows():
        theme = str(row.get("主题名称", "")).strip()
        if theme:
            if theme not in themes_set:
                themes_set.add(theme)
                themes_seen.append(theme)
            theme_terms.setdefault(theme, set())
        raw_term = str(row.get("关键词", "")).strip()
        if not raw_term or raw_term.lower() == "nan":
            raw_term = str(row.get("对应行业/概念", "")).strip()
        if not raw_term or raw_term.lower() == "nan":
            continue
        terms = _split_terms(raw_term)
        if not terms:
            continue
        for term in terms:
            if term not in terms_set:
                terms_set.add(term)
                terms_seen.append(term)
            if theme:
                theme_terms.setdefault(theme, set()).add(term)

    stats = {
        "themes_in_map_count": len(themes_set),
        "terms_in_map_count": len(terms_set),
        "rows_in_map_count": rows_in_map_count,
        "sample_themes": themes_seen[:5],
        "sample_terms": terms_seen[:10],
    }
    return theme_terms, stats


def _theme_map_stats_from_map(theme_map: ThemeMap) -> Tuple[Dict[str, Set[str]], Dict[str, object]]:
    theme_terms = _terms_from_theme_map(theme_map)
    themes = sorted(theme_terms.keys())
    terms_set: Set[str] = set()
    terms_seen: List[str] = []
    for _, terms in theme_terms.items():
        for term in sorted(terms):
            if term not in terms_set:
                terms_set.add(term)
                terms_seen.append(term)
    rows_in_map_count = sum(len(entries) for entries in theme_map.values())
    stats = {
        "themes_in_map_count": len(themes),
        "terms_in_map_count": len(terms_set),
        "rows_in_map_count": int(rows_in_map_count),
        "sample_themes": themes[:5],
        "sample_terms": terms_seen[:10],
    }
    return theme_terms, stats


class ThemeExtractor:
    def extract(self, signals: List[Signal], as_of: Optional[pd.Timestamp] = None) -> List[str]:
        raise NotImplementedError


@dataclass
class DefaultThemeExtractor(ThemeExtractor):
    max_themes: int = 5

    def extract(self, signals: List[Signal], as_of: Optional[pd.Timestamp] = None) -> List[str]:
        seen = []
        for signal in signals:
            theme = signal.core_theme
            if theme and theme not in seen:
                seen.append(theme)
            if len(seen) >= self.max_themes:
                break
        return seen


class ConceptMapper:
    def map(
        self,
        signals: List[Signal],
        theme_map: ThemeMap,
        themes: List[str],
        theme_map_path: Optional[Path] = None,
    ) -> Tuple[ThemeMap, Dict[str, object], Dict[str, str]]:
        raise NotImplementedError

    def flatten(self, theme_map: ThemeMap) -> List[str]:
        raise NotImplementedError


@dataclass
class DefaultConceptMapper(ConceptMapper):
    def _signal_candidate_keys(self, signal: Signal) -> List[str]:
        candidates: List[str] = []

        def _add(value) -> None:
            if value is None:
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    _add(item)
                return
            text = str(value).strip()
            if text and text not in candidates:
                candidates.append(text)

        _add(getattr(signal, "theme", None))
        _add(getattr(signal, "signal_theme", None))
        _add(getattr(signal, "signal_themes", None))
        _add(getattr(signal, "signal_themes_cn", None))
        return candidates

    def map(
        self,
        signals: List[Signal],
        theme_map: ThemeMap,
        themes: List[str],
        theme_map_path: Optional[Path] = None,
    ) -> Tuple[ThemeMap, Dict[str, object], Dict[str, str]]:
        allowed_ids = {signal.id for signal in signals}
        if themes:
            allowed_ids = {signal.id for signal in signals if signal.core_theme in themes}
        mapped_theme_map = {sid: entries for sid, entries in theme_map.items() if sid in allowed_ids}

        theme_terms: Dict[str, Set[str]] = {}
        if theme_map_path is not None:
            theme_terms = _terms_from_theme_map_csv(theme_map_path)

        candidate_keys: List[str] = []
        signal_theme_key_map: Dict[str, str] = {}

        for signal in signals:
            if signal.id not in allowed_ids:
                continue
            keys = self._signal_candidate_keys(signal)
            for key in keys:
                if key not in candidate_keys:
                    candidate_keys.append(key)
            if signal.id in mapped_theme_map and mapped_theme_map[signal.id]:
                continue
            if not theme_terms:
                continue
            matched_terms: Set[str] = set()
            matched_keys: List[str] = []
            for key in keys:
                terms = theme_terms.get(key)
                if terms:
                    matched_terms.update(terms)
                    matched_keys.append(key)
            if matched_terms:
                mapped_theme_map[signal.id] = [
                    {"type": "concept", "values": sorted(matched_terms)}
                ]
                if matched_keys:
                    signal_theme_key_map[signal.id] = matched_keys[0]

        theme_keys = list(theme_terms.keys())
        theme_key_hit_count = sum(1 for key in candidate_keys if key in theme_terms)
        theme_key_miss_count = len(candidate_keys) - theme_key_hit_count
        debug_stats = {
            "signals_theme_key_sample": candidate_keys[:10],
            "signals_theme_key_count": len(candidate_keys),
            "theme_map_theme_sample": theme_keys[:10],
            "theme_key_miss_count": theme_key_miss_count,
            "theme_key_hit_count": theme_key_hit_count,
        }
        return mapped_theme_map, debug_stats, signal_theme_key_map

    def flatten(self, theme_map: ThemeMap) -> List[str]:
        seen = []
        for entries in theme_map.values():
            for entry in entries:
                if entry["type"] in ("industry", "concept"):
                    for value in entry["values"]:
                        if value not in seen:
                            seen.append(value)
        return seen


class ThemeScorer:
    def score(
        self,
        indicator_df: pd.DataFrame,
        signals: List[Signal],
        theme_map: ThemeMap,
    ) -> Tuple[pd.DataFrame, Dict[str, List[Dict[str, object]]]]:
        raise NotImplementedError


@dataclass
class DefaultThemeScorer(ThemeScorer):
    def score(
        self,
        indicator_df: pd.DataFrame,
        signals: List[Signal],
        theme_map: ThemeMap,
    ) -> Tuple[pd.DataFrame, Dict[str, List[Dict[str, object]]]]:
        return score_stocks(indicator_df, signals, theme_map)


def build_snapshot_candidates(
    theme_map: ThemeMap,
    snapshot_dir: Path,
    membership_terms_by_ticker: Optional[Dict[str, List[str]]] = None,
    theme_map_path: Optional[Path] = None,
) -> Tuple[List[str], Dict[str, int], str, pd.DataFrame]:
    membership_path = snapshot_dir / "concept_membership.csv"
    prices_path = snapshot_dir / "prices.csv"
    if not membership_path.exists():
        _io_debug_exit(
            membership_path,
            FileNotFoundError(f"Missing concept_membership.csv: {membership_path}"),
        )
    if not prices_path.exists():
        _io_debug_exit(prices_path, FileNotFoundError(f"Missing prices.csv: {prices_path}"))

    try:
        membership = pd.read_csv(
            membership_path, dtype={"ticker": str, "concept": str, "industry": str}
        )
    except (FileNotFoundError, PermissionError) as exc:
        _io_debug_exit(membership_path, exc)
    if len(membership) == 0:
        raise ValueError(f"membership has 0 rows: {membership_path}")
    if "ticker" not in membership.columns:
        raise ValueError(
            "membership missing join key column(s) ['ticker']; "
            f"columns={list(membership.columns)}"
        )
    membership["ticker"] = membership["ticker"].map(normalize_ticker)
    membership["concept"] = membership.get("concept", "").astype(str).str.strip()
    membership["industry"] = membership.get("industry", membership["concept"]).astype(str).str.strip()
    membership["name"] = membership.get("name", "").astype(str).str.strip()
    membership["description"] = membership.get("description", "").astype(str).str.strip()

    try:
        prices = pd.read_csv(prices_path, dtype={"ticker": str})
    except (FileNotFoundError, PermissionError) as exc:
        _io_debug_exit(prices_path, exc)
    if len(prices) == 0:
        raise ValueError(f"prices has 0 rows: {prices_path}")
    if "ticker" not in prices.columns:
        raise ValueError(
            "prices missing join key column(s) ['ticker']; "
            f"columns={list(prices.columns)}"
        )
    prices["ticker"] = prices["ticker"].map(normalize_ticker)

    n_prices_tickers = int(prices["ticker"].nunique())
    n_membership_tickers = int(membership["ticker"].nunique())

    concept_set: Set[str] = set()
    matched_terms_by_ticker: Dict[str, List[str]] = {}
    theme_terms: Dict[str, Set[str]] = {}
    theme_stats: Dict[str, object] = {}

    if theme_map_path is not None:
        theme_terms, theme_stats = _theme_map_stats_from_csv(theme_map_path)
    if not theme_terms:
        theme_terms, theme_stats = _theme_map_stats_from_map(theme_map)

    for terms in theme_terms.values():
        concept_set.update(terms)

    if membership_terms_by_ticker is not None:
        theme_tickers = []
        for ticker, terms in membership_terms_by_ticker.items():
            term_set = set([str(t).strip() for t in terms if str(t).strip()])
            matched_terms = set()
            for mapped_terms in theme_terms.values():
                intersection = term_set.intersection(mapped_terms)
                if intersection:
                    matched_terms.update(intersection)
            if matched_terms:
                matched_terms_by_ticker[ticker] = sorted(matched_terms)
                theme_tickers.append(ticker)
        theme_tickers = sorted(set(theme_tickers))
    else:
        candidates_from_theme = membership[membership["concept"].isin(concept_set)]
        theme_tickers = sorted(set(candidates_from_theme["ticker"].tolist()))
    n_candidates_from_theme = len(theme_tickers)

    if n_candidates_from_theme == 0:
        candidates = sorted(set(prices["ticker"].tolist()))
        candidate_source = "universe_fallback"
    else:
        candidates = theme_tickers
        candidate_source = "theme"

    debug = {
        "n_prices_tickers": n_prices_tickers,
        "n_membership_tickers": n_membership_tickers,
        "n_candidates_from_theme": n_candidates_from_theme,
        "n_candidates_final": len(candidates),
        "candidate_source": candidate_source,
    }
    for key in (
        "themes_in_map_count",
        "terms_in_map_count",
        "rows_in_map_count",
        "sample_themes",
        "sample_terms",
    ):
        if key in theme_stats:
            debug[key] = theme_stats[key]
    if matched_terms_by_ticker:
        debug["matched_terms_by_ticker"] = matched_terms_by_ticker

    return candidates, debug, candidate_source, membership
