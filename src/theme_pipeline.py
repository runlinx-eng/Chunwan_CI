from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .scoring import score_stocks
from .signals import Signal

ThemeMap = Dict[str, List[Dict[str, List[str]]]]


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
    def map(self, signals: List[Signal], theme_map: ThemeMap, themes: List[str]) -> ThemeMap:
        raise NotImplementedError

    def flatten(self, theme_map: ThemeMap) -> List[str]:
        raise NotImplementedError


@dataclass
class DefaultConceptMapper(ConceptMapper):
    def map(self, signals: List[Signal], theme_map: ThemeMap, themes: List[str]) -> ThemeMap:
        allowed_ids = {signal.id for signal in signals}
        if themes:
            allowed_ids = {signal.id for signal in signals if signal.core_theme in themes}
        return {sid: entries for sid, entries in theme_map.items() if sid in allowed_ids}

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
    theme_map: ThemeMap, snapshot_dir: Path
) -> Tuple[List[str], Dict[str, int], str, pd.DataFrame]:
    membership_path = snapshot_dir / "concept_membership.csv"
    prices_path = snapshot_dir / "prices.csv"
    if not membership_path.exists():
        raise FileNotFoundError(f"Missing concept_membership.csv: {membership_path}")
    if not prices_path.exists():
        raise FileNotFoundError(f"Missing prices.csv: {prices_path}")

    membership = pd.read_csv(membership_path, dtype={"ticker": str, "concept": str, "industry": str})
    membership["ticker"] = membership["ticker"].astype(str).str.strip()
    membership["concept"] = membership.get("concept", "").astype(str).str.strip()
    membership["industry"] = membership.get("industry", membership["concept"]).astype(str).str.strip()
    membership["name"] = membership.get("name", "").astype(str).str.strip()
    membership["description"] = membership.get("description", "").astype(str).str.strip()

    prices = pd.read_csv(prices_path, dtype={"ticker": str})
    prices["ticker"] = prices["ticker"].astype(str).str.strip()

    n_prices_tickers = int(prices["ticker"].nunique())
    n_membership_tickers = int(membership["ticker"].nunique())

    concepts = []
    for entries in theme_map.values():
        for entry in entries:
            if entry["type"] in ("industry", "concept"):
                concepts.extend(entry["values"])
    concepts = [str(c).strip() for c in concepts if str(c).strip()]
    concept_set = set(concepts)

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

    return candidates, debug, candidate_source, membership
