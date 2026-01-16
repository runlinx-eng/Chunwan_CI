from dataclasses import dataclass
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
