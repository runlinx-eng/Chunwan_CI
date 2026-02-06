from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
import yaml


@dataclass(frozen=True)
class Signal:
    id: str
    theme: str
    core_theme: str
    keywords: List[str]
    priority: str
    description: str
    weight: float
    phase: str
    family: str
    formula: str
    horizon_days: int
    decay: float
    guardrails: Dict[str, Any]


PRIORITY_WEIGHT = {
    "high": 1.0,
    "medium": 0.6,
    "low": 0.3,
}

ALLOWED_SIGNAL_FAMILIES = {
    "legacy_keyword",
    "theme_strength",
    "theme_event",
    "technical_momentum",
    "risk_control",
}


def _parse_family(item: Dict[str, Any]) -> str:
    family = str(item.get("family", "legacy_keyword")).strip() or "legacy_keyword"
    if family not in ALLOWED_SIGNAL_FAMILIES:
        raise ValueError(
            f"invalid signal family={family!r}, allowed={sorted(ALLOWED_SIGNAL_FAMILIES)}"
        )
    return family


def _parse_formula(item: Dict[str, Any]) -> str:
    formula = str(item.get("formula", "keyword_hit")).strip()
    if not formula:
        raise ValueError("signal formula must be non-empty")
    return formula


def _parse_horizon_days(item: Dict[str, Any]) -> int:
    raw = item.get("horizon_days", 20)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid horizon_days={raw!r}") from exc
    if value <= 0:
        raise ValueError(f"horizon_days must be > 0, got {value}")
    return value


def _parse_decay(item: Dict[str, Any]) -> float:
    raw = item.get("decay", 1.0)
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid decay={raw!r}") from exc
    if value <= 0:
        raise ValueError(f"decay must be > 0, got {value}")
    return value


def _parse_guardrails(item: Dict[str, Any]) -> Dict[str, Any]:
    raw = item.get("guardrails", {})
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"guardrails must be an object, got {type(raw).__name__}")
    return raw


def load_signals(path: str) -> List[Signal]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    signals = []
    for item in raw.get("signals", []):
        priority = item.get("priority", "low")
        weight = item.get("weight")
        core_theme = item.get("core_theme", item.get("theme", ""))
        if weight is None:
            if item.get("id") == "signal_009":
                weight = 0.0
            else:
                weight = PRIORITY_WEIGHT.get(priority, 0.3)
        signals.append(
            Signal(
                id=item["id"],
                theme=item["theme"],
                core_theme=core_theme,
                keywords=item.get("keywords", []),
                priority=priority,
                description=item.get("description", ""),
                weight=float(weight),
                phase=item.get("phase", "live"),
                family=_parse_family(item),
                formula=_parse_formula(item),
                horizon_days=_parse_horizon_days(item),
                decay=_parse_decay(item),
                guardrails=_parse_guardrails(item),
            )
        )
    return signals


def load_theme_industry_map(path: str) -> Dict[str, List[Dict[str, List[str]]]]:
    df = pd.read_csv(path)
    mapping: Dict[str, List[Dict[str, List[str]]]] = {}
    has_new_format = "map_type" in df.columns and "map_values" in df.columns
    for _, row in df.iterrows():
        signal_id = str(row["主题ID"])
        if has_new_format:
            map_type = str(row["map_type"]).strip().lower()
            values = [s.strip() for s in str(row["map_values"]).split("、") if s.strip()]
        else:
            map_type = "concept"
            values = [s.strip() for s in str(row["对应行业/概念"]).split("、") if s.strip()]
        if not values:
            continue
        mapping.setdefault(signal_id, []).append({"type": map_type, "values": values})
    return mapping
