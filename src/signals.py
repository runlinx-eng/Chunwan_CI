from dataclasses import dataclass
from typing import Dict, List

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


PRIORITY_WEIGHT = {
    "high": 1.0,
    "medium": 0.6,
    "low": 0.3,
}


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
