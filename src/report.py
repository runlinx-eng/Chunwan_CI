from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

from .signals import Signal


def build_report(
    scored_df: pd.DataFrame,
    signals: List[Signal],
    hit_map: Dict[str, List[Dict[str, object]]],
    as_of: pd.Timestamp,
    top_n: int,
    themes_used: Optional[List[str]] = None,
) -> Dict:
    top_df = scored_df.sort_values("final_score", ascending=False).head(top_n)
    rows = []
    for _, row in top_df.iterrows():
        hits = hit_map.get(row["ticker"], [])
        merged_hits = {}
        for hit in hits:
            core_theme = hit["theme"]
            entry = merged_hits.setdefault(
                core_theme,
                {
                    "signal_id": hit.get("signal_id"),
                    "signal_ids": [],
                    "signal_theme": hit.get("signal_theme"),
                    "signal_themes": [],
                    "theme": core_theme,
                    "weight": 0.0,
                    "match_paths": set(),
                    "matched_terms": set(),
                    "matched_source": set(),
                },
            )
            if hit.get("signal_id"):
                entry["signal_ids"].append(hit["signal_id"])
            if hit.get("signal_theme"):
                entry["signal_themes"].append(hit["signal_theme"])
            entry["weight"] += float(hit.get("weight", 0.0))
            entry["match_paths"].update(hit.get("match_paths", []))
            entry["matched_terms"].update(hit.get("matched_terms", []))
            entry["matched_source"].update(hit.get("matched_source", []))
        hits = []
        for entry in merged_hits.values():
            entry["signal_ids"] = sorted(set(entry["signal_ids"]))
            entry["signal_themes"] = sorted(set(entry["signal_themes"]))
            entry["match_paths"] = sorted(entry["match_paths"])
            entry["matched_terms"] = sorted(entry["matched_terms"])
            entry["matched_source"] = sorted(entry["matched_source"])
            hits.append(entry)
        tech_components = {
            "momentum_20": float(0.5 * row["momentum_20_rank"]),
            "momentum_60": float(0.3 * row["momentum_60_rank"]),
            "volume": float(0.2 * row["volume_rank"]),
        }
        theme_components = [
            {
                "theme": hit["theme"],
                "weight": float(hit["weight"]),
                "signal_ids": hit.get("signal_ids", []),
                "signal_themes": hit.get("signal_themes", []),
                "match_paths": hit.get("match_paths", []),
            }
            for hit in hits
        ]

        score_theme_total = float(row["theme_score"])
        score_tech_total = float(row["technical_score"])
        score_total = score_theme_total + score_tech_total

        if themes_used:
            themes_used_list = list(dict.fromkeys(themes_used))[:5]
        else:
            themes_used_list = list(dict.fromkeys([hit["theme"] for hit in hits if hit.get("theme")]))[:5]

        concept_hits = []
        if row.get("concept") or row.get("industry"):
            concept_hits.append(
                {
                    "concept": row.get("concept") or "",
                    "industry": row.get("industry") or "",
                    "evidence": "membership",
                }
            )

        contributions = [
            ("theme", score_theme_total, f"theme:+{score_theme_total:.3f}"),
            ("momentum_20", tech_components["momentum_20"], f"tech_momentum_20:+{tech_components['momentum_20']:.3f}"),
            ("momentum_60", tech_components["momentum_60"], f"tech_momentum_60:+{tech_components['momentum_60']:.3f}"),
            ("volume", tech_components["volume"], f"tech_volume:+{tech_components['volume']:.3f}"),
        ]
        contributions.sort(key=lambda x: (-x[1], x[0]))
        why_in_top5 = [item[2] for item in contributions[:3]]

        reason = {
            "themes_used": themes_used_list,
            "concept_hits": concept_hits,
            "why_in_top5": why_in_top5,
        }
        rows.append(
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "industry": row["industry"],
                "final_score": float(row["final_score"]),
                "theme_hits": hits,
                "score_breakdown": {
                    "score_total": float(score_total),
                    "score_tech_total": float(score_tech_total),
                    "score_theme_total": float(score_theme_total),
                    "tech_components": tech_components,
                    "theme_components": theme_components,
                    "theme_score": float(row["theme_score"]),
                    "momentum_20_rank": float(row["momentum_20_rank"]),
                    "momentum_60_rank": float(row["momentum_60_rank"]),
                    "volume_rank": float(row["volume_rank"]),
                    "final_score": float(row["final_score"]),
                },
                "data_date": as_of.strftime("%Y-%m-%d"),
                "indicators": {
                    "momentum_20": float(row["momentum_20"]),
                    "momentum_60": float(row["momentum_60"]),
                    "volatility_20": float(row["volatility_20"]),
                    "avg_volume_20": float(row["avg_volume_20"]),
                },
                "reason": reason,
            }
        )

    return {
        "as_of": as_of.strftime("%Y-%m-%d"),
        "top_n": top_n,
        "count": len(rows),
        "results": rows,
    }
