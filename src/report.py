from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .signals import Signal


def build_report(
    scored_df: pd.DataFrame,
    signals: List[Signal],
    hit_map: Dict[str, List[Dict[str, object]]],
    as_of: pd.Timestamp,
    top_n: int,
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
        theme_details = []
        risk_details = []
        for hit in hits:
            theme = hit["theme"]
            weight = hit["weight"]
            match_paths = "/".join(hit.get("match_paths", [])) if hit.get("match_paths") else "unknown"
            detail = f"{theme}(权重{weight:.2f}, 命中路径:{match_paths})"
            if hit["signal_id"] == "signal_009":
                risk_details.append(detail)
            else:
                theme_details.append(detail)

        reason_parts = []
        if theme_details:
            reason_parts.append(f"命中主题: {', '.join(theme_details)}")
        if risk_details:
            reason_parts.append(f"风险提示: {', '.join(risk_details)}")
        reason_parts.append(
            "评分构成: "
            f"主题{row['theme_score']:.3f}"
            f"+0.5*20日动量分位{row['momentum_20_rank']:.3f}"
            f"+0.3*60日动量分位{row['momentum_60_rank']:.3f}"
            f"+0.2*均量分位{row['volume_rank']:.3f}"
            f"={row['final_score']:.3f}"
        )
        if row.get("indicator_missing"):
            reason_parts.append("指标缺失按0处理")
        reason_parts.append(f"20日动量: {row['momentum_20']:.4f}")
        reason_parts.append(f"60日动量: {row['momentum_60']:.4f}")
        reason_parts.append(f"20日波动率: {row['volatility_20']:.4f}")
        reason_parts.append(f"20日均量: {row['avg_volume_20']:.0f}")
        rows.append(
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "industry": row["industry"],
                "final_score": float(row["final_score"]),
                "theme_hits": hits,
                "score_breakdown": {
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
                "reason": "; ".join(reason_parts),
            }
        )

    return {
        "as_of": as_of.strftime("%Y-%m-%d"),
        "top_n": top_n,
        "count": len(rows),
        "results": rows,
    }
