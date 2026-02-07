import pandas as pd

from src.report import build_report


def _base_row(theme_score: float, final_score: float) -> dict:
    return {
        "ticker": "000001",
        "name": "平安银行",
        "industry": "",
        "concept": "",
        "description": "",
        "momentum_20": 0.1,
        "momentum_60": 0.2,
        "volatility_20": 0.03,
        "avg_volume_20": 1000000.0,
        "momentum_20_rank": 0.6,
        "momentum_60_rank": 0.7,
        "volume_rank": 0.8,
        "theme_score": theme_score,
        "technical_score": 0.65,
        "risk_penalty": 0.2,
        "score_w_theme": 1.0,
        "score_w_tech": 1.0,
        "score_w_risk": 0.0,
        "final_score": final_score,
    }


def test_report_marks_no_theme_hit_reason():
    as_of = pd.Timestamp("2026-02-05")
    scored_df = pd.DataFrame([_base_row(theme_score=0.0, final_score=0.65)])
    report = build_report(
        scored_df=scored_df,
        signals=[],
        hit_map={},
        as_of=as_of,
        top_n=1,
        themes_used=["测试主题"],
        provider="akshare",
    )
    row = report["results"][0]
    assert "命中主题: 无（当前样本未命中主题映射，按技术因子排序）" in row["reason"]
    assert "as_of=2026-02-05" in row["reason"]
    assert row["exchange_ticker"] == "000001.SZ"
    assert row["reason_struct"]["theme_evidence_status"] == "missing"
    assert row["reason_struct"]["ranking_mode"] == "tech_only_due_to_no_theme_hits"
    assert row["reason_struct"]["theme_hit_count"] == 0


def test_report_marks_theme_hit_reason():
    as_of = pd.Timestamp("2026-02-05")
    scored_df = pd.DataFrame([_base_row(theme_score=1.2, final_score=1.85)])
    hit_map = {
        "000001": [
            {
                "signal_id": "s1",
                "signal_theme": "测试主题",
                "theme": "测试主题",
                "weight": 1.2,
                "match_paths": ["concept"],
                "matched_terms": ["AI技术服务"],
                "matched_source": ["map"],
            }
        ]
    }
    report = build_report(
        scored_df=scored_df,
        signals=[],
        hit_map=hit_map,
        as_of=as_of,
        top_n=1,
        themes_used=["测试主题"],
        provider="akshare",
    )
    row = report["results"][0]
    assert "命中主题: 测试主题" in row["reason"]
    assert "命中主题: 无（当前样本未命中主题映射，按技术因子排序）" not in row["reason"]
    assert "as_of=2026-02-05" in row["reason"]
    assert row["exchange_ticker"] == "000001.SZ"
    assert row["reason_struct"]["theme_evidence_status"] == "hit"
    assert row["reason_struct"]["ranking_mode"] == "theme_tech"
    assert row["reason_struct"]["theme_hit_count"] == 1
