from pathlib import Path

import pytest

from src.signals import load_signals


def test_legacy_signals_defaults_are_backfilled():
    signals = load_signals("signals.yaml")
    assert len(signals) > 0
    for signal in signals:
        assert signal.family == "legacy_keyword"
        assert signal.formula == "keyword_hit"
        assert signal.horizon_days > 0
        assert signal.decay > 0
        assert isinstance(signal.guardrails, dict)


def test_v2_fields_parse_successfully(tmp_path: Path):
    content = """
signals:
  - id: signal_x
    theme: test theme
    core_theme: test core
    keywords: ["a"]
    priority: high
    family: theme_strength
    formula: concept_strength_v1
    horizon_days: 15
    decay: 0.8
    guardrails:
      max_weight: 1.2
"""
    path = tmp_path / "signals_v2.yaml"
    path.write_text(content, encoding="utf-8")

    signals = load_signals(str(path))
    assert len(signals) == 1
    signal = signals[0]
    assert signal.family == "theme_strength"
    assert signal.formula == "concept_strength_v1"
    assert signal.horizon_days == 15
    assert signal.decay == 0.8
    assert signal.guardrails.get("max_weight") == 1.2


def test_invalid_family_raises(tmp_path: Path):
    content = """
signals:
  - id: signal_bad
    theme: bad
    keywords: []
    priority: low
    family: unknown_family
"""
    path = tmp_path / "signals_bad.yaml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="invalid signal family"):
        load_signals(str(path))
