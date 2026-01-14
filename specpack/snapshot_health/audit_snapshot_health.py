import json
import math
import subprocess
from pathlib import Path

import pandas as pd
import yaml


def load_snapshot_membership(snapshot_dir: Path) -> pd.DataFrame:
    path = snapshot_dir / "concept_membership.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing membership file: {path}")
    df = pd.read_csv(path)
    required = {"ticker", "name", "concept", "industry", "description"}
    missing = required - set(df.columns)
    if missing:
        raise AssertionError(f"membership missing columns: {sorted(missing)}")
    if df["ticker"].duplicated().any():
        raise AssertionError("membership ticker is not unique")
    return df


def load_snapshot_prices(snapshot_dir: Path) -> pd.DataFrame:
    path = snapshot_dir / "prices.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing prices file: {path}")
    df = pd.read_csv(path)
    required = {"date", "ticker", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise AssertionError(f"prices missing columns: {sorted(missing)}")
    return df


def check_report_fields(report: dict) -> None:
    results = report.get("results", [])
    for row in results:
        for field in ("theme_hits", "score_breakdown", "data_date"):
            if field not in row:
                raise AssertionError(f"missing field: {field}")
        indicators = row.get("indicators", {})
        for key, value in indicators.items():
            if value is None:
                raise AssertionError(f"indicator {key} is None")
            if isinstance(value, float) and math.isnan(value):
                raise AssertionError(f"indicator {key} is NaN")
        breakdown = row.get("score_breakdown", {})
        for key, value in breakdown.items():
            if value is None:
                raise AssertionError(f"score_breakdown {key} is None")
            if isinstance(value, float) and math.isnan(value):
                raise AssertionError(f"score_breakdown {key} is NaN")
        themes = [hit.get("theme") for hit in row.get("theme_hits", []) if hit.get("theme")]
        if len(themes) != len(set(themes)):
            raise AssertionError("duplicate core theme in a single stock")


def load_signal_core_map() -> dict:
    raw = yaml.safe_load(Path("signals.yaml").read_text(encoding="utf-8"))
    mapping = {}
    for item in raw.get("signals", []):
        mapping[item["id"]] = item.get("core_theme", item.get("theme", ""))
    return mapping


def load_core_theme_concepts(signal_to_core: dict) -> dict:
    df = pd.read_csv("theme_to_industry.csv")
    core_map = {}
    for _, row in df.iterrows():
        signal_id = str(row["主题ID"])
        core_theme = signal_to_core.get(signal_id)
        if not core_theme:
            continue
        concepts = str(row.get("对应行业/概念", ""))
        tokens = [
            t.strip()
            for t in concepts.replace("；", ";").replace("，", ",").replace("、", ",").split(",")
            if t.strip()
        ]
        core_map.setdefault(core_theme, set()).update(tokens)
    return core_map


def main() -> None:
    conf = yaml.safe_load(Path("specpack/snapshot_replay/assertions.yaml").read_text(encoding="utf-8"))
    cmd = conf["run"]["cmd"]
    output_json = Path(conf["run"]["output_json"])
    as_of = conf["run"]["as_of"]

    print(f"[snapshot_health] running: {cmd}")
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        raise SystemExit(ret)

    if not output_json.exists():
        raise FileNotFoundError(f"missing report: {output_json}")

    report = json.loads(output_json.read_text(encoding="utf-8"))
    snapshot_dir = Path("data/snapshots") / as_of
    membership = load_snapshot_membership(snapshot_dir)
    prices = load_snapshot_prices(snapshot_dir)

    counts = prices.groupby("ticker").size()
    if (counts < 121).any():
        raise AssertionError("min_count < 121 in snapshot prices.csv")

    if prices["date"].max() > as_of:
        raise AssertionError("prices date exceeds as_of")
    if as_of not in set(prices["date"].astype(str).tolist()):
        raise AssertionError("prices missing as_of date")

    check_report_fields(report)

    signal_to_core = load_signal_core_map()
    core_to_concepts = load_core_theme_concepts(signal_to_core)

    core_seen = set()
    for row in report.get("results", []):
        for hit in row.get("theme_hits", []):
            core_theme = hit.get("theme")
            if core_theme:
                core_seen.add(core_theme)

    for core_theme in core_seen:
        concepts = core_to_concepts.get(core_theme, set())
        if not concepts:
            raise AssertionError(f"no concepts found for core theme: {core_theme}")
        matched = membership[membership["concept"].isin(concepts)]["concept"].nunique()
        if matched < 2:
            raise AssertionError(f"core theme {core_theme} concepts < 2")

    print("[snapshot_health] passed")


if __name__ == "__main__":
    main()
