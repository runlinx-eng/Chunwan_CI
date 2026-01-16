#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path
from typing import List, Optional

import pandas as pd


TICKER_COLUMNS = ["ticker", "symbol", "code"]
CONCEPT_COLUMNS = ["concept", "theme", "board"]
NAME_COLUMNS = ["name", "stock_name"]
INDUSTRY_COLUMNS = ["industry"]
DESCRIPTION_COLUMNS = ["description"]


def pick_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def normalize_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_prices_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_dir():
        candidate = path / "prices.csv"
        if candidate.exists():
            return candidate
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest concepts CSV into snapshot membership")
    parser.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    parser.add_argument("--prices-snapshot", required=True, help="Path to prices.csv or snapshot dir")
    parser.add_argument("--concepts-input", required=True, help="Path to external concepts CSV")
    parser.add_argument("--out-dir", default=None, help="Output dir (default data/snapshots/<as_of>/)")
    parser.add_argument("--min-concept-members", type=int, default=1)
    args = parser.parse_args()

    as_of = args.as_of
    prices_path = resolve_prices_path(args.prices_snapshot)
    concepts_path = Path(args.concepts_input)
    out_dir = Path(args.out_dir) if args.out_dir else Path("data/snapshots") / as_of

    if not prices_path.exists():
        raise FileNotFoundError(f"prices snapshot not found: {prices_path}")
    if not concepts_path.exists():
        raise FileNotFoundError(f"concepts input not found: {concepts_path}")

    prices = pd.read_csv(prices_path)
    if "ticker" not in prices.columns:
        raise AssertionError("prices.csv must contain 'ticker' column")
    prices["ticker"] = normalize_series(prices["ticker"])
    prices_tickers = set(prices["ticker"].tolist())

    concepts = pd.read_csv(concepts_path)

    ticker_col = pick_column(concepts, TICKER_COLUMNS)
    concept_col = pick_column(concepts, CONCEPT_COLUMNS)
    if ticker_col is None or concept_col is None:
        raise AssertionError("concepts input missing ticker or concept column")

    name_col = pick_column(concepts, NAME_COLUMNS)
    industry_col = pick_column(concepts, INDUSTRY_COLUMNS)
    description_col = pick_column(concepts, DESCRIPTION_COLUMNS)

    concepts["ticker"] = normalize_series(concepts[ticker_col])
    concepts["concept"] = normalize_series(concepts[concept_col])

    concepts["name"] = (
        normalize_series(concepts[name_col]) if name_col else ""
    )
    concepts["industry"] = (
        normalize_series(concepts[industry_col]) if industry_col else ""
    )
    concepts["description"] = (
        normalize_series(concepts[description_col]) if description_col else ""
    )

    concepts = concepts[(concepts["ticker"] != "") & (concepts["concept"] != "")]
    concepts = concepts.drop_duplicates(subset=["ticker", "concept"], keep="first")
    concepts = concepts[concepts["ticker"].isin(prices_tickers)]

    if args.min_concept_members > 1 and not concepts.empty:
        counts = concepts.groupby("concept").size()
        keep_concepts = counts[counts >= args.min_concept_members].index
        concepts = concepts[concepts["concept"].isin(keep_concepts)]

    if concepts.empty:
        raise AssertionError("no concept memberships after filtering")

    concepts["name"] = concepts.apply(
        lambda row: row["name"] if row["name"] else f"STOCK_{row['ticker']}", axis=1
    )

    concepts = concepts[["ticker", "name", "concept", "industry", "description"]]
    concepts = concepts.sort_values(["concept", "ticker"]).reset_index(drop=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    membership_path = out_dir / "concept_membership.csv"
    concepts.to_csv(membership_path, index=False)

    # Manifest
    rows_concept_membership = int(len(concepts))
    rows_prices = int(len(prices))
    unique_tickers = int(concepts["ticker"].nunique())
    unique_concepts = int(concepts["concept"].nunique())
    min_concept_members = int(concepts.groupby("concept").size().min()) if not concepts.empty else 0
    min_price_bars = int(prices.groupby("ticker").size().min()) if not prices.empty else 0

    manifest = {
        "as_of": as_of,
        "stats": {
            "unique_tickers": unique_tickers,
            "unique_concepts": unique_concepts,
            "min_concept_members": min_concept_members,
            "min_price_bars": min_price_bars,
            "rows_concept_membership": rows_concept_membership,
            "rows_prices": rows_prices,
        },
        "files": {
            "concept_membership.csv": {"sha256": sha256_file(membership_path)},
            "prices.csv": {"sha256": sha256_file(prices_path)},
        },
    }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
