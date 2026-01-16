import pandas as pd


def main() -> None:
    base_dir = "data/snapshots/2026-01-20"
    membership = pd.read_csv(f"{base_dir}/concept_membership.csv")
    prices = pd.read_csv(f"{base_dir}/prices.csv")

    for col in ("ticker", "concept"):
        if col not in membership.columns:
            raise AssertionError(f"missing column: {col}")
        if membership[col].isna().any():
            raise AssertionError(f"null values in {col}")
        if (membership[col].astype(str).str.strip() == "").any():
            raise AssertionError(f"empty values in {col}")

    if membership.duplicated(subset=["ticker", "concept"]).any():
        raise AssertionError("duplicate (ticker, concept) in membership")

    membership_tickers = set(membership["ticker"].astype(str).tolist())
    prices_tickers = set(prices["ticker"].astype(str).tolist())
    if not membership_tickers.issubset(prices_tickers):
        raise AssertionError("membership tickers not subset of prices tickers")

    unique_concepts = membership["concept"].nunique()
    if unique_concepts < 8:
        raise AssertionError("unique_concepts < 8")

    min_concept_members = membership.groupby("concept").size().min()
    if min_concept_members < 50:
        raise AssertionError("min_concept_members < 50")


if __name__ == "__main__":
    main()
