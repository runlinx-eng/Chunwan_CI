#!/usr/bin/env python3
import argparse
import csv
import math
from datetime import date, timedelta
from pathlib import Path
import random


def business_days(end_date: date, count: int):
    days = []
    cur = end_date
    while len(days) < count:
        if cur.weekday() < 5:
            days.append(cur)
        cur -= timedelta(days=1)
    return list(sorted(days))


def main():
    parser = argparse.ArgumentParser(description="Generate offline snapshot data")
    parser.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    parser.add_argument("--n-tickers", type=int, default=500)
    parser.add_argument("--n-concepts", type=int, default=10)
    parser.add_argument("--min-count", type=int, default=160)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    rng = random.Random(args.seed)

    if args.n_tickers < 300:
        raise ValueError("n-tickers must be >= 300")
    if args.n_concepts < 8:
        raise ValueError("n-concepts must be >= 8")
    if args.min_count < 130:
        raise ValueError("min-count must be >= 130")

    theme_map_path = Path("theme_to_industry.csv")
    concept_candidates = []
    if theme_map_path.exists():
        text = theme_map_path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if "对应行业/概念" not in line and "," not in line:
                continue
            parts = line.split(",")
            if len(parts) < 4:
                continue
            raw = parts[3]
            for token in raw.replace("；", ";").replace("，", ",").replace("、", ",").split(","):
                token = token.strip()
                if token and token not in concept_candidates:
                    concept_candidates.append(token)

    concepts = []
    for candidate in concept_candidates:
        if len(concepts) >= args.n_concepts:
            break
        concepts.append(candidate)
    while len(concepts) < args.n_concepts:
        concepts.append(f"扩展概念_{len(concepts)+1:02d}")

    tickers = [f"A{idx:04d}" for idx in range(1, args.n_tickers + 1)]

    membership_rows = []
    for i, ticker in enumerate(tickers):
        concept = concepts[i % len(concepts)]
        membership_rows.append(
            {
                "ticker": ticker,
                "name": f"STOCK_{ticker}",
                "concept": concept,
                "industry": concept,
                "description": f"{concept} 主题",
            }
        )

    seen = set()
    for row in membership_rows:
        if row["ticker"] in seen:
            raise RuntimeError("duplicate ticker in membership")
        seen.add(row["ticker"])

    dates = business_days(as_of, args.min_count)
    price_rows = []
    for t_idx, ticker in enumerate(tickers):
        base = 10 + (t_idx % 20) * 0.2
        drift = 0.02 + (t_idx % 7) * 0.001
        for d_idx, d in enumerate(dates):
            close = base + d_idx * drift + (t_idx % 5) * 0.01
            volume = 1_000_000 + d_idx * 800 + (t_idx % 10) * 50
            if not (math.isfinite(close) and math.isfinite(volume) and volume > 0):
                raise RuntimeError("invalid price or volume generated")
            price_rows.append(
                {
                    "date": d.isoformat(),
                    "ticker": ticker,
                    "close": f"{close:.4f}",
                    "volume": int(volume),
                }
            )

    output_dir = Path("data/snapshots") / args.as_of
    output_dir.mkdir(parents=True, exist_ok=True)

    membership_rows.sort(key=lambda r: (r["concept"], r["ticker"]))
    price_rows.sort(key=lambda r: (r["ticker"], r["date"]))

    with (output_dir / "concept_membership.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["ticker", "name", "concept", "industry", "description"]
        )
        writer.writeheader()
        writer.writerows(membership_rows)

    with (output_dir / "prices.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "ticker", "close", "volume"])
        writer.writeheader()
        writer.writerows(price_rows)

    min_count = len(dates)
    print(
        f"as_of={args.as_of} seed={args.seed} unique_tickers={len(tickers)} "
        f"unique_concepts={len(concepts)} min_count={min_count} out_dir={output_dir}"
    )


if __name__ == "__main__":
    main()
