#!/usr/bin/env python3
import argparse
import csv
import re
from pathlib import Path

import pandas as pd


def read_base_schema(repo_root: Path) -> list:
    base_path = repo_root / "theme_to_industry.csv"
    if not base_path.exists():
        raise SystemExit(f"missing base schema file: {base_path}")

    first = base_path.read_text(encoding="utf-8").splitlines()[0].strip()
    header = [x.strip() for x in first.split(",") if x.strip()]
    if header not in (["theme", "industry"], ["theme", "concept"]):
        raise SystemExit(f"Unsupported base header from theme_to_industry.csv: {header}")
    return header


def split_terms(s: str) -> list:
    # 支持：逗号/中文逗号/分号/中文分号/顿号/竖线/换行/空白
    parts = re.split(r"[,\uFF0C;\uFF1B\u3001\|\s]+", str(s))
    out = []
    for p in parts:
        p = p.strip()
        if p:
            out.append(p)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Input CN theme map csv")
    ap.add_argument("--out", dest="out_path", required=True, help="Output normalized csv")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    base_header = read_base_schema(repo_root)
    out_term_col = base_header[1]  # industry or concept

    in_path = Path(args.in_path)
    if not in_path.exists():
        raise SystemExit(f"missing input file: {in_path}")

    df = pd.read_csv(in_path)
    required_cols = ["主题名称", "对应行业/概念"]
    for c in required_cols:
        if c not in df.columns:
            raise SystemExit(f"missing required column {c}; got columns={list(df.columns)}")

    rows = []
    themes = []
    terms_set = set()

    for _, r in df.iterrows():
        theme = str(r["主题名称"]).strip()
        if not theme:
            continue
        if theme not in themes:
            themes.append(theme)

        terms = split_terms(r["对应行业/概念"])
        for t in terms:
            terms_set.add(t)
            rows.append({"theme": theme, out_term_col: t})

    if not rows:
        raise SystemExit("no rows generated; check input content")

    out_path = Path(args.out_path)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=base_header)
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print(
        f"summary: base_header={base_header} themes_count={len(themes)} "
        f"terms_count={len(terms_set)} rows_written={len(rows)} out={out_path}"
    )


if __name__ == "__main__":
    main()
