from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.utils import parse_date, previous_trading_date


@dataclass
class RunResult:
    returncode: int
    stdout: str
    stderr: str
    json_path: Path
    csv_path: Path
    command: list[str]


def _inject_style() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@500&display=swap');
        :root {
          --bg-cream: #f6efe4;
          --ink: #1f2a37;
          --card: #fffdf8;
          --accent: #116149;
          --accent-soft: #d8efe7;
          --line: #e4dac8;
        }
        .stApp {
          background:
            radial-gradient(circle at 0% 0%, #f7e8cf 0%, transparent 46%),
            radial-gradient(circle at 100% 100%, #d8efe7 0%, transparent 52%),
            var(--bg-cream);
          color: var(--ink);
          font-family: "Space Grotesk", "PingFang SC", "Microsoft YaHei", sans-serif;
        }
        .stTextInput input, .stNumberInput input, .stDateInput input {
          border-radius: 10px;
        }
        .stButton button {
          border: 1px solid var(--accent);
          background: linear-gradient(90deg, #116149 0%, #1a7f5d 100%);
          color: white;
          border-radius: 10px;
          font-weight: 700;
          letter-spacing: 0.3px;
        }
        .stCode pre {
          font-family: "JetBrains Mono", monospace;
        }
        div[data-testid="stMetric"] {
          background: var(--card);
          border: 1px solid var(--line);
          border-radius: 12px;
          padding: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _select_default(files: list[Path], preferred: str) -> int:
    for i, p in enumerate(files):
        if p.name == preferred:
            return i
    return 0


def _list_signal_files() -> list[Path]:
    files = sorted(ROOT_DIR.glob("*.yaml"))
    return files if files else [ROOT_DIR / "signals.yaml"]


def _list_theme_map_files() -> list[Path]:
    files = sorted(ROOT_DIR.glob("theme_to_industry*.csv"))
    return files if files else [ROOT_DIR / "theme_to_industry.csv"]


def _output_paths(run_date: str, top_n: int) -> tuple[Path, Path]:
    as_of = previous_trading_date(parse_date(run_date))
    base = ROOT_DIR / "outputs" / f"report_{as_of.strftime('%Y-%m-%d')}_top{top_n}"
    return base.with_suffix(".json"), base.with_suffix(".csv")


def _run_screener(
    run_date: str,
    top_n: int,
    provider: str,
    no_fallback: bool,
    no_cache: bool,
    snapshot_as_of: str | None,
    signals_path: Path,
    theme_map_path: Path,
    theme_weight: float,
) -> RunResult:
    cmd = [
        sys.executable,
        "-m",
        "src.run",
        "--date",
        run_date,
        "--top",
        str(top_n),
        "--provider",
        provider,
        "--signals",
        str(signals_path),
        "--theme-map",
        str(theme_map_path),
        "--theme-weight",
        str(theme_weight),
    ]
    if no_fallback:
        cmd.append("--no-fallback")
    if no_cache:
        cmd.append("--no-cache")
    if provider == "snapshot":
        if not snapshot_as_of:
            raise ValueError("snapshot 模式必须提供 snapshot_as_of")
        cmd.extend(["--snapshot-as-of", snapshot_as_of])

    json_path, csv_path = _output_paths(run_date, top_n)
    proc = subprocess.run(
        cmd,
        cwd=ROOT_DIR,
        text=True,
        capture_output=True,
        check=False,
    )
    return RunResult(
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        json_path=json_path,
        csv_path=csv_path,
        command=cmd,
    )


def _results_table(report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(report.get("results", []), start=1):
        score_breakdown = item.get("score_breakdown", {})
        theme_hits = item.get("theme_hits", [])
        rows.append(
            {
                "rank": idx,
                "ticker": item.get("ticker", ""),
                "name": item.get("name", ""),
                "final_score": score_breakdown.get("final_score", ""),
                "theme_score": score_breakdown.get("score_theme_total", ""),
                "tech_score": score_breakdown.get("score_technical", ""),
                "theme_hit_count": len(theme_hits) if isinstance(theme_hits, list) else 0,
                "reason": item.get("reason", ""),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(
        page_title="A股主题选股器",
        page_icon="A",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_style()

    st.title("节前 A 股主题选股器")
    st.caption("输入参数，运行策略，查看 Top N 候选股和可解释依据。")

    signal_files = _list_signal_files()
    theme_map_files = _list_theme_map_files()

    with st.sidebar:
        st.subheader("运行参数")
        run_date = st.date_input("交易日", value=date.today()).strftime("%Y-%m-%d")
        top_n = st.slider("Top N", min_value=3, max_value=50, value=10, step=1)
        provider = st.selectbox("数据源", ["mock", "snapshot", "akshare"], index=2)
        snapshot_as_of = st.text_input(
            "snapshot_as_of (仅 snapshot 模式)", value="2026-01-20"
        ).strip()
        no_fallback = st.checkbox("失败不降级 (--no-fallback)", value=True)
        no_cache = st.checkbox("禁用缓存 (--no-cache)", value=False)
        theme_weight = st.slider("主题权重", min_value=0.0, max_value=2.0, value=1.0, step=0.1)
        signal_choice = st.selectbox(
            "signals 文件",
            options=[str(p.relative_to(ROOT_DIR)) for p in signal_files],
            index=_select_default(signal_files, "signals.yaml"),
        )
        theme_map_choice = st.selectbox(
            "theme_map 文件",
            options=[str(p.relative_to(ROOT_DIR)) for p in theme_map_files],
            index=_select_default(theme_map_files, "theme_to_industry_em_2026-01-20.csv"),
        )
        run_clicked = st.button("运行选股")

    if run_clicked:
        with st.spinner("正在执行策略..."):
            try:
                result = _run_screener(
                    run_date=run_date,
                    top_n=top_n,
                    provider=provider,
                    no_fallback=no_fallback,
                    no_cache=no_cache,
                    snapshot_as_of=snapshot_as_of if snapshot_as_of else None,
                    signals_path=ROOT_DIR / signal_choice,
                    theme_map_path=ROOT_DIR / theme_map_choice,
                    theme_weight=float(theme_weight),
                )
                st.session_state["last_result"] = result
            except Exception as exc:
                st.error(f"执行失败: {exc}")
                return

    result: RunResult | None = st.session_state.get("last_result")
    if result is None:
        st.info("请在左侧设置参数并点击“运行选股”。")
        return

    st.code(" ".join(result.command), language="bash")

    if result.returncode != 0:
        st.error(f"运行失败，退出码 {result.returncode}")
        if result.stderr:
            st.code(result.stderr, language="text")
        if result.stdout:
            with st.expander("标准输出"):
                st.code(result.stdout, language="text")
        return

    if not result.json_path.exists():
        st.error(f"未找到输出文件: {result.json_path}")
        return

    report = json.loads(result.json_path.read_text(encoding="utf-8"))
    meta = report.get("meta", {})
    warnings = report.get("debug", {}).get("warnings", [])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("As Of", str(report.get("as_of", "")))
    c2.metric("Top N", int(report.get("top_n", 0)))
    c3.metric("结果数", int(report.get("count", 0)))
    c4.metric("Issues", int(report.get("issues", 0)))

    if warnings:
        st.warning(" | ".join([str(w) for w in warnings]))

    if isinstance(meta, dict) and meta.get("provider_fallback"):
        st.info(f"provider_fallback: {meta.get('provider_fallback_reason', 'unknown')}")

    table = _results_table(report)
    st.subheader("Top N 结果")
    st.dataframe(table, use_container_width=True, hide_index=True)

    st.subheader("下载产物")
    st.download_button(
        "下载 JSON 报告",
        data=result.json_path.read_bytes(),
        file_name=result.json_path.name,
        mime="application/json",
    )
    if result.csv_path.exists():
        st.download_button(
            "下载 CSV 报告",
            data=result.csv_path.read_bytes(),
            file_name=result.csv_path.name,
            mime="text/csv",
        )

    with st.expander("运行日志"):
        st.code(result.stdout.strip() or "(empty)", language="text")
        if result.stderr.strip():
            st.code(result.stderr.strip(), language="text")

    with st.expander("完整 JSON"):
        st.json(report)


if __name__ == "__main__":
    main()
