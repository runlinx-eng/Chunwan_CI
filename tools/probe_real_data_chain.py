#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def _git_rev(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return out
    except Exception:
        return ""


def _classify(stderr_text: str, stdout_text: str, return_code: int) -> str:
    if return_code == 0:
        return "ok"
    text = f"{stderr_text}\n{stdout_text}"
    patterns = {
        "dependency_missing": [
            r"No module named",
            r"akshare not available",
            r"ModuleNotFoundError",
        ],
        "network_blocked": [
            r"NameResolutionError",
            r"Failed to resolve",
            r"nodename nor servname provided",
            r"ConnectionError",
            r"Max retries exceeded",
            r"getaddrinfo",
        ],
        "ssl_or_cert_error": [
            r"SSLError",
            r"CERTIFICATE_VERIFY_FAILED",
            r"TLS",
            r"SSL",
        ],
        "provider_rate_limit": [
            r"429",
            r"Too Many Requests",
            r"rate limit",
        ],
    }
    for failure_type, exprs in patterns.items():
        for expr in exprs:
            if re.search(expr, text, flags=re.IGNORECASE):
                return failure_type
    return "unknown_runtime_error"


def _hint(failure_type: str) -> str:
    hints: Dict[str, str] = {
        "ok": "akshare real-data path is reachable and executable",
        "dependency_missing": "install missing dependencies in .venv (pip install -r requirements.txt)",
        "network_blocked": "check DNS/network policy; current runtime cannot reach EastMoney endpoints",
        "ssl_or_cert_error": "check python ssl stack and system cert chain",
        "provider_rate_limit": "retry later or add provider-side throttle/backoff",
        "unknown_runtime_error": "inspect stderr excerpt for stack trace root cause",
    }
    return hints.get(failure_type, "inspect probe output")


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe akshare real-data runtime and classify failure")
    parser.add_argument("--date", required=True, help="as-of date YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=3, help="top N")
    parser.add_argument("--python-bin", default="", help="python executable for src.run")
    parser.add_argument(
        "--out",
        default="artifacts_metrics/real_data_probe_latest.json",
        help="output json path",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when probe status is not ok",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    python_bin = args.python_bin.strip() or str(repo_root / ".venv" / "bin" / "python")
    cmd: List[str] = [
        python_bin,
        "-m",
        "src.run",
        "--date",
        args.date,
        "--top",
        str(args.top),
        "--provider",
        "akshare",
        "--no-fallback",
        "--no-cache",
    ]
    started_at = datetime.now(timezone.utc)
    run = subprocess.run(
        cmd,
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    finished_at = datetime.now(timezone.utc)

    failure_type = _classify(run.stderr or "", run.stdout or "", run.returncode)
    status = "ok" if failure_type == "ok" else "failed"
    payload = {
        "created_at": finished_at.isoformat().replace("+00:00", "Z"),
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "finished_at": finished_at.isoformat().replace("+00:00", "Z"),
        "git_rev": _git_rev(repo_root),
        "status": status,
        "failure_type": failure_type,
        "hint": _hint(failure_type),
        "command": cmd,
        "return_code": run.returncode,
        "stdout_excerpt": (run.stdout or "")[-4000:],
        "stderr_excerpt": (run.stderr or "")[-4000:],
    }

    out_path = (repo_root / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    ts = finished_at.strftime("%Y%m%d_%H%M%S")
    history_path = out_path.parent / f"real_data_probe_{ts}.json"
    history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[real_data_probe] status={status} failure_type={failure_type} out={out}".format(
            status=status, failure_type=failure_type, out=out_path
        )
    )

    if args.strict and status != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
