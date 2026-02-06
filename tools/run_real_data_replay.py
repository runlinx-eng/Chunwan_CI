#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


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


def _git_rev(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay akshare runs across dates and summarize failure types")
    parser.add_argument("--dates", required=True, help="comma-separated as-of dates")
    parser.add_argument("--top", type=int, default=3, help="top N")
    parser.add_argument("--python-bin", default="", help="python executable for src.run")
    parser.add_argument(
        "--out",
        default="artifacts_metrics/real_data_replay_latest.json",
        help="output summary json path",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero if success_rate < min_success_rate",
    )
    parser.add_argument(
        "--min-success-rate",
        type=float,
        default=0.8,
        help="minimum success rate required when --strict is set",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    python_bin = args.python_bin.strip() or str(repo_root / ".venv" / "bin" / "python")
    dates = [item.strip() for item in args.dates.split(",") if item.strip()]
    if not dates:
        raise SystemExit("no dates provided")

    runs: List[Dict[str, object]] = []
    failure_hist: Dict[str, int] = {}
    success_count = 0

    for as_of in dates:
        started = datetime.now(timezone.utc)
        cmd = [
            python_bin,
            "-m",
            "src.run",
            "--date",
            as_of,
            "--top",
            str(args.top),
            "--provider",
            "akshare",
            "--no-fallback",
            "--no-cache",
        ]
        proc = subprocess.run(
            cmd,
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        finished = datetime.now(timezone.utc)
        failure_type = _classify(proc.stderr or "", proc.stdout or "", proc.returncode)
        status = "ok" if failure_type == "ok" else "failed"
        if status == "ok":
            success_count += 1
        failure_hist[failure_type] = failure_hist.get(failure_type, 0) + 1
        runs.append(
            {
                "date": as_of,
                "status": status,
                "failure_type": failure_type,
                "return_code": proc.returncode,
                "started_at": started.isoformat().replace("+00:00", "Z"),
                "finished_at": finished.isoformat().replace("+00:00", "Z"),
                "stderr_excerpt": (proc.stderr or "")[-1500:],
            }
        )

    total = len(runs)
    success_rate = float(success_count) / float(total) if total else 0.0
    global_status = "ok" if success_count == total else "degraded"
    if total > 0 and failure_hist.get("network_blocked", 0) == total:
        global_status = "blocked_by_environment"

    payload: Dict[str, object] = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "git_rev": _git_rev(repo_root),
        "total_runs": total,
        "success_count": success_count,
        "success_rate": success_rate,
        "global_status": global_status,
        "failure_histogram": failure_hist,
        "runs": runs,
    }

    out_path = (repo_root / args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    history = out_path.parent / f"real_data_replay_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    history.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        "[real_data_replay] status={status} success_rate={rate:.3f} out={out}".format(
            status=global_status, rate=success_rate, out=out_path
        )
    )

    if args.strict and success_rate < float(args.min_success_rate):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
