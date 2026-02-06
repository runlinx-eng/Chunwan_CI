# P11 Release Checklist (GitHub Finalization)

目的：把“本地可运行”收口到“GitHub 可复现、可审计、可回滚”。

## 1) 本地验收（必须全过）

- [x] 分支检查：`bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean`
- [x] 发布流水线：`bash tools/run_release_pipeline.sh`
- [x] 策略有效性门禁：`bash specpack/strategy_effectiveness/verify.sh`

## 2) 提交与推送

- [x] 暂存变更：`git add <changed-files>`
- [x] 提交：`git commit -m "chore: finalize p11 release closure"`
- [x] 推送并设置 upstream：`git push -u origin <your-branch>`

## 3) 发起 PR

- [x] 目标分支：`main`
- [x] PR 描述必须包含：
  - [x] 本次变更范围（P8-P10/P11）
  - [x] 本地门禁结果（关键命令 + 结果）
  - [x] 风险与回滚方案
  - [x] 产物路径（`artifacts_metrics/*`）

PR 信息（2026-02-06）：
- PR: `https://github.com/runlinx-eng/Chunwan_CI/pull/1`

## 4) GitHub CI 通过

- [x] `ci_smoke` 通过
- [x] `phase10_verify` 通过
- [x] （如触发）`release_bundle` 通过

当前状态（2026-02-06）：
- PR 已合并到 `main`，`ci_smoke` / `release_bundle` / `phase10_verify` 均通过。

建议命令（已登录 gh CLI 时）：

```bash
gh pr checks <PR_NUMBER>
gh run list --limit 20
```

## 5) 合并与合并后核验

- [x] PR 已 merge 到 `main`
- [x] 拉取主分支后重新执行：
  - [x] `bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean`
  - [x] `bash tools/run_release_pipeline.sh`

复验说明（2026-02-06）：
- 由于本机 `main` 分支已在另一个 worktree 使用，本次在 `origin/main` 基线分支 `codex/p11-main-verify` 完成等价复验。

## 6) 交付留痕（写入看板/交接）

- [x] PR 链接
- [x] 关键 CI Run ID
- [x] merge commit SHA
- [x] 最终产物指针（例如 `artifacts_metrics/strategy_effectiveness_latest.json`）

留痕信息（2026-02-06）：
- PR: `https://github.com/runlinx-eng/Chunwan_CI/pull/1`
- Merge SHA: `5b8aca2d76b1544b7bed128222616ea17024639c`
- CI Runs:
  - `ci_smoke` on PR: `21745154280`
  - `release_bundle` on PR: `21745154256`
  - `ci_smoke` on main (post-merge): `21750195343`
  - `phase10_verify` on main: `21750613821`
- Final artifacts:
  - `artifacts_metrics/strategy_effectiveness_latest.json`
  - `artifacts_metrics/regression_matrix_timeseries_latest.json`

> 完成 1-6 后，才算 P11 完成。
