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
- [ ] `phase10_verify` 通过
- [x] （如触发）`release_bundle` 通过

当前状态（2026-02-06）：
- PR 校验和 `release_bundle` 已通过，但 PR 仍显示 `mergeStateStatus=BLOCKED`。

建议命令（已登录 gh CLI 时）：

```bash
gh pr checks <PR_NUMBER>
gh run list --limit 20
```

## 5) 合并与合并后核验

- [ ] PR 已 merge 到 `main`
- [ ] 拉取主分支后重新执行：
  - [ ] `bash tools/git_guard.sh --strict --require-prefix --require-upstream --require-clean`
  - [ ] `bash tools/run_release_pipeline.sh`

## 6) 交付留痕（写入看板/交接）

- [ ] PR 链接
- [ ] 关键 CI Run ID
- [ ] merge commit SHA
- [ ] 最终产物指针（例如 `artifacts_metrics/strategy_effectiveness_latest.json`）

> 完成 1-6 后，才算 P11 完成。
