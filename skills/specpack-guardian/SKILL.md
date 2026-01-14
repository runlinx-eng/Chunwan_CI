---
name: specpack-guardian
description: Maintain and enforce specpack validation for the A-share screener. Use when modifying data sources, mappings, scoring logic, or output schema so assertions and verification scripts stay in sync and are run before changes are accepted.
---

# Specpack Guardian

Follow this workflow whenever data sources, mappings, scoring, or output fields change:

1) Update spec definitions
- Edit `specpack/*/SPEC.md` to reflect new behavior or expectations.
- Edit `specpack/*/assertions.yaml` to capture the core targets:
  - no future data
  - TopN row count
  - explanation fields in `reason`
  - core theme convergence (3-5)
  - snapshot reproducibility (when applicable)

2) Update verification scripts
- Ensure `specpack/*/verify.sh` enforces the updated assertions.
- Keep the scripts runnable with one command:
  - `bash specpack/mvp_smoke/verify.sh`
  - `bash specpack/snapshot_replay/verify.sh`
  - `bash specpack/verify_all.sh`

3) Run verification before finalizing changes
- Execute `bash specpack/verify_all.sh`.
- If a pack fails, adjust code or assertions until all packs pass.

Guidance
- Keep assertions strict but stable across runs.
- Use snapshot fixtures for reproducibility checks.
- Preserve backward compatibility in outputs; only add fields when necessary.
