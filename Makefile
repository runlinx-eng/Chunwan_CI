PY?=python3

snapshot:
	@test -n "$(AS_OF)" || (echo "AS_OF is required" && exit 1)
	@test -n "$(CONCEPTS)" || (echo "CONCEPTS is required" && exit 1)
	$(PY) scripts/fetch_snapshot.py --as-of $(AS_OF) --source em --concepts $(CONCEPTS)

run:
	@test -n "$(DATE)" || (echo "DATE is required" && exit 1)
	@test -n "$(TOP)" || (echo "TOP is required" && exit 1)
	@if [ -n "$(SNAPSHOT_AS_OF)" ]; then \
		$(PY) -m src.run --date $(DATE) --top $(TOP) --provider snapshot --no-fallback --snapshot-as-of $(SNAPSHOT_AS_OF); \
	else \
		$(PY) -m src.run --date $(DATE) --top $(TOP); \
	fi

verify:
	bash specpack/verify_all.sh

strategy-effectiveness:
	bash specpack/strategy_effectiveness/verify.sh

git-guard:
	bash tools/git_guard.sh --require-prefix --require-upstream

real-data-probe:
	@test -n "$(DATE)" || (echo "DATE is required" && exit 1)
	./.venv/bin/python tools/probe_real_data_chain.py --date "$(DATE)" --top $(if $(TOP),$(TOP),3)

real-data-replay:
	@test -n "$(DATES)" || (echo "DATES is required" && exit 1)
	./.venv/bin/python tools/run_real_data_replay.py --dates "$(DATES)" --top $(if $(TOP),$(TOP),3)

release:
	bash tools/run_release_pipeline.sh $(if $(SNAPSHOTS),--snapshots "$(SNAPSHOTS)",) $(if $(TOP_N),--top-n "$(TOP_N)",)
