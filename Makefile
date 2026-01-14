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
