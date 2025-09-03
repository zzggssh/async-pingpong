PY ?= python3
RUN ?= $(PY) scripts/run_all.py
PORT ?= 9999

.PHONY: run_5m run_15s clean

run_5m:
	$(RUN) --duration 300 --port $(PORT)

run_15s:
	$(RUN) --duration 15 --port $(PORT)

clean:
	rm -f logs/*.log || true
