# Everything this project does, from a clone.
#
#   make setup      install dependencies into .venv
#   make demo       build the warehouse from the committed sample and show it
#   make poll       start collecting (Ctrl-C to stop)
#   make build      build the warehouse from whatever has been collected
#   make test       dbt tests + pytest
#   make rain       the weather regression
#   make sensitivity  how much the cancellation threshold moves the answer
#
# `make demo` is the one to run first: it needs no cloud account, no
# credentials and no waiting, because a sample of real observations is
# committed.

PY := .venv/Scripts/python.exe
ifeq (,$(wildcard .venv/Scripts/python.exe))
PY := .venv/bin/python
endif

DBT := $(PY) -m dbt.cli.main
DBT_ARGS := --profiles-dir .

.PHONY: setup demo poll build test dbt-test pytest rain sensitivity seeds docs clean

setup:
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

# --------------------------------------------------------------------------
# The five-minute version: real data, committed, no waiting.
# --------------------------------------------------------------------------
demo:
	cd transform && MTA_RAW_GLOB='../data/sample/**/*.ndjson.gz' \
	  MTA_DUCKDB='../data/demo.duckdb' $(abspath $(PY)) -m dbt.cli.main build $(DBT_ARGS)
	@$(PY) scripts/summarise.py data/demo.duckdb

# --------------------------------------------------------------------------
# The real thing.
# --------------------------------------------------------------------------
poll:
	cd ingest && $(abspath $(PY)) poller.py

seeds:
	cd ingest && $(abspath $(PY)) static_gtfs.py

build:
	cd transform && $(abspath $(PY)) -m dbt.cli.main build $(DBT_ARGS)
	@$(PY) scripts/summarise.py data/warehouse.duckdb

# --------------------------------------------------------------------------
# Checks.
# --------------------------------------------------------------------------
test: dbt-test pytest

dbt-test:
	cd transform && $(abspath $(PY)) -m dbt.cli.main test $(DBT_ARGS)

pytest:
	$(PY) -m pytest tests -q

# --------------------------------------------------------------------------
# Analysis.
# --------------------------------------------------------------------------
rain:
	$(PY) -m analysis.rain_regression

sensitivity:
	$(PY) -m analysis.sensitivity

docs:
	cd transform && $(abspath $(PY)) -m dbt.cli.main docs generate $(DBT_ARGS)
	@echo "Then: cd transform && dbt docs serve --profiles-dir ."

clean:
	rm -rf transform/target transform/logs data/demo.duckdb data/sensitivity.duckdb
