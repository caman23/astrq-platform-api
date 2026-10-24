# ASTRQ Stock Tools Platform
# `make up` builds the dashboard and runs everything in one process on :8000.

PY      ?= python3
VENV    := .venv
BIN     := $(VENV)/bin
SPEED   ?= 30

.PHONY: install up dev backend frontend test lint typecheck check simulate backtest replay docker-up clean ssi-check ssi-universe dnse-check

$(BIN)/python:
	$(PY) -m venv $(VENV) || $(PY) -m venv --without-pip $(VENV)
	@if [ ! -x $(BIN)/pip ]; then $(PY) -m pip --python $(BIN)/python install -q -U pip; fi

install: $(BIN)/python
	$(BIN)/python -m pip install -q -e "backend[dev]"
	cd frontend && npm install --no-audit --no-fund

frontend/dist/index.html: $(shell find frontend/src -type f) frontend/index.html
	cd frontend && npm run build

up: frontend/dist/index.html
	$(BIN)/astrq serve --speed $(SPEED) --plain-logs

# Hot-reload frontend on :5173 (proxying to the backend on :8000)
dev:
	@echo "backend on :8000, dashboard on http://localhost:5173"
	$(BIN)/astrq serve --speed $(SPEED) --plain-logs & \
	cd frontend && npm run dev; kill %1

backend:
	$(BIN)/astrq serve --speed $(SPEED) --plain-logs

frontend:
	cd frontend && npm run dev

test:
	cd backend && ../$(BIN)/pytest -q -p no:logging

lint:
	cd backend && ../$(BIN)/ruff check astrq tests && ../$(BIN)/ruff format --check astrq tests

typecheck:
	cd backend && ../$(BIN)/mypy astrq
	cd frontend && npm run typecheck

check: lint typecheck test

# Record a full synthetic trading day to JSONL (input for replay/backtest)
simulate:
	$(BIN)/astrq simulate --out data/ticks/replay.jsonl

backtest:
	$(BIN)/astrq backtest --input data/ticks/replay.jsonl

# Run the dashboard on recorded events instead of the live synthetic feed
replay: frontend/dist/index.html
	ASTRQ_PROVIDER__KIND=replay $(BIN)/astrq serve --plain-logs

# DNSE LightSpeed API: test credentials (WebSocket auth) + 30 s stream sample
dnse-check:
	$(BIN)/astrq dnse-check --listen 30

# SSI FastConnect Data: test credentials + 30 s stream sample; write official VN100 list
ssi-check:
	$(BIN)/astrq ssi-check --listen 30

ssi-universe:
	$(BIN)/astrq ssi-universe --index VN100

docker-up:
	docker compose up --build

clean:
	rm -rf frontend/dist data/snapshots
