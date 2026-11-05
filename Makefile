# ASTRQ Stock Tools Platform (Rust backend + React dashboard)
# `make up` builds everything and runs backend + dashboard in one process on :8000.

export PATH := $(HOME)/.cargo/bin:$(PATH)
SPEED   ?= 30
BIN     := backend-rs/target/release/astrq
CARGO   := cd backend-rs && cargo

.PHONY: install build up dev backend frontend test lint fmt check simulate backtest replay \
        docker-up clean ssi-check ssi-universe dnse-check r2-check

install:
	cd frontend && npm install --no-audit --no-fund
	$(CARGO) fetch

build:
	$(CARGO) build --release
	cd frontend && npm run build

$(BIN): $(shell find backend-rs/src -type f) backend-rs/Cargo.toml
	$(CARGO) build --release

frontend/dist/index.html: $(shell find frontend/src -type f) frontend/index.html
	cd frontend && npm run build

up: $(BIN) frontend/dist/index.html
	$(BIN) serve --speed $(SPEED) --plain-logs

# Hot-reload frontend on :5173 (proxying to the backend on :8000)
dev: $(BIN)
	@echo "backend on :8000, dashboard on http://localhost:5173"
	$(BIN) serve --speed $(SPEED) --plain-logs & \
	cd frontend && npm run dev; kill %1

backend: $(BIN)
	$(BIN) serve --speed $(SPEED) --plain-logs

frontend:
	cd frontend && npm run dev

test:
	$(CARGO) test

lint:
	$(CARGO) fmt --check
	$(CARGO) clippy --all-targets -- -D warnings
	cd frontend && npm run typecheck

fmt:
	$(CARGO) fmt

check: lint test

# Record a full synthetic trading day to JSONL (input for replay/backtest)
simulate: $(BIN)
	$(BIN) simulate --out data/ticks/replay.jsonl

backtest: $(BIN)
	$(BIN) backtest --input data/ticks/replay.jsonl

# Run the dashboard on recorded events instead of the live feed
replay: $(BIN) frontend/dist/index.html
	ASTRQ_PROVIDER__KIND=replay $(BIN) serve --plain-logs

# DNSE LightSpeed API: test credentials (WebSocket auth) + 30 s stream sample
dnse-check: $(BIN)
	$(BIN) dnse-check --listen 30

# SSI FastConnect Data: test credentials + 30 s stream sample; write official VN100 list
ssi-check: $(BIN)
	$(BIN) ssi-check --listen 30

ssi-universe: $(BIN)
	$(BIN) ssi-universe --index VN100

r2-check: $(BIN)
	ASTRQ_STORAGE__REMOTE__KIND=r2 $(BIN) r2-check

docker-up:
	docker compose up --build

clean:
	rm -rf frontend/dist data/snapshots
	$(CARGO) clean
