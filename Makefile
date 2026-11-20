# astrq-platform-api — Rust backend (realtime feed, tools, REST + WebSocket API).
# The dashboard lives in astrq-platform-ui; by default it is expected next to this repo
# (../astrq-platform-ui, override with UI_DIR=...).

export PATH := $(HOME)/.cargo/bin:$(PATH)
SPEED   ?= 30
BIN     := target/release/astrq
UI_DIR  ?= ../astrq-platform-ui
UI_REPO ?= $(shell gh repo view --json owner -q .owner.login 2>/dev/null)/astrq-platform-ui
IMAGE   ?= astrq-platform-api:local

.PHONY: install build up dev backend test lint fmt check simulate backtest replay \
        ui-dist docker-build clean ssi-check ssi-universe dnse-check r2-check

install:
	cargo fetch

build:
	cargo build --release

$(BIN): $(shell find src -type f) Cargo.toml Cargo.lock
	cargo build --release

# Serve the dashboard too when the UI repo is checked out (and built) next to this one
UI_DIST   := $(wildcard $(UI_DIR)/dist/index.html)
SERVE_ENV := $(if $(UI_DIST),ASTRQ_API__FRONTEND_DIST=$(abspath $(UI_DIR))/dist,)

up: $(BIN)
	$(SERVE_ENV) $(BIN) serve --speed $(SPEED) --plain-logs

backend: up

# Backend on :8000 + Vite hot reload on :5173 (from the UI repo)
dev: $(BIN)
	@test -d $(UI_DIR) || (echo "clone astrq-platform-ui into $(UI_DIR) first" && false)
	@echo "backend on :8000, dashboard on http://localhost:5173"
	$(BIN) serve --speed $(SPEED) --plain-logs & \
	cd $(UI_DIR) && npm run dev; kill %1

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt

check: lint test

# Record a full synthetic trading day to JSONL (input for replay/backtest)
simulate: $(BIN)
	$(BIN) simulate --out data/ticks/replay.jsonl

backtest: $(BIN)
	$(BIN) backtest --input data/ticks/replay.jsonl

# Run on recorded events instead of the live feed
replay: $(BIN)
	ASTRQ_PROVIDER__KIND=replay $(SERVE_ENV) $(BIN) serve --plain-logs

# Dashboard release pinned in ui.version -> ./ui-dist (what the image ships)
ui-dist:
	rm -rf ui-dist
	mkdir -p ui-dist
	gh release download "$$(cat ui.version)" --repo $(UI_REPO) --pattern ui-dist.tar.gz --output - \
	  | tar -xz -C ui-dist

docker-build:
	mkdir -p ui-dist
	docker build -t $(IMAGE) .

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

clean:
	rm -rf ui-dist data/snapshots
	cargo clean
