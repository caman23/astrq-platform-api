# ASTRQ — Stock Tools Platform

Read-only, realtime analysis tools for Vietnamese equities. Tool #1 is the
**Large Order Radar** (Radar Lệnh Lớn) for the VN100 basket: it detects single
block trades and split same-side parent orders tick by tick, scores them, and
shows them on a sweeping radar screen. Hovering a blip shows each large order
(time window, side, value, trade count, VWAP, price impact) and its child trades.

> Thông tin chỉ mang tính tham khảo, không phải khuyến nghị đầu tư.
> The system never places orders or touches trading accounts.

**Status:** everything runs end-to-end on **synthetic and replayed data**. Two real-feed
adapters — **DNSE LightSpeed API** (default choice, online registration) and **SSI
FastConnect Data** — are implemented from the vendors' official docs and tested against
local fake servers, but have **not yet run against a live feed**. Follow the
first-session checklists in `docs/provider-notes.md`.

### Using DNSE LightSpeed API

1. Register online: developers.dnse.com.vn → Đăng ký → EntradeX → OTP → copy API Key + Secret.
2. Fill `ASTRQ_PROVIDER__DNSE__API_KEY` / `..._API_SECRET` in `.env` (already set to `kind=dnse`).
3. `make dnse-check` (during trading hours) — auth + 30 s stream sample.
4. `make up`.

### Using SSI FastConnect Data (registration via SSI counter / broker)

1. Create a ConsumerID/ConsumerSecret on SSI iBoard (FastConnect Data service).
2. In `.env`: `ASTRQ_PROVIDER__SSI__CONSUMER_ID=…`, `ASTRQ_PROVIDER__SSI__CONSUMER_SECRET=…`
3. `make ssi-check` — token, VN100 lookup and a 30 s stream sample (best during trading hours).
4. `make ssi-universe` — write the official VN100 list to `config/universe_vn100.yaml`.
5. In `.env`: `ASTRQ_PROVIDER__KIND=ssi`, `ASTRQ_CONTEXT__KIND=recorded`,
   `ASTRQ_SESSIONS__ENFORCE_TRADING_DAYS=true`, then `make up`.

## Quick start

Requirements: Python ≥ 3.12, Node ≥ 20.

```bash
./run.sh            # one command: installs on first run, builds the UI, runs the feed from .env
./run.sh --demo     # synthetic market (no key needed, works outside trading hours)
./run.sh --check    # verify DNSE key + 30 s stream sample
```

Or step by step:

```bash
make install        # venv + backend deps + frontend deps
make up             # build dashboard, run backend + dashboard on http://127.0.0.1:8000
```

The default provider is a deterministic synthetic market running 30× faster than
real time and starting just before the ATO match, so the radar fills up within
seconds. `make up SPEED=60` runs faster.

Other targets:

| Command | What it does |
|---|---|
| `make dev` | backend on :8000 + Vite hot reload on :5173 |
| `make test` / `make lint` / `make typecheck` / `make check` | tests, ruff, mypy (strict) + tsc |
| `make simulate` | write a full synthetic trading day to `data/ticks/replay.jsonl` |
| `make backtest` | run the radar offline over that file and print signal statistics |
| `make replay` | serve the dashboard from recorded events instead of the synthetic feed |
| `make docker-up` | Docker Compose (not yet tested; Docker was unavailable while building) |

Telegram alerts: copy `.env.example` to `.env` and set
`ASTRQ_ALERTS__TELEGRAM__ENABLED=true`, `..._BOT_TOKEN`, `..._CHAT_ID`.
Bot commands: `/status`, `/on <tool>`, `/off <tool>`, `/min <info|notice|alert>`.

## Embedding the radar (iframe)

```html
<iframe src="https://radar.astrq.io.vn/embed?min=notice"
        title="Radar Lệnh Lớn" style="width:100%;max-width:560px;height:620px;border:0"
        loading="lazy"></iframe>
```

| Param | Values | Default |
|---|---|---|
| `side` | `all` · `buy` · `sell` | `all` |
| `min` | `info` · `notice` · `alert` (minimum severity shown) | `notice` |
| `symbols` | comma list, e.g. `FPT,VCB,HPG` | all |
| `header` / `legend` | `0` hides it | `1` |
| `bg` | `transparent` | dark |
| `click` | `open` (open full dashboard for that symbol in a new tab) · `none` | `open` |

Only sites listed in `api.frame_ancestors` (config/settings.yaml) may embed it — the
browser blocks every other site (CSP `frame-ancestors`). Default: `astrq.io.vn` and its
subdomains.

## Layout

```
backend/astrq/
  core/        models, config, bus, ingest, runner (isolation), registry, alerts,
               scheduler/platform, sessions, market rules, metrics, recorder, offline harness
  providers/   synthetic, replay, vendor skeletons (DNSE/SSI)
  tools/       large_order_radar/ (tool #1), tick_counter/ (sample plugin)
  api/         FastAPI REST + WebSocket, serves frontend/dist
backend/tests/ unit, property (hypothesis), golden replay scenarios, isolation, API
frontend/      React + Vite + TypeScript radar dashboard
config/        settings.yaml, universe_vn100.yaml (sample — NOT the official basket)
docs/          architecture, ADRs, provider notes, open questions, writing-a-tool
```

## API

| Endpoint | |
|---|---|
| `GET /health`, `GET /ready`, `GET /metrics` | liveness, readiness, Prometheus text |
| `GET /api/status` | clock, session, feed status, tick rate, feed vs internal latency, tools |
| `GET /api/tools` · `POST /api/tools/{id}/enabled` | manifests/config/status · enable/disable |
| `GET /api/tools/large_order_radar/view[?symbol=X]` | aggregated radar view (today) |
| `GET /api/signals?symbol=&side=&min_severity=&limit=` | today's signals |
| `WS /ws` | `hello`, `status` (1 s), `signal`, `alert`, `summary` messages |

See `docs/architecture.md` for the design and `docs/writing-a-tool.md` to add a tool.
