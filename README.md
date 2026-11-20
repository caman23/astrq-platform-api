# ASTRQ — Stock Tools Platform

Read-only, realtime analysis tools for Vietnamese equities. Tool #1 is the
**Large Order Radar** (Radar Lệnh Lớn) for the VN100 basket: it detects single
block trades and split same-side parent orders tick by tick, scores them, and
shows them on a sweeping radar screen. Hovering a blip shows each large order
(time window, side, value, trade count, VWAP, price impact), its child trades and what
the price did 5/15/30 minutes after it.

Signals: block trades and split parent orders, **absorption** (one-sided large flow the
price does not follow), **stealth accumulation/distribution** (regular medium-sized
same-side trades below the large-trade bar), auction and put-through blocks. The
dashboard adds an accuracy table (share of orders whose price moved their way), a
timeline to replay the day, a sector heatmap of net large-order flow and a market
flow chart (buy/sell per 1/5/15/30 minutes + cumulative net). Viewers can star symbols
into a watchlist (filter ★), get sound + desktop notifications for alerts (all or
watchlist only), and export the VN100 table as CSV — all kept in the browser.

**Indicator screener (Lọc chỉ báo):** a second page (`/?view=screener`) filters VN100 by
conditions on daily-bar indicators — MA/EMA, RSI, MACD (line/signal/hist), Bollinger
(bands, %B), Stochastic %K, ATR %, % change, volume vs average, average traded value,
distance from n-day high/low — compared with a number or another indicator (`>`, `≥`, `<`,
`≤`, crosses above/below), all or any. Presets for common screens; results sortable,
exportable, one click opens the symbol on the radar. It runs **only when asked** (not
realtime); the large-order radar stays the default page. Daily bars come from DNSE OpenAPI
`GET /price/ohlc` (resolution `1D`, signed with the same API key as the feed; ~100 requests
for a full scan, cached 10 min in session / 6 h after the close) or a deterministic
synthetic history when no DNSE key is set. `astrq screener-check --symbol FPT` tests the
DNSE bars endpoint.

**Past sessions:** every tool snapshot also writes a compact per-day archive
(`data/snapshots/archive/<tool>/YYYY-MM-DD.json.gz`: day totals + follow-through of every
order, ~50 KB); the last one written after the close is the end-of-day snapshot. With the
day's recorded signals it lets the dashboard reopen any session still within
`storage.retention_days` (date picker next to the title, or `/?date=YYYY-MM-DD`) — radar,
timeline, sectors, flow, accuracy table. Outside trading hours an empty live radar opens the
last session automatically. On disk-less hosts the archive and signals go to R2 and are
fetched back on demand.

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

Requirements: Rust (stable, via rustup), Node ≥ 20.

```bash
./run.sh            # one command: installs on first run, builds the UI, runs the feed from .env
./run.sh --demo     # synthetic market (no key needed, works outside trading hours)
./run.sh --check    # verify DNSE key + 30 s stream sample
```

Or step by step:

```bash
make install        # fetch Rust crates + frontend deps
make up             # build dashboard, run backend + dashboard on http://127.0.0.1:8000
```

The default provider is a deterministic synthetic market running 30× faster than
real time and starting just before the ATO match, so the radar fills up within
seconds. `make up SPEED=60` runs faster.

Other targets:

| Command | What it does |
|---|---|
| `make dev` | backend on :8000 + Vite hot reload on :5173 |
| `make test` / `make lint` / `make check` | cargo test · rustfmt + clippy (-D warnings) + tsc |
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
| `timeline` | `1` shows a slider under the radar to scrub or replay today's session | off |

### Sector heatmap (second iframe)

```html
<iframe src="https://radar.astrq.io.vn/embed/sectors?compact=1"
        width="100%" height="460" style="border:0" loading="lazy"></iframe>
```

Net large-order flow (aggressive large buys − large sells) per sector; tiles are
coloured by direction and shaded by size, with the strongest symbols listed. Options:
`header=0`, `bg=transparent`, `click=none`, `compact=1`.

Only sites listed in `api.frame_ancestors` (config/settings.yaml) may embed it — the
browser blocks every other site (CSP `frame-ancestors`). Default: `astrq.io.vn` and its
subdomains.

## Layout

```
backend-rs/src/
  core/        models, config, bus, ingest, runner (isolation), alerts, platform/scheduler,
               sessions, market rules, metrics, recorder, R2 remote store, offline harness
  providers/   DNSE LightSpeed, SSI FastConnect, synthetic, replay, recorded baseline
  tools/       large_order_radar/ (tool #1), tick_counter/ (sample plugin)
  api/         axum REST + WebSocket, serves frontend/dist
  main.rs      CLI: serve · simulate · backtest · dnse-check · ssi-check · ssi-universe · r2-check
backend-rs/tests/ golden replay scenarios (parity with v1), isolation, vendors, API
frontend/      React + Vite + TypeScript radar dashboard
config/        settings.yaml, universe_vn100.yaml (sample — NOT the official basket)
docs/          architecture, ADRs (0006: Rust rewrite), provider notes, writing-a-tool
```

## API

Interactive docs: **Swagger UI at `/docs`**, OpenAPI 3.1 spec at `/openapi.json` (generated from the handlers with utoipa; `api.docs_enabled: false` turns both off).

| Endpoint | |
|---|---|
| `GET /docs` · `GET /openapi.json` | Swagger UI · OpenAPI spec |
| `GET /health`, `GET /ready`, `GET /metrics` | liveness, readiness, Prometheus text |
| `GET /api/status` | clock, session, feed status, tick rate, feed vs internal latency, tools |
| `GET /api/tools` · `POST /api/tools/{id}/enabled` | manifests/config/status · enable/disable |
| `GET /api/tools/large_order_radar/view` | radar overview for today: newest 5 orders per symbol, last 8 trades of the newest one, each order's `outcome` (price 5/15/30 min later and now) and the `stats` accuracy table. `?symbol=X`: everything for X · `?full=1`: v1-sized document · `?at=HH:MM`: the radar as it was at that time · `?sectors=1`: per-sector heatmap data · `?flow=1&bucket=5`: market-wide large buy/sell per bucket (1·5·15·30 min) with cumulative net, also with `at` |
| `GET /api/signals?symbol=&side=&min_severity=&limit=&date=` | today's signals (or a past session's) |
| `GET /api/screener/meta` · `POST /api/screener/run` | indicator catalog + presets · run a scan: `{"conditions": [{"left": {"ind": "rsi", "n": 14}, "op": "lt", "right": {"value": 30}}], "logic": "all", "symbols": null}` (≤ 8 conditions, body ≤ 16 KB, results cached 60 s, ≤ 2 scans at once) |
| `GET /api/history` | `live_date` and the past sessions that can be viewed (`dates`) |
| any view above `&date=YYYY-MM-DD` | the same view for a past session (`archived: true`) |
| `WS /ws` | `hello`, `status` (1 s), `signal`, `alert`, `summary` messages |

See `docs/architecture.md` for the design and `docs/writing-a-tool.md` to add a tool.

## Security & abuse limits

Read-only service; there is no trading endpoint and nothing user-supplied reaches a file
path, shell, or outbound URL (dates are parsed, symbols must be in the universe). Large
floods are Cloudflare's job — keep the origin reachable only through Cloudflare (tunnel,
or `api.origin_secret` on Render). The process itself also defends (`api.limits` in
`config/settings.yaml`, all on by default):

| Threat | Defense |
|---|---|
| One client hammering the API | token bucket per IP (15 req/s, burst 90; IPv6 per /64) → `429` + `Retry-After`; `/health` exempt |
| Spoofed client IP | `CF-Connecting-IP` / `X-Forwarded-For` only believed from the local tunnel, a private-network proxy, requests carrying the origin secret, or with `api.trust_proxy_headers` |
| Many slow requests / overload | ≤ 256 requests in flight (`503` beyond), 20 s response timeout |
| Cache-busting queries | unknown params dropped, numbers clamped, `at` → `HH:MM`, unknown symbols `404`; per-key single flight; ≤ 4 concurrent builds, `503` after 8 s wait; 32 MB cache cap |
| Slow-loris / socket exhaustion | 10 s header-read timeout, ≤ 4096 connections (extra ones closed at once), HTTP/2 ≤ 64 streams |
| WebSocket floods | ≤ 2000 clients, ≤ 8 per IP, 4 KB max frame, 10 s send timeout |
| Internals exposure | `/metrics` only for direct local requests or `api.metrics_token` |
| Clickjacking / XSS hardening | CSP (`script-src 'self'`, `object-src 'none'`, `frame-ancestors` allow-list), `nosniff`, `Referrer-Policy`, `Permissions-Policy`, `COOP` |

Recommended on Cloudflare (free plan): a WAF rate-limiting rule on `/api/*` and `/ws`
(e.g. 120 requests / 10 s per IP → block 1 min), Bot Fight Mode, and the origin secret
transform rule from `docs/deploy-render.md`.
