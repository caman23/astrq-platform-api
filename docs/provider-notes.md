# Provider notes

Rule: a field is "verified" only after reading official docs or a captured real
message. Below, **doc** = official SSI guide (guide.ssi.com.vn, FastConnect Data:
Connection guide, API Specs, Streaming data); **src** = source of the official
`ssi-fc-data` 2.2.2 package (PyPI, by SSI, MIT); **live** = observed on real traffic.
Nothing is marked *live* yet.

## SSI FastConnect Data — implemented (`backend-rs/src/providers/ssi/`)

| Item | Status | Value / handling |
|---|---|---|
| Credentials | doc | ConsumerID + ConsumerSecret created on iBoard; env `ASTRQ_PROVIDER__SSI__CONSUMER_ID/SECRET` |
| Token | doc + src | `POST https://fc-data.ssi.com.vn/api/v2/Market/AccessToken` body `{consumerID, consumerSecret}` → `data.accessToken` (JWT); refreshed 1 h before `exp` (same as src) |
| Streaming transport | src | classic SignalR 1.5 at `https://fc-datahub.ssi.com.vn/v2.0/signalr`, hub `FcMarketDataV2Hub`, push method `Broadcast`, error method `Error` |
| Subscribe | doc + src | invoke `SwitchChannels("X-TRADE:FPT-VCB-…")` or `"X-TRADE:ALL"`; `-` separator (doc) |
| Message envelope | doc | `{"DataType": "X-TRADE", "Content": "<json string>"}`; key casing inconsistent → matched case-insensitively; trailing commas tolerated |
| Aggressor flag | doc | `Side`: `BU` = buy, `SD` = sell, other = unknown → `side_source=provider` |
| Trade fields | doc | `LastPrice`, `LastVol`, `TotalVol`, `TradingDate` (DD/MM/YYYY), `Time` (HH:MM:SS), `EstMatchedPrice` |
| Timestamp timezone | **assumed** | Vietnam local (documented sample is within VN trading hours). Second resolution. |
| Price unit | **assumed** (doc sample) | VND (sample Ceiling 24500). Counter `price_looks_like_thousands` + `price_multiplier` config |
| Volume unit | **unverified** | shares assumed; `volume_multiplier` config |
| Sequence numbers | not provided | `TotalVol` (cumulative) used to drop repeats and count `missed_volume`; `raw_ref=tv:<TotalVol>` for dedupe |
| TradingSession codes | **unverified** | mapping table page did not render; codes collected in `stats.sessions_seen`, mapped via `provider.ssi.session_map`; unmapped → session calendar |
| Put-through in X-TRADE | **unknown** | no flag documented → `is_negotiated=False`; put-through may be absent or mixed in — check against SSI board after the first session |
| Symbols per subscription / rate limits | **unknown** | not documented; fallback `subscribe_all=true` (filter locally) |
| Index constituents | doc | `GET api/v2/Market/IndexComponents?indexCode=VN100` → `data[].IndexComponent[].StockSymbol` (`astrq ssi-universe`) |
| Data storage/redistribution terms | **unknown** | we store locally only (ticks + raw messages, 14-day retention). Confirm with SSI before sharing anything. |

### Deliberate deviations from the official client

- TLS certificates are **verified** (the official client disables verification: `CERT_NONE`, `verify=False`).
- Headers/tokens are **never printed** (the official client `print`s the headers incl. the Bearer token).
- asyncio instead of threads; reconnect with exponential backoff and a silence watchdog
  (`idle_reconnect_sec`) during sessions; a 401 at negotiate forces a fresh token.

### First live session checklist

1. `astrq ssi-check --listen 60` during trading hours: confirm token, VN100 list, sample ticks.
2. Compare 3–5 ticks against the SSI iBoard time & sales: price unit, volume unit, time zone, side.
3. Copy `sessions_seen` codes into `provider.ssi.session_map` (config/settings.yaml).
4. Run a full session; check `missed_volume`, `repeat_dropped`, `hub_errors` in the System tab.
5. Keep a few raw lines from `data/raw/ssi/` as test fixtures (`backend-rs/tests/vendors.rs`).
6. Update this table: change *assumed/unverified* to *live*.

## DNSE LightSpeed API — implemented (`backend-rs/src/providers/dnse/`) — **default choice**

Sources: **doc** = developers.dnse.com.vn (intro/register_guide, market-data/connect,
sdk/build_websocket); **sdk** = `dnse` 0.5.0 on PyPI (github.com/dnse-tech). Where the
SDK contradicts the docs (subscribe payload without `action`, an `auth_error` action the
docs say does not exist, different field names) **the docs win**.

| Item | Status | Value / handling |
|---|---|---|
| Registration | doc | fully online: developers.dnse.com.vn → Đăng ký → EntradeX → OTP (Smart/Email) → API Key + Secret shown once |
| Fees | **unknown** | not stated in the guide; see DNSE fee schedule / LightSpeed product terms |
| Endpoint | doc | `wss://ws-openapi.dnse.com.vn/v1/stream?encoding=json` (msgpack also offered) |
| Auth | doc + sdk | `welcome` → `{"action":"auth",api_key,signature,timestamp,nonce}` within 30 s; signature = hex HMAC-SHA256(secret, `key:ts:nonce`); ts ±5 min; nonce = µs, never reused → `auth_success` or `error` (`AUTH_FAILED`, `MAX_CONNECTIONS_EXCEEDED`) |
| Subscribe | doc | `{"action":"subscribe","channels":[{"name":"tick_extra.G1.json","symbols":[...]}]}` → `subscribed` |
| Keep-alive / expiry | doc | server `ping` every 3 min → `pong` (echo timestamp) within 60 s; max 8 h per connection (`connection_expired`) → reconnect with new nonce |
| Limits | doc | 10 connections/user, 200 streams/connection (whether a stream = channel×symbol is **unverified**; `auth_success.rate_limit` is shown in stats) |
| Trade fields | doc | `symbol, boardId, matchPrice, matchQtty, side (tick_extra), totalVolumeTraded, tradingSessionId, time{Seconds,Nanos}` |
| Side values | **partly** | doc sample shows `SELL`; `BUY` assumed; counted in `stats.sides_seen` |
| Boards | doc | G1 round lot · G3 after-hours · G4 odd lot · **T1/T3 negotiated (put-through)** → `is_negotiated` |
| Price unit | **unverified** | doc samples look like thousand VND (24.35); `price_unit: auto` multiplies stock prices < 1000 by 1000 (`price_scaled_x1000`) |
| Session codes | **unverified** | docs only show `"40"`; collected in `sessions_seen`, mapped via `provider.dnse.session_map` |
| Data envelope | **unverified** | doc field names vs SDK names (`T`, `price`, `volume`, `timestamp`) — both accepted; `message_shapes` shows which one arrives |
| VN100 constituents | **not available** from what we read | keep `config/universe_vn100.yaml` (sample) or use `astrq ssi-universe` if you have SSI keys |

### First live session checklist (DNSE)

1. `make dnse-check` during trading hours: auth OK, sample ticks printed.
2. Compare a few ticks with the DNSE/EntradeX price board: price in VND, volume, time, side.
3. Read `message_shapes`, `sides_seen`, `sessions_seen` in the output / System tab; fill
   `provider.dnse.session_map`; set `price_unit` explicitly once confirmed.
4. If the subscribe is rejected for too many streams, reduce `provider.dnse.channels`
   (e.g. drop `expected_price.G1`) and report back — sharding across connections is next.
5. Keep raw lines from `data/raw/dnse/` as fixtures in `backend-rs/tests/vendors.rs`.
