#![allow(clippy::result_large_err)] // tungstenite handshake callback signature
//! DNSE and SSI adapters against local fake servers. Fixtures follow the vendors'
//! official documentation (not captured traffic — extend with data/raw/* captures).

mod common;

use astrq::core::config::{DnseConfig, Secret, SessionsConfig, SsiConfig};
use astrq::core::models::{MarketEvent, Session, Side, SideSource};
use astrq::core::sessions::SessionCalendar;
use astrq::providers::dnse::parse::DnseNormalizer;
use astrq::providers::dnse::{build_auth_message, DnseProvider};
use astrq::providers::ssi::parse::{decode_broadcast, SsiNormalizer};
use astrq::providers::ssi::SsiProvider;
use astrq::providers::MarketDataProvider;
use base64::Engine;
use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;

fn cal() -> Arc<SessionCalendar> {
    Arc::new(SessionCalendar::new(&SessionsConfig::default()))
}

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 21, 3, 0, 0).unwrap()
}

const T_1015: i64 = 1_792_552_530; // 2026-10-21 10:15:30 VN

fn doc_tick(over: Value) -> Value {
    let mut m = json!({
        "marketId": "STO", "boardId": "G1", "isin": "VN000000FPT1", "symbol": "FPT",
        "matchPrice": 124.0, "matchQtty": 5000.0, "side": "BUY", "avgPrice": 123.8,
        "totalVolumeTraded": 800000, "grossTradeAmount": 99.0, "tradingSessionId": "40",
        "time": {"Seconds": T_1015, "Nanos": 72_000_000},
    });
    for (k, v) in over.as_object().unwrap() {
        m[k] = v.clone();
    }
    m
}

// ------------------------------------------------------------------ DNSE

#[test]
fn dnse_doc_and_sdk_shapes() {
    let mut n = DnseNormalizer::new(DnseConfig::default(), cal());
    let evs = n.events(&doc_tick(json!({})), now());
    let MarketEvent::Tick(t) = &evs[0] else {
        panic!()
    };
    assert_eq!(
        (t.symbol.as_str(), t.price, t.volume),
        ("FPT", 124_000.0, 5000)
    ); // auto x1000
    assert_eq!(
        (t.side, t.side_source, t.is_negotiated),
        (Side::Buy, SideSource::Provider, false)
    );
    assert_eq!(
        t.ts_exchange,
        Utc.timestamp_opt(T_1015, 72_000_000).unwrap()
    );
    assert_eq!(t.raw_ref.as_deref(), Some("G1:tv:800000"));

    let mut n2 = DnseNormalizer::new(
        DnseConfig {
            price_unit: "vnd".into(),
            ..DnseConfig::default()
        },
        cal(),
    );
    let sdk = json!({"T": "te", "symbol": "vcb", "price": 61500, "volume": 300, "side": "S", "timestamp": T_1015 * 1000, "total_volume": 1000});
    let MarketEvent::Tick(t) = &n2.events(&sdk, now())[0] else {
        panic!()
    };
    assert_eq!(
        (t.symbol.as_str(), t.price, t.side),
        ("VCB", 61500.0, Side::Sell)
    );
    assert_eq!(n2.shapes_seen.get("trade:sdk"), Some(&1));
}

#[test]
fn dnse_putthrough_repeats_session_map_expected_price() {
    let mut n = DnseNormalizer::new(DnseConfig::default(), cal());
    let MarketEvent::Tick(pt) = &n.events(
        &doc_tick(json!({"boardId": "T1", "side": "", "totalVolumeTraded": 500000})),
        now(),
    )[0] else {
        panic!()
    };
    assert!(pt.is_negotiated && pt.side == Side::Unknown);
    assert_eq!(n.events(&doc_tick(json!({})), now()).len(), 1);
    assert!(n.events(&doc_tick(json!({})), now()).is_empty()); // repeat on G1
    assert_eq!(n.stats.get("repeat_dropped"), Some(&1));

    let mut sm = DnseConfig::default();
    sm.session_map.insert("40".into(), Session::ContinuousAm);
    let MarketEvent::Tick(t) =
        &DnseNormalizer::new(sm, cal()).events(&doc_tick(json!({})), now())[0]
    else {
        panic!()
    };
    assert_eq!(t.session, Session::ContinuousAm);

    let ato = Utc
        .with_ymd_and_hms(2026, 10, 21, 2, 10, 0)
        .unwrap()
        .timestamp(); // 09:10 VN
    let exp = json!({"symbol": "FPT", "boardId": "G1", "expectedTradePrice": 124.5, "expectedTradeQuantity": 20000, "time": {"Seconds": ato, "Nanos": 0}});
    let evs = DnseNormalizer::new(DnseConfig::default(), cal()).events(&exp, now());
    let MarketEvent::ExpectedPrice(e) = &evs[0] else {
        panic!()
    };
    assert_eq!((e.session, e.price), (Session::Ato, 124_500.0));
}

#[test]
fn dnse_auth_signature_matches_docs() {
    let m = build_auth_message("key1", "secret1", 1_700_000_000.123456);
    assert_eq!(m["timestamp"], 1_700_000_000i64);
    assert_eq!(m["nonce"], "1700000000123456");
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret1").unwrap();
    mac.update(b"key1:1700000000:1700000000123456");
    assert_eq!(m["signature"], hex::encode(mac.finalize().into_bytes()));
    assert!(!m.to_string().contains("secret1"));
}

#[tokio::test]
async fn dnse_end_to_end_against_fake_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(json!({})));
    let seen2 = seen.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let path = Arc::new(Mutex::new(String::new()));
        let p2 = path.clone();
        let mut ws =
            tokio_tungstenite::accept_hdr_async(stream, move |req: &Request, resp: Response| {
                *p2.lock() = req.uri().to_string();
                Ok(resp)
            })
            .await
            .unwrap();
        let recv = |m: Option<Result<Message, _>>| -> Value {
            serde_json::from_str(m.unwrap().unwrap().to_text().unwrap()).unwrap()
        };
        ws.send(Message::Text(
            json!({"action": "welcome"}).to_string().into(),
        ))
        .await
        .unwrap();
        let auth = recv(ws.next().await);
        let mut mac = Hmac::<Sha256>::new_from_slice(b"sek").unwrap();
        mac.update(
            format!(
                "{}:{}:{}",
                auth["api_key"].as_str().unwrap(),
                auth["timestamp"],
                auth["nonce"].as_str().unwrap()
            )
            .as_bytes(),
        );
        let sig_ok = auth["signature"] == hex::encode(mac.finalize().into_bytes());
        ws.send(Message::Text(
            json!({"action": "auth_success", "rate_limit": {"subscriptions_max": 3}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let sub1 = recv(ws.next().await);
        let sub2 = recv(ws.next().await);
        ws.send(Message::Text(
            json!({"action": "subscribed"}).to_string().into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({"action": "ping", "timestamp": 123})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let pong = recv(ws.next().await);
        for t in [
            doc_tick(json!({})),
            doc_tick(json!({})),
            doc_tick(json!({"side": "SELL", "totalVolumeTraded": 806000, "matchQtty": 6000.0})),
        ] {
            ws.send(Message::Text(t.to_string().into())).await.unwrap();
        }
        ws.send(Message::Text(
            json!({"action": "connection_expired", "code": "MAX_DURATION_EXCEEDED"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        *seen2.lock() =
            json!({"path": *path.lock(), "sig_ok": sig_ok, "sub": [sub1, sub2], "pong": pong});
        let _ = ws.next().await;
    });
    let cfg = DnseConfig {
        api_key: Some("k".into()),
        api_secret: Some(Secret::new("sek")),
        ws_url: format!("ws://127.0.0.1:{port}/v1/stream"),
        ..DnseConfig::default()
    };
    let mut p = DnseProvider::new(cfg, cal(), None);
    p.connect().await.unwrap();
    p.subscribe(&["fpt".into(), "VCB".into()]).await.unwrap();
    let mut got = vec![];
    let err = loop {
        match p.next_event().await {
            Ok(Some(ev)) => got.push(ev),
            Ok(None) => panic!("unexpected end"),
            Err(e) => break e.to_string(),
        }
    };
    p.close().await;
    server.abort();
    assert!(err.contains("expired"), "{err}");
    let s = seen.lock().clone();
    assert_eq!(s["path"], "/v1/stream?encoding=json");
    assert_eq!(s["sig_ok"], true);
    // server budget 3 streams (channel x symbol): tick_extra first, expected_price gets the rest
    assert_eq!(
        s["sub"],
        json!([
            {"action": "subscribe", "channels": [{"name": "tick_extra.G1.json", "symbols": ["FPT", "VCB"]}]},
            {"action": "subscribe", "channels": [{"name": "expected_price.G1.json", "symbols": ["FPT"]}]},
        ])
    );
    assert_eq!(s["pong"], json!({"action": "pong", "timestamp": 123}));
    let sides: Vec<(Side, i64)> = got
        .iter()
        .filter_map(|e| e.as_tick())
        .map(|t| (t.side, t.volume))
        .collect();
    assert_eq!(sides, vec![(Side::Buy, 5000), (Side::Sell, 6000)]);
}

#[tokio::test]
async fn dnse_auth_failed_is_reported_without_secret() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.send(Message::Text(
            json!({"action": "welcome"}).to_string().into(),
        ))
        .await
        .unwrap();
        let _ = ws.next().await;
        ws.send(Message::Text(
            json!({"action": "error", "code": "AUTH_FAILED", "message": "bad sig"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let _ = ws.close(None).await;
    });
    let cfg = DnseConfig {
        api_key: Some("k".into()),
        api_secret: Some(Secret::new("topsecret")),
        ws_url: format!("ws://127.0.0.1:{port}"),
        ..DnseConfig::default()
    };
    let err = DnseProvider::new(cfg, cal(), None)
        .connect()
        .await
        .unwrap_err()
        .to_string();
    server.abort();
    assert!(
        err.contains("bad sig") && !err.contains("topsecret"),
        "{err}"
    );
}

// ------------------------------------------------------------------ SSI

fn x_trade(over: Value) -> Value {
    let mut c = json!({
        "RType": "X-TRADE", "TradingDate": "21/10/2026", "Time": "10:15:30", "Symbol": "FPT",
        "LastPrice": 124000.0, "LastVol": 5000.0, "TotalVol": 800000.0, "EstMatchedPrice": 0.0,
        "TradingSession": "LO", "Side": "BU",
    });
    for (k, v) in over.as_object().unwrap() {
        c[k] = v.clone();
    }
    // documented shape: Content is a JSON *string*; doc samples even carry trailing commas
    let content = c.to_string();
    let content = format!("{},}}", &content[..content.len() - 1]);
    Value::String(json!({"DataType": "X-TRADE", "Content": content}).to_string())
}

#[test]
fn ssi_x_trade_side_repeats_units() {
    let mut n = SsiNormalizer::new(SsiConfig::default(), cal());
    let (dt, c) = decode_broadcast(&x_trade(json!({}))).unwrap();
    let MarketEvent::Tick(t) = &n.events(&dt, &c, now())[0] else {
        panic!()
    };
    assert_eq!((t.price, t.volume, t.side), (124_000.0, 5000, Side::Buy));
    assert_eq!(
        t.ts_exchange,
        Utc.with_ymd_and_hms(2026, 10, 21, 3, 15, 30).unwrap()
    ); // VN -> UTC
    assert_eq!(t.raw_ref.as_deref(), Some("tv:800000"));
    let (dt, c) = decode_broadcast(&x_trade(json!({}))).unwrap();
    assert!(n.events(&dt, &c, now()).is_empty()); // snapshot repeat
    let (dt, c) = decode_broadcast(&x_trade(json!({"TotalVol": 815000.0, "Side": "SD"}))).unwrap();
    let MarketEvent::Tick(t) = &n.events(&dt, &c, now())[0] else {
        panic!()
    };
    assert_eq!(t.side, Side::Sell);
    assert_eq!(n.stats.get("missed_volume"), Some(&10000));
}

fn jwt(exp: i64) -> String {
    let b = |v: Value| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v.to_string());
    format!(
        "{}.{}.sig",
        b(json!({"alg": "none"})),
        b(json!({"exp": exp}))
    )
}

#[tokio::test]
async fn ssi_end_to_end_against_fake_signalr_hub() {
    use axum::extract::ws::{Message as AxMsg, WebSocketUpgrade};
    use axum::extract::RawQuery;
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Router;

    let token = jwt(Utc::now().timestamp() + 8 * 3600);
    let seen = Arc::new(Mutex::new(json!({})));
    let tok = token.clone();
    let seen_ws = seen.clone();
    let app = Router::new()
        .route(
            "/api/v2/Market/AccessToken",
            post(move |axum::Json(body): axum::Json<Value>| {
                let t = tok.clone();
                async move {
                    assert_eq!(body.as_object().unwrap().keys().cloned().collect::<Vec<_>>(), vec!["consumerID", "consumerSecret"]);
                    axum::Json(json!({"status": 200, "message": "Success", "data": {"accessToken": t}}))
                }
            }),
        )
        .route("/v2.0/signalr/negotiate", post(|| async { axum::Json(json!({"ConnectionToken": "ct-1", "ProtocolVersion": "1.5"})) }))
        .route(
            "/v2.0/signalr/connect",
            get(move |ws: WebSocketUpgrade, RawQuery(q): RawQuery, headers: HeaderMap| {
                let seen = seen_ws.clone();
                async move {
                    let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                    ws.on_upgrade(move |mut sock| async move {
                        let send = |v: Value| AxMsg::Text(v.to_string().into());
                        sock.send(send(json!({"C": "c1", "S": 1, "M": []}))).await.unwrap();
                        let invoke: Value = match sock.recv().await {
                            Some(Ok(AxMsg::Text(t))) => serde_json::from_str(&t).unwrap(),
                            other => panic!("{other:?}"),
                        };
                        sock.send(AxMsg::Text("{}".into())).await.unwrap(); // keep-alive
                        let frames = [x_trade(json!({})), x_trade(json!({})), x_trade(json!({"TotalVol": 806000.0, "Side": "SD", "LastVol": 6000.0}))];
                        let calls: Vec<Value> = frames.iter().map(|f| json!({"H": "FcMarketDataV2Hub", "M": "Broadcast", "A": [f]})).collect();
                        sock.send(send(json!({"C": "c2", "M": calls}))).await.unwrap();
                        sock.send(send(json!({"M": [{"H": "OtherHub", "M": "Broadcast", "A": [x_trade(json!({"TotalVol": 9e6}))]}]}))).await.unwrap();
                        *seen.lock() = json!({"query": q, "auth": auth, "invoke": invoke});
                        let _ = sock.send(AxMsg::Close(None)).await;
                    })
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let raw: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));
    let raw2 = raw.clone();
    let cfg = SsiConfig {
        consumer_id: Some("id".into()),
        consumer_secret: Some(Secret::new("s3cret")),
        api_url: base.clone(),
        stream_url: base,
        ..SsiConfig::default()
    };
    let sink: astrq::providers::RawSink =
        Arc::new(move |line: &str, _ts| raw2.lock().push(line.to_string()));
    let mut p = SsiProvider::new(cfg, cal(), Some(sink), None);
    p.connect().await.unwrap();
    p.subscribe(&["VCB".into(), "FPT".into()]).await.unwrap();
    let mut got = vec![];
    let err = loop {
        match p.next_event().await {
            Ok(Some(ev)) => got.push(ev),
            Ok(None) => panic!("unexpected end"),
            Err(e) => break e.to_string(),
        }
    };
    p.close().await;
    server.abort();
    assert!(err.contains("closed"), "{err}");
    let s = seen.lock().clone();
    assert!(
        s["query"]
            .as_str()
            .unwrap()
            .starts_with("transport=webSockets&connectionToken=ct-1"),
        "{s}"
    );
    assert_eq!(s["auth"], format!("Bearer {token}"));
    assert_eq!(
        s["invoke"],
        json!({"H": "FcMarketDataV2Hub", "M": "SwitchChannels", "A": ["X-TRADE:FPT-VCB"], "I": 0})
    );
    let sides: Vec<(Side, i64)> = got
        .iter()
        .filter_map(|e| e.as_tick())
        .map(|t| (t.side, t.volume))
        .collect();
    assert_eq!(sides, vec![(Side::Buy, 5000), (Side::Sell, 6000)]);
    assert_eq!(raw.lock().len(), 3, "raw messages of our hub are recorded");
}
