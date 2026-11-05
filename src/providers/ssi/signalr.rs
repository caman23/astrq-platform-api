//! Minimal asyncio client for classic ASP.NET SignalR (protocol 1.5), as used by SSI
//! FastConnect Data streaming. Steps mirror the official `ssi-fc-data` 2.2.2 client:
//!   1. POST {base}/v2.0/signalr/negotiate?connectionData=[{"name": HUB}]&clientProtocol=1.5
//!   2. WebSocket {wss base}/v2.0/signalr/connect?transport=webSockets&connectionToken=...
//!   3. Invoke: {"H": HUB, "M": "SwitchChannels", "A": ["X-TRADE:FPT-VCB"], "I": n}
//!   4. Push: {"M": [{"H": HUB, "M": "Broadcast", "A": [<json string>]}]}; "{}" = keep-alive.
//!
//! Unlike the official client, TLS certificates ARE verified and headers are never printed.

use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub const HUB: &str = "FcMarketDataV2Hub";
pub const SIGNALR_PATH: &str = "v2.0/signalr";
pub const PROTOCOL: &str = "1.5";

#[derive(Debug)]
pub struct AuthError;

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("negotiate unauthorized (token rejected or expired)")
    }
}
impl std::error::Error for AuthError {}

pub struct SignalRClient {
    url: String,
    auth: String,
    conn_data: String,
    http: reqwest::Client,
    ws: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    counter: i64,
}

impl SignalRClient {
    pub fn new(base_url: &str, token: &str, http: reqwest::Client) -> Self {
        Self {
            url: format!("{}/{SIGNALR_PATH}", base_url.trim_end_matches('/')),
            auth: format!("Bearer {token}"),
            conn_data: json!([{"name": HUB}]).to_string(),
            http,
            ws: None,
            counter: -1,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        let r = self
            .http
            .post(format!("{}/negotiate", self.url))
            .query(&[
                ("connectionData", self.conn_data.as_str()),
                ("clientProtocol", PROTOCOL),
            ])
            .header("Authorization", &self.auth)
            .send()
            .await?;
        match r.status().as_u16() {
            401 => return Err(AuthError.into()),
            200 => {}
            s => bail!("negotiate HTTP {s}"),
        }
        let data: Value = r.json().await?;
        let token = data["ConnectionToken"]
            .as_str()
            .ok_or_else(|| anyhow!("negotiate: no ConnectionToken"))?;
        let proto = data["ProtocolVersion"].as_str().unwrap_or(PROTOCOL);
        let ws_base = self
            .url
            .replacen("https://", "wss://", 1)
            .replacen("http://", "ws://", 1);
        let query = serde_urlencoded_pairs(&[
            ("transport", "webSockets"),
            ("connectionToken", token),
            ("connectionData", &self.conn_data),
            ("clientProtocol", proto),
        ]);
        let mut req = format!("{ws_base}/connect?{query}").into_client_request()?;
        req.headers_mut()
            .insert("Authorization", HeaderValue::from_str(&self.auth)?);
        let (ws, _) = connect_async(req).await?;
        self.ws = Some(ws);
        Ok(())
    }

    pub async fn invoke(&mut self, method: &str, args: Vec<Value>) -> Result<()> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow!("not connected"))?;
        self.counter += 1;
        ws.send(Message::Text(
            json!({"H": HUB, "M": method, "A": args, "I": self.counter})
                .to_string()
                .into(),
        ))
        .await?;
        Ok(())
    }

    /// Next (method, args) addressed to our hub; `None` when the socket closes.
    pub async fn next_call(
        &mut self,
        pending: &mut std::collections::VecDeque<(String, Vec<Value>)>,
    ) -> Result<Option<(String, Vec<Value>)>> {
        loop {
            if let Some(c) = pending.pop_front() {
                return Ok(Some(c));
            }
            let ws = self.ws.as_mut().ok_or_else(|| anyhow!("not connected"))?;
            let raw = match ws.next().await {
                Some(Ok(Message::Text(t))) => t.to_string(),
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e.into()),
            };
            if raw.is_empty() {
                continue;
            }
            let Ok(msg) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            if let Some(e) = msg.get("E").filter(|e| !e.is_null()) {
                bail!("server error: {e}");
            }
            for call in msg["M"].as_array().cloned().unwrap_or_default() {
                if call["H"].as_str().unwrap_or("").eq_ignore_ascii_case(HUB) {
                    pending.push_back((
                        call["M"].as_str().unwrap_or("").to_string(),
                        call["A"].as_array().cloned().unwrap_or_default(),
                    ));
                }
            }
        }
    }

    pub async fn close(&mut self) {
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.close(None).await;
        }
    }
}

fn serde_urlencoded_pairs(pairs: &[(&str, &str)]) -> String {
    fn enc(s: &str) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                b' ' => "+".into(),
                _ => format!("%{b:02X}"),
            })
            .collect()
    }
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}
