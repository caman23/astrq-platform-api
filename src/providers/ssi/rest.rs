//! SSI FastConnect Data REST: access token + index components.
//!
//! Verified against the official guide (api-specs) and the official `ssi-fc-data` 2.2.2
//! client: POST api/v2/Market/AccessToken with {"consumerID","consumerSecret"}; response
//! {"status": 200, "message": ..., "data": {"accessToken": <JWT>}}. Never logged.

use anyhow::{bail, Result};
use base64::Engine;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::config::SsiConfig;

pub const TOKEN_PATH: &str = "api/v2/Market/AccessToken";
pub const INDEX_COMPONENTS_PATH: &str = "api/v2/Market/IndexComponents";
const REFRESH_MARGIN_SEC: f64 = 3600.0; // same margin as the official client

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn jwt_exp(token: &str) -> Option<f64> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    serde_json::from_slice::<Value>(&bytes).ok()?["exp"].as_f64()
}

pub struct SsiRestClient {
    pub cfg: SsiConfig,
    http: reqwest::Client,
    token: Option<String>,
    expires_at: f64,
}

impl SsiRestClient {
    pub fn new(cfg: SsiConfig, http: reqwest::Client) -> Self {
        Self {
            cfg,
            http,
            token: None,
            expires_at: 0.0,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{path}", self.cfg.api_url.trim_end_matches('/'))
    }

    pub async fn token(&mut self) -> Result<String> {
        if let Some(t) = &self.token {
            if now() < self.expires_at - REFRESH_MARGIN_SEC {
                return Ok(t.clone());
            }
        }
        let (Some(id), Some(secret)) = (&self.cfg.consumer_id, &self.cfg.consumer_secret) else {
            bail!("missing SSI ConsumerID/ConsumerSecret");
        };
        let r = self
            .http
            .post(self.url(TOKEN_PATH))
            .json(&json!({"consumerID": id, "consumerSecret": secret.expose()}))
            .send()
            .await?;
        let status = r.status().as_u16();
        let body: Value = r
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("token endpoint HTTP {status}: non-JSON body"))?;
        let token = body["data"]["accessToken"].as_str().map(String::from);
        let ok_status =
            body["status"].as_i64() == Some(200) || body["status"].as_str() == Some("200");
        match token {
            Some(t) if status == 200 && ok_status => {
                // unknown expiry -> assume 8h so we refresh periodically anyway
                self.expires_at = jwt_exp(&t).unwrap_or_else(|| now() + 8.0 * 3600.0);
                self.token = Some(t.clone());
                Ok(t)
            }
            _ => bail!(
                "token request failed (HTTP {status}, status={}): {}",
                body["status"],
                body["message"]
            ),
        }
    }

    pub fn invalidate(&mut self) {
        self.token = None;
    }

    /// (exchange, symbols) of an index, e.g. VN100.
    pub async fn index_components(
        &mut self,
        index_code: &str,
    ) -> Result<(Option<String>, Vec<String>)> {
        let token = self.token().await?;
        let r = self
            .http
            .get(self.url(INDEX_COMPONENTS_PATH))
            .query(&[
                ("indexCode", index_code),
                ("pageIndex", "1"),
                ("pageSize", "1000"),
            ])
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?;
        let body: Value = r.json().await?;
        let mut exchange = None;
        let mut symbols = Vec::new();
        for item in body["data"].as_array().cloned().unwrap_or_default() {
            if let Some(e) = item["Exchange"].as_str() {
                exchange = Some(e.to_string());
            }
            for comp in item["IndexComponent"]
                .as_array()
                .cloned()
                .unwrap_or_default()
            {
                if let Some(s) = comp["StockSymbol"].as_str() {
                    symbols.push(s.to_ascii_uppercase());
                }
            }
        }
        Ok((exchange, symbols))
    }
}
