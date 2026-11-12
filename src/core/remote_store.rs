//! Remote persistence on S3-compatible storage (Cloudflare R2) for disk-less hosts.
//!
//! What is kept (all small, same object layout as the Python service):
//!   * `{prefix}baselines/summaries.json.gz` — per-day, per-symbol trade-size summaries;
//!   * `{prefix}signals/YYYY-MM-DD.jsonl.gz` — today's signals, re-uploaded periodically.
//!
//! Raw ticks are NOT uploaded (too large for free egress).
//! Requests are signed with AWS Signature V4 (R2: region "auto", path-style URLs).

use anyhow::{bail, Result};
use chrono::{DateTime, NaiveDate, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::core::config::RemoteStorageConfig;
use crate::providers::recorded_baseline::summarize_day;

pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn hmac(key: &[u8], msg: &str) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    m.update(msg.as_bytes());
    m.finalize().into_bytes().to_vec()
}

fn uri_encode(s: &str, keep_slash: bool) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            b'/' if keep_slash => "/".into(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Headers (including Authorization) for an AWS SigV4 request.
#[allow(clippy::too_many_arguments)]
pub fn sign_v4(
    method: &str,
    url: &reqwest::Url,
    headers: &BTreeMap<String, String>,
    payload_hash: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    now: DateTime<Utc>,
) -> BTreeMap<String, String> {
    let service = "s3";
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let day = now.format("%Y%m%d").to_string();
    let mut host = url.host_str().unwrap_or("").to_string();
    if let Some(port) = url.port() {
        host = format!("{host}:{port}");
    }
    let mut canon: BTreeMap<String, String> = headers
        .iter()
        .map(|(k, v)| {
            (
                k.to_ascii_lowercase().trim().to_string(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    canon.insert("host".into(), host);
    canon.insert("x-amz-date".into(), amz_date.clone());
    canon.insert("x-amz-content-sha256".into(), payload_hash.into());
    let signed: Vec<&str> = canon.keys().map(String::as_str).collect();
    let signed = signed.join(";");
    let mut q: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    q.sort();
    let query = q
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, false), uri_encode(v, false)))
        .collect::<Vec<_>>()
        .join("&");
    let path = percent_decode(url.path());
    let canonical_request = [
        method.to_string(),
        uri_encode(&path, true),
        query,
        canon
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>(),
        signed.clone(),
        payload_hash.to_string(),
    ]
    .join("\n");
    let scope = format!("{day}/{region}/{service}/aws4_request");
    let to_sign = [
        "AWS4-HMAC-SHA256".to_string(),
        amz_date.clone(),
        scope.clone(),
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    ]
    .join("\n");
    let k = hmac(
        &hmac(
            &hmac(&hmac(format!("AWS4{secret_key}").as_bytes(), &day), region),
            service,
        ),
        "aws4_request",
    );
    let signature = hex::encode(hmac(&k, &to_sign));
    let mut out = headers.clone();
    out.insert("x-amz-date".into(), amz_date);
    out.insert("x-amz-content-sha256".into(), payload_hash.into());
    out.insert(
        "Authorization".into(),
        format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed}, Signature={signature}"),
    );
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    let _ = e.write_all(data);
    e.finish().unwrap_or_default()
}

pub fn gunzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    GzDecoder::new(data).read_to_end(&mut out)?;
    Ok(out)
}

pub struct R2Client {
    cfg: RemoteStorageConfig,
    http: reqwest::Client,
}

impl R2Client {
    pub fn new(cfg: RemoteStorageConfig, http: Option<reqwest::Client>) -> Self {
        Self {
            cfg,
            http: http.unwrap_or_default(),
        }
    }

    fn url(&self, key: &str) -> Result<reqwest::Url> {
        let endpoint = self
            .cfg
            .endpoint
            .as_deref()
            .unwrap_or("")
            .trim_end_matches('/');
        let bucket = self.cfg.bucket.as_deref().unwrap_or("");
        Ok(reqwest::Url::parse(&format!(
            "{endpoint}/{bucket}/{}{key}",
            self.cfg.prefix
        ))?)
    }

    fn sign(
        &self,
        method: &str,
        url: &reqwest::Url,
        body: &[u8],
        headers: BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        let hash = if body.is_empty() {
            EMPTY_SHA256.to_string()
        } else {
            hex::encode(Sha256::digest(body))
        };
        sign_v4(
            method,
            url,
            &headers,
            &hash,
            self.cfg.access_key_id.as_deref().unwrap_or(""),
            self.cfg
                .secret_access_key
                .as_ref()
                .map(|s| s.expose())
                .unwrap_or(""),
            &self.cfg.region,
            Utc::now(),
        )
    }

    pub async fn put(&self, key: &str, body: Vec<u8>, content_type: &str) -> Result<()> {
        let url = self.url(key)?;
        let headers = self.sign(
            "PUT",
            &url,
            &body,
            BTreeMap::from([("content-type".to_string(), content_type.to_string())]),
        );
        let mut req = self.http.put(url).body(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let r = req.send().await?;
        if r.status().as_u16() >= 300 {
            bail!("R2 PUT {key}: HTTP {}", r.status().as_u16());
        }
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let url = self.url(key)?;
        let headers = self.sign("GET", &url, b"", BTreeMap::new());
        let mut req = self.http.get(url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        let r = req.send().await?;
        match r.status().as_u16() {
            404 => Ok(None),
            s if s >= 300 => bail!("R2 GET {key}: HTTP {s}"),
            _ => Ok(Some(r.bytes().await?.to_vec())),
        }
    }
}

/// Restore/persist the small state a disk-less host needs between restarts.
pub struct RemoteSync {
    pub client: R2Client,
    pub data_dir: PathBuf,
    pub keep_days: usize,
    pub last_error: Mutex<Option<String>>,
    pub last_upload: Mutex<Option<DateTime<Utc>>>,
    index: tokio::sync::Mutex<Option<BTreeSet<NaiveDate>>>,
}

impl RemoteSync {
    pub fn new(client: R2Client, data_dir: PathBuf, keep_days: usize) -> Self {
        Self {
            client,
            data_dir,
            keep_days,
            last_error: Mutex::new(None),
            last_upload: Mutex::new(None),
            index: tokio::sync::Mutex::new(None),
        }
    }

    fn baselines_dir(&self) -> PathBuf {
        self.data_dir.join("baselines")
    }
    fn signals_file(&self, d: NaiveDate) -> PathBuf {
        self.data_dir.join("signals").join(format!("{d}.jsonl"))
    }
    fn err(&self, what: &str, e: anyhow::Error) {
        tracing::warn!("R2 {what} failed: {e:#}");
        *self.last_error.lock() = Some(format!("{what}: {e:#}"));
    }

    pub async fn restore(&self, today: NaiveDate) {
        let res: Result<()> = async {
            if let Some(blob) = self.client.get("baselines/summaries.json.gz").await? {
                let summaries: Map<String, Value> = serde_json::from_slice(&gunzip(&blob)?)?;
                std::fs::create_dir_all(self.baselines_dir())?;
                for (day, s) in &summaries {
                    let f = self.baselines_dir().join(format!("{day}.json"));
                    if !f.exists() {
                        std::fs::write(f, s.to_string())?;
                    }
                }
                tracing::info!("restored {} baseline summaries from R2", summaries.len());
            }
            let local = self.signals_file(today);
            if let Some(sig) = self
                .client
                .get(&format!("signals/{today}.jsonl.gz"))
                .await?
            {
                if !local.exists() {
                    std::fs::create_dir_all(local.parent().expect("dir"))?;
                    std::fs::write(&local, gunzip(&sig)?)?;
                    tracing::info!("restored today's signals from R2");
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = res {
            self.err("restore", e); // storage is best-effort; never block startup
        }
    }

    pub async fn upload_signals(&self, today: NaiveDate) {
        let f = self.signals_file(today);
        let Ok(data) = std::fs::read(&f) else { return };
        match self
            .client
            .put(
                &format!("signals/{today}.jsonl.gz"),
                gzip(&data),
                "application/gzip",
            )
            .await
        {
            Ok(()) => {
                *self.last_upload.lock() = Some(Utc::now());
                self.add_to_index(today).await;
            }
            Err(e) => self.err("signals", e),
        }
    }

    // ---------------------------------------------------------------- past sessions

    /// Trading dates with signals in R2 (`signals/index.json`), loaded once per process
    /// so a disk-less host can list past sessions after a restart.
    pub async fn dates(&self) -> BTreeSet<NaiveDate> {
        let mut idx = self.index.lock().await;
        if idx.is_none() {
            let loaded = match self.client.get("signals/index.json").await {
                Ok(Some(b)) => serde_json::from_slice(&b).unwrap_or_default(),
                Ok(None) => BTreeSet::new(),
                Err(e) => {
                    self.err("index", e);
                    return BTreeSet::new(); // retry on the next call
                }
            };
            *idx = Some(loaded);
        }
        idx.clone().unwrap_or_default()
    }

    async fn add_to_index(&self, d: NaiveDate) {
        let mut dates = self.dates().await;
        if !dates.insert(d) {
            return;
        }
        let skip = dates.len().saturating_sub(self.keep_days.max(1));
        let dates: BTreeSet<NaiveDate> = dates.into_iter().skip(skip).collect();
        let body = serde_json::to_vec(&dates).unwrap_or_default();
        match self
            .client
            .put("signals/index.json", body, "application/json")
            .await
        {
            Ok(()) => *self.index.lock().await = Some(dates),
            Err(e) => self.err("index", e),
        }
    }

    /// `archive/{tool}/{d}.json.gz` (already gzip-compressed on disk).
    pub async fn upload_archive(&self, tool: &str, d: NaiveDate, path: &Path) {
        let Ok(data) = std::fs::read(path) else {
            return;
        };
        if let Err(e) = self
            .client
            .put(
                &format!("archive/{tool}/{d}.json.gz"),
                data,
                "application/gzip",
            )
            .await
        {
            self.err("archive", e);
        }
    }

    /// Bring a past session's signals and tool archives back to local disk (disk-less
    /// hosts). Returns whether the signals file now exists locally.
    pub async fn fetch_day(&self, d: NaiveDate, archives: &[(String, PathBuf)]) -> bool {
        let local = self.signals_file(d);
        let res: Result<()> = async {
            if !local.exists() {
                if let Some(sig) = self.client.get(&format!("signals/{d}.jsonl.gz")).await? {
                    std::fs::create_dir_all(local.parent().expect("dir"))?;
                    std::fs::write(&local, gunzip(&sig)?)?;
                }
            }
            for (tool, path) in archives {
                if path.exists() {
                    continue;
                }
                if let Some(a) = self
                    .client
                    .get(&format!("archive/{tool}/{d}.json.gz"))
                    .await?
                {
                    std::fs::create_dir_all(path.parent().expect("dir"))?;
                    std::fs::write(path, a)?;
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = res {
            self.err("fetch_day", e);
        }
        local.exists()
    }

    /// Summarize today's ticks and merge into the remote summaries object.
    pub async fn upload_day_summary(&self, today: NaiveDate) {
        let ticks = self.data_dir.join("ticks").join(format!("{today}.jsonl"));
        let res: Result<usize> = async {
            let mut summaries: BTreeMap<String, Value> =
                match self.client.get("baselines/summaries.json.gz").await? {
                    Some(blob) => serde_json::from_slice(&gunzip(&blob)?)?,
                    None => BTreeMap::new(),
                };
            if ticks.exists() {
                let summary = summarize_day(&ticks)?;
                if !summary.is_empty() {
                    let v = serde_json::to_value(&summary)?;
                    std::fs::create_dir_all(self.baselines_dir())?;
                    std::fs::write(
                        self.baselines_dir().join(format!("{today}.json")),
                        v.to_string(),
                    )?;
                    summaries.insert(today.to_string(), v);
                }
            }
            let skip = summaries.len().saturating_sub(self.keep_days);
            let keep: BTreeMap<String, Value> = summaries.into_iter().skip(skip).collect();
            self.client
                .put(
                    "baselines/summaries.json.gz",
                    gzip(serde_json::to_string(&keep)?.as_bytes()),
                    "application/gzip",
                )
                .await?;
            Ok(keep.len())
        }
        .await;
        match res {
            Ok(n) => tracing::info!("uploaded baseline summaries ({n} days) to R2"),
            Err(e) => self.err("summary", e),
        }
    }

    pub fn exists(path: &Path) -> bool {
        path.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    const AK: &str = "AKIAIOSFODNN7EXAMPLE";
    const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn sig(h: &BTreeMap<String, String>) -> String {
        h["Authorization"]
            .rsplit("Signature=")
            .next()
            .unwrap()
            .to_string()
    }

    #[test]
    fn sigv4_matches_aws_published_examples() {
        let t = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();
        // AWS S3 docs, "Example: GET Object"
        let url = reqwest::Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let h = sign_v4(
            "GET",
            &url,
            &BTreeMap::from([("range".into(), "bytes=0-9".into())]),
            EMPTY_SHA256,
            AK,
            SK,
            "us-east-1",
            t,
        );
        assert!(
            h["Authorization"].contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date")
        );
        assert_eq!(
            sig(&h),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        // AWS S3 docs, "Example: GET Bucket (List Objects)"
        let url =
            reqwest::Url::parse("https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J")
                .unwrap();
        let h = sign_v4(
            "GET",
            &url,
            &BTreeMap::new(),
            EMPTY_SHA256,
            AK,
            SK,
            "us-east-1",
            t,
        );
        assert_eq!(
            sig(&h),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }
}
