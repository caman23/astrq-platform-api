//! Baselines computed from our own recorded tick files (ADR-0004).
//!
//! For each finished trading day `data/ticks/YYYY-MM-DD.jsonl`, a per-symbol summary
//! (median / p95 trade value, matched value) of continuous-session, non-negotiated trades
//! is computed once and cached in `data/baselines/YYYY-MM-DD.json` (same format as the
//! Python service). The baseline for day D aggregates the last `lookback_days` summaries
//! before D; with fewer than `min_days` recorded sessions a symbol stays `baseline_cold`.

use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

use crate::core::context::{Baseline, ContextProvider};

/// Liquidity tiers by average daily matched value (VND). Starting points; tune on data.
pub const TIER_HIGH: f64 = 200e9;
pub const TIER_MID: f64 = 50e9;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DaySummary {
    pub n: f64,
    pub median: f64,
    pub p95: f64,
    pub total: f64,
}

pub type Summaries = BTreeMap<String, DaySummary>;

fn quantile(sorted: &[f64], q: f64) -> f64 {
    sorted[((q * sorted.len() as f64) as usize).min(sorted.len() - 1)]
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

#[derive(Deserialize)]
struct TickLine {
    event_type: String,
    symbol: String,
    #[serde(default)]
    value: f64,
    #[serde(default)]
    session: String,
    #[serde(default)]
    is_negotiated: bool,
}

pub fn summarize_day(tick_file: &Path) -> Result<Summaries> {
    let f = std::fs::File::open(tick_file)?;
    let mut values: HashMap<String, Vec<f64>> = HashMap::new();
    for line in std::io::BufReader::new(f).lines() {
        let line = line?;
        if !line.contains("\"tick\"") {
            continue;
        }
        let Ok(ev) = serde_json::from_str::<TickLine>(&line) else {
            continue;
        };
        if ev.event_type != "tick"
            || ev.is_negotiated
            || !matches!(ev.session.as_str(), "continuous_am" | "continuous_pm")
        {
            continue;
        }
        values.entry(ev.symbol).or_default().push(ev.value);
    }
    Ok(values
        .into_iter()
        .map(|(sym, mut v)| {
            v.sort_by(f64::total_cmp);
            let s = DaySummary {
                n: v.len() as f64,
                median: quantile(&v, 0.5),
                p95: quantile(&v, 0.95),
                total: v.iter().sum(),
            };
            (sym, s)
        })
        .collect())
}

pub struct RecordedBaselineProvider {
    pub ticks_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub lookback_days: usize,
    pub min_days: usize,
    by_date: Mutex<HashMap<NaiveDate, HashMap<String, Baseline>>>,
}

fn dated_files(dir: &Path, ext: &str) -> Vec<NaiveDate> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return vec![];
    };
    rd.flatten()
        .filter_map(|e| {
            let p = e.path();
            (p.extension()? == ext).then_some(())?;
            NaiveDate::parse_from_str(p.file_stem()?.to_str()?, "%Y-%m-%d").ok()
        })
        .collect()
}

impl RecordedBaselineProvider {
    pub fn new(
        ticks_dir: PathBuf,
        cache_dir: PathBuf,
        lookback_days: usize,
        min_days: usize,
    ) -> Self {
        Self {
            ticks_dir,
            cache_dir,
            lookback_days,
            min_days,
            by_date: Mutex::new(HashMap::new()),
        }
    }

    pub fn day_summary(&self, d: NaiveDate) -> Option<Summaries> {
        let cached = self.cache_dir.join(format!("{d}.json"));
        if let Ok(text) = std::fs::read_to_string(&cached) {
            if let Ok(s) = serde_json::from_str(&text) {
                return Some(s);
            }
        }
        let src = self.ticks_dir.join(format!("{d}.jsonl"));
        if !src.exists() {
            return None;
        }
        let summary = summarize_day(&src).ok()?;
        let _ = std::fs::create_dir_all(&self.cache_dir);
        let _ = std::fs::write(&cached, serde_json::to_string(&summary).unwrap_or_default());
        Some(summary)
    }

    fn compute(&self, d: NaiveDate) -> HashMap<String, Baseline> {
        // A day is available if we have its raw ticks or an already computed summary
        // (summaries may be restored from remote storage on disk-less hosts).
        let known: BTreeSet<NaiveDate> = dated_files(&self.ticks_dir, "jsonl")
            .into_iter()
            .chain(dated_files(&self.cache_dir, "json"))
            .filter(|x| *x < d)
            .collect();
        let days: Vec<NaiveDate> = known.into_iter().rev().take(self.lookback_days).collect();
        let mut per_symbol: HashMap<String, Vec<DaySummary>> = HashMap::new();
        for day in &days {
            for (sym, s) in self.day_summary(*day).unwrap_or_default() {
                per_symbol.entry(sym).or_default().push(s);
            }
        }
        let mut out = HashMap::new();
        for (sym, rows) in per_symbol {
            if rows.len() < self.min_days {
                continue;
            }
            let avg = rows.iter().map(|r| r.total).sum::<f64>() / rows.len() as f64;
            out.insert(
                sym.clone(),
                Baseline {
                    symbol: sym,
                    trading_date: d,
                    median_trade_value: median(
                        &mut rows.iter().map(|r| r.median).collect::<Vec<_>>(),
                    ),
                    p95_trade_value: median(&mut rows.iter().map(|r| r.p95).collect::<Vec<_>>()),
                    avg_daily_value: avg,
                    liquidity_tier: if avg >= TIER_HIGH {
                        "high"
                    } else if avg >= TIER_MID {
                        "mid"
                    } else {
                        "low"
                    }
                    .into(),
                    source: format!("recorded:{}d", rows.len()),
                },
            );
        }
        tracing::info!(
            "recorded baselines for {d}: {} symbols from {} days",
            out.len(),
            days.len()
        );
        out
    }
}

#[async_trait]
impl ContextProvider for RecordedBaselineProvider {
    fn name(&self) -> &str {
        "recorded"
    }
    async fn baseline(&self, symbol: &str, d: NaiveDate) -> Result<Option<Baseline>> {
        let mut by = self.by_date.lock().await;
        by.entry(d).or_insert_with(|| {
            // warm-up path only (never per tick): a short synchronous scan is fine

            self.compute(d)
        });
        Ok(by[&d].get(symbol).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_day(dir: &Path, d: &str, values: &[f64], extra: &[&str]) {
        std::fs::create_dir_all(dir.join("ticks")).unwrap();
        let mut lines: Vec<String> = values
            .iter()
            .map(|v| format!(r#"{{"event_type":"tick","symbol":"FPT","value":{v},"session":"continuous_am","is_negotiated":false}}"#))
            .collect();
        lines.extend(extra.iter().map(|s| s.to_string()));
        lines.push(r#"{"event_type":"quote","symbol":"FPT"}"#.into());
        std::fs::write(
            dir.join(format!("ticks/{d}.jsonl")),
            lines.join("\n") + "\n",
        )
        .unwrap();
    }

    #[tokio::test]
    async fn baseline_from_recorded_days() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        write_day(
            d,
            "2026-10-19",
            &[10e6, 20e6, 30e6],
            &[
                r#"{"event_type":"tick","symbol":"FPT","value":1e11,"session":"atc","is_negotiated":false}"#,
                r#"{"event_type":"tick","symbol":"FPT","value":1e11,"session":"continuous_pm","is_negotiated":true}"#,
            ],
        );
        write_day(d, "2026-10-20", &[20e6, 40e6, 60e6], &[]);
        let date = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        let p = RecordedBaselineProvider::new(d.join("ticks"), d.join("baselines"), 10, 2);
        let b = p
            .baseline("FPT", date("2026-10-21"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.median_trade_value, 30e6); // median of daily medians (20M, 40M)
        assert_eq!(b.avg_daily_value, (60e6 + 120e6) / 2.0); // auction & put-through excluded
        assert_eq!(
            (b.liquidity_tier.as_str(), b.source.as_str()),
            ("low", "recorded:2d")
        );
        assert!(d.join("baselines/2026-10-19.json").exists());
        let cold = RecordedBaselineProvider::new(d.join("ticks"), d.join("b2"), 10, 3);
        assert!(cold
            .baseline("FPT", date("2026-10-21"))
            .await
            .unwrap()
            .is_none());
        assert!(p
            .baseline("FPT", date("2026-10-20"))
            .await
            .unwrap()
            .is_none());
    }
}
