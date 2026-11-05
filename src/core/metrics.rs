//! Tiny in-process metrics registry with Prometheus text exposition.

use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::LazyLock;

type Key = (String, Vec<(String, String)>);

fn key(name: &str, labels: &[(&str, &str)]) -> Key {
    let mut l: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    l.sort();
    (name.to_string(), l)
}

#[derive(Default)]
struct Summary {
    count: u64,
    total: f64,
    maximum: f64,
    recent: VecDeque<f64>,
}

impl Summary {
    fn observe(&mut self, v: f64) {
        self.count += 1;
        self.total += v;
        self.maximum = self.maximum.max(v);
        if self.recent.len() == 512 {
            self.recent.pop_front();
        }
        self.recent.push_back(v);
    }
    fn quantile(&self, q: f64) -> f64 {
        if self.recent.is_empty() {
            return 0.0;
        }
        let mut d: Vec<f64> = self.recent.iter().copied().collect();
        d.sort_by(f64::total_cmp);
        d[((q * d.len() as f64) as usize).min(d.len() - 1)]
    }
}

#[derive(Default)]
pub struct Metrics {
    counters: Mutex<BTreeMap<Key, f64>>,
    gauges: Mutex<BTreeMap<Key, f64>>,
    summaries: Mutex<BTreeMap<Key, Summary>>,
}

impl Metrics {
    pub fn inc(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        *self.counters.lock().entry(key(name, labels)).or_insert(0.0) += value;
    }
    pub fn set(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.gauges.lock().insert(key(name, labels), value);
    }
    pub fn observe(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        self.summaries
            .lock()
            .entry(key(name, labels))
            .or_default()
            .observe(value);
    }
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.counters
            .lock()
            .get(&key(name, labels))
            .copied()
            .unwrap_or(0.0)
    }
    pub fn summary(&self, name: &str, labels: &[(&str, &str)]) -> Value {
        match self.summaries.lock().get(&key(name, labels)) {
            Some(s) => {
                json!({"count": s.count, "p50": s.quantile(0.5), "p95": s.quantile(0.95), "max": s.maximum})
            }
            None => json!({"count": 0, "p50": 0.0, "p95": 0.0, "max": 0.0}),
        }
    }

    pub fn render_prometheus(&self) -> String {
        fn fmt(name: &str, labels: &[(String, String)], extra: Option<String>) -> String {
            let mut parts: Vec<String> =
                labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
            parts.extend(extra);
            if parts.is_empty() {
                format!("astrq_{name}")
            } else {
                format!("astrq_{name}{{{}}}", parts.join(","))
            }
        }
        let mut lines = Vec::new();
        for ((n, lb), v) in self.counters.lock().iter() {
            lines.push(format!("{} {v}", fmt(n, lb, None)));
        }
        for ((n, lb), v) in self.gauges.lock().iter() {
            lines.push(format!("{} {v}", fmt(n, lb, None)));
        }
        for ((n, lb), s) in self.summaries.lock().iter() {
            for q in [0.5, 0.95] {
                lines.push(format!(
                    "{} {}",
                    fmt(n, lb, Some(format!("quantile=\"{q}\""))),
                    s.quantile(q)
                ));
            }
            lines.push(format!(
                "{} {}",
                fmt(&format!("{n}_count"), lb, None),
                s.count
            ));
            lines.push(format!(
                "{} {}",
                fmt(&format!("{n}_sum"), lb, None),
                s.total
            ));
        }
        lines.join("\n") + "\n"
    }
}

pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);
