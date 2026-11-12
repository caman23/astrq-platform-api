//! Past sessions: their recorded signals plus each tool's end-of-day archive, loaded on
//! demand into a small bounded cache (dashboard "xem lại phiên cũ").
//!
//! Abuse resistance: only dates inside the retention window that are known to have data
//! can be requested, loads run one at a time, misses are remembered for a while, and at
//! most `CAPACITY` days are held in memory.

use chrono::NaiveDate;
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::core::models::Signal;

/// Days kept in memory: a full day of signals can take tens of MB and free hosts have
/// 512 MB, so one at a time (re-reading another day from disk takes well under a second).
const CAPACITY: usize = 1;
/// How long a date without data is not looked up again.
const MISS_TTL: Duration = Duration::from_secs(600);

pub struct ArchivedDay {
    pub date: NaiveDate,
    pub signals: Vec<Arc<Signal>>,
    /// tool id -> uncompressed archive bytes
    pub archives: HashMap<String, Vec<u8>>,
}

impl ArchivedDay {
    pub fn by_tool(&self, tool_id: &str) -> Vec<Arc<Signal>> {
        self.signals
            .iter()
            .filter(|s| s.tool_id == tool_id)
            .cloned()
            .collect()
    }
}

#[derive(Default)]
pub struct History {
    days: Mutex<VecDeque<Arc<ArchivedDay>>>,
    misses: Mutex<HashMap<NaiveDate, Instant>>,
    loading: tokio::sync::Mutex<()>,
}

impl History {
    pub fn cached(&self, d: NaiveDate) -> Option<Arc<ArchivedDay>> {
        self.days.lock().iter().find(|x| x.date == d).cloned()
    }

    fn recently_missing(&self, d: NaiveDate) -> bool {
        let mut m = self.misses.lock();
        m.retain(|_, t| t.elapsed() < MISS_TTL);
        m.contains_key(&d)
    }

    /// Return the day from the cache, or run `load` (once at a time for all callers).
    pub async fn get_or_load<F, Fut>(&self, d: NaiveDate, load: F) -> Option<Arc<ArchivedDay>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<ArchivedDay>>,
    {
        if let Some(day) = self.cached(d) {
            return Some(day);
        }
        if self.recently_missing(d) {
            return None;
        }
        let _one = self.loading.lock().await;
        if let Some(day) = self.cached(d) {
            return Some(day);
        }
        match load().await {
            Some(day) => {
                let day = Arc::new(day);
                let mut days = self.days.lock();
                if days.len() >= CAPACITY {
                    days.pop_front();
                }
                days.push_back(day.clone());
                Some(day)
            }
            None => {
                self.misses.lock().insert(d, Instant::now());
                None
            }
        }
    }
}

/// Read a day's signals file and the given archives (blocking I/O).
pub fn read_day(
    date: NaiveDate,
    signals_file: &PathBuf,
    archives: &[(String, PathBuf)],
) -> Option<ArchivedDay> {
    let text = std::fs::read_to_string(signals_file).ok()?;
    let signals: Vec<Arc<Signal>> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<Signal>(l).ok())
        .map(Arc::new)
        .collect();
    if signals.is_empty() {
        return None;
    }
    let archives = archives
        .iter()
        .filter_map(|(tool, path)| {
            let gz = std::fs::read(path).ok()?;
            let raw = crate::core::remote_store::gunzip(&gz).ok()?;
            Some((tool.clone(), raw))
        })
        .collect();
    Some(ArchivedDay {
        date,
        signals,
        archives,
    })
}

/// Dates that have a `YYYY-MM-DD.jsonl` file in `dir`.
pub fn local_dates(dir: &std::path::Path) -> BTreeSet<NaiveDate> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return BTreeSet::new();
    };
    entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let d = name.strip_suffix(".jsonl")?;
            NaiveDate::parse_from_str(d, "%Y-%m-%d").ok()
        })
        .collect()
}
