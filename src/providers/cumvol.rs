//! Cumulative-volume tracking for feeds without sequence numbers.
//!
//! Vendors (SSI `TotalVol`, DNSE `totalVolumeTraded`) send the day's cumulative matched
//! volume with every trade. That lets us drop repeated snapshots and estimate how much
//! volume was missed between two received trades.

use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub struct CumulativeVolumeTracker {
    last: HashMap<String, f64>,
}

impl CumulativeVolumeTracker {
    /// `false` if this trade is a repeat/out-of-order snapshot that must be dropped.
    pub fn accept(
        &mut self,
        key: &str,
        total: Option<f64>,
        volume: f64,
        stats: &mut BTreeMap<String, u64>,
    ) -> bool {
        let Some(total) = total else { return true };
        if let Some(prev) = self.last.get(key).copied() {
            if total <= prev {
                *stats.entry("repeat_dropped".into()).or_default() += 1;
                return false;
            }
            let missed = total - prev - volume;
            if missed > 0.0 {
                *stats.entry("missed_volume".into()).or_default() += missed as u64;
            }
        }
        self.last.insert(key.to_string(), total);
        true
    }
}
