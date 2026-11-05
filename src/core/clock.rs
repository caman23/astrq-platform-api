//! Clocks. Detection and scheduling run on a *market clock* so the same code works
//! live (wall clock), in accelerated simulation, and in replay (event time).

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::time::Instant;

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    /// Move a manual/event clock forward (no-op for real-time clocks).
    fn advance_to(&self, _ts: DateTime<Utc>) {}
    /// Simulated seconds per wall second (1 for wall/event clocks).
    fn speed(&self) -> f64 {
        1.0
    }
    fn kind(&self) -> &'static str;
}

pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    fn kind(&self) -> &'static str {
        "WallClock"
    }
}

/// Simulated time starting at `start`, advancing `speed`x faster than wall time.
/// `speed == 0` means manual: time only moves via `advance_to` (offline runs).
pub struct SimClock {
    start: DateTime<Utc>,
    speed: f64,
    mono0: Instant,
    manual: Mutex<DateTime<Utc>>,
}

impl SimClock {
    pub fn new(start: DateTime<Utc>, speed: f64) -> Self {
        Self {
            start,
            speed,
            mono0: Instant::now(),
            manual: Mutex::new(start),
        }
    }
}

impl Clock for SimClock {
    fn now(&self) -> DateTime<Utc> {
        if self.speed <= 0.0 {
            return *self.manual.lock();
        }
        let elapsed = self.mono0.elapsed().as_secs_f64() * self.speed;
        self.start + Duration::microseconds((elapsed * 1e6) as i64)
    }
    fn advance_to(&self, ts: DateTime<Utc>) {
        let mut m = self.manual.lock();
        if ts > *m {
            *m = ts;
        }
    }
    fn speed(&self) -> f64 {
        self.speed
    }
    fn kind(&self) -> &'static str {
        "SimClock"
    }
}

/// Time = latest exchange timestamp seen (replay).
pub struct EventClock {
    now: Mutex<DateTime<Utc>>,
}

impl EventClock {
    pub fn new(start: Option<DateTime<Utc>>) -> Self {
        Self {
            now: Mutex::new(start.unwrap_or(DateTime::<Utc>::UNIX_EPOCH)),
        }
    }
}

impl Clock for EventClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock()
    }
    fn advance_to(&self, ts: DateTime<Utc>) {
        let mut n = self.now.lock();
        if ts > *n {
            *n = ts;
        }
    }
    fn kind(&self) -> &'static str {
        "EventClock"
    }
}
