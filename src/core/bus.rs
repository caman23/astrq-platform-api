//! Event/signal bus: fan-out to bounded per-subscriber queues.
//!
//! Publishing never blocks: a slow subscriber overflows its own queue (counted as
//! `dropped`) instead of stalling the bus or other subscribers.

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::core::metrics::METRICS;

type Predicate<T> = Box<dyn Fn(&T) -> bool + Send + Sync>;

struct Sub<T> {
    name: String,
    predicate: Predicate<T>,
    tx: mpsc::Sender<T>,
    dropped: Arc<AtomicU64>,
}

pub struct Subscriber<T> {
    pub name: String,
    pub rx: mpsc::Receiver<T>,
    pub dropped: Arc<AtomicU64>,
}

impl<T> Subscriber<T> {
    pub fn lag(&self) -> usize {
        self.rx.len()
    }
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub struct Bus<T> {
    name: String,
    default_capacity: usize,
    subs: Mutex<Vec<Sub<T>>>,
}

impl<T: Clone + Send + 'static> Bus<T> {
    pub fn new(name: &str, default_capacity: usize) -> Self {
        Self { name: name.into(), default_capacity: default_capacity.max(1), subs: Mutex::new(Vec::new()) }
    }

    pub fn publish(&self, item: &T) {
        let mut subs = self.subs.lock();
        subs.retain(|s| !s.tx.is_closed());
        for s in subs.iter() {
            if !(s.predicate)(item) {
                continue;
            }
            if let Err(mpsc::error::TrySendError::Full(_)) = s.tx.try_send(item.clone()) {
                s.dropped.fetch_add(1, Ordering::Relaxed);
                METRICS.inc("bus_dropped_total", 1.0, &[("bus", &self.name), ("subscriber", &s.name)]);
            }
        }
    }

    pub fn subscribe(
        &self,
        name: &str,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
        capacity: Option<usize>,
    ) -> Subscriber<T> {
        let (tx, rx) = mpsc::channel(capacity.unwrap_or(self.default_capacity).max(1));
        let dropped = Arc::new(AtomicU64::new(0));
        self.subs.lock().push(Sub { name: name.into(), predicate: Box::new(predicate), tx, dropped: dropped.clone() });
        Subscriber { name: name.into(), rx, dropped }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subs.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_subscriber_drops_without_blocking_others() {
        let bus: Bus<u32> = Bus::new("t", 2);
        let mut slow = bus.subscribe("slow", |_| true, Some(1));
        let mut even = bus.subscribe("even", |x| x % 2 == 0, Some(10));
        for i in 0..6 {
            bus.publish(&i);
        }
        assert_eq!(slow.dropped(), 5);
        assert_eq!(slow.rx.recv().await, Some(0));
        let mut got = vec![];
        while let Ok(v) = even.rx.try_recv() {
            got.push(v);
        }
        assert_eq!(got, vec![0, 2, 4]);
    }
}
