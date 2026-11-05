//! Asynchronous JSONL recorder (ticks, signals, raw vendor messages). Never blocks the
//! hot path: lines go through a bounded channel and are flushed in a background task.
//! Recorded tick files are directly usable by the replay provider.

use chrono::{DateTime, NaiveDate, Utc};
use parking_lot::Mutex;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::core::metrics::METRICS;
use crate::core::sessions::SessionCalendar;

pub struct JsonlRecorder {
    pub dir: PathBuf,
    calendar: SessionCalendar,
    kind: String,
    tx: mpsc::Sender<(NaiveDate, String)>,
    rx: Mutex<Option<mpsc::Receiver<(NaiveDate, String)>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

fn flush(dir: &Path, batch: &[(NaiveDate, String)]) {
    let mut by_day: std::collections::BTreeMap<NaiveDate, Vec<&str>> = Default::default();
    for (d, line) in batch {
        by_day.entry(*d).or_default().push(line);
    }
    for (d, lines) in by_day {
        let path = dir.join(format!("{d}.jsonl"));
        let res = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .and_then(|mut f| {
                let mut buf = lines.join("\n");
                buf.push('\n');
                f.write_all(buf.as_bytes())
            });
        if let Err(e) = res {
            tracing::warn!("recorder write {} failed: {e}", path.display());
        }
    }
}

impl JsonlRecorder {
    pub fn new(dir: PathBuf, calendar: SessionCalendar, kind: &str) -> Self {
        let (tx, rx) = mpsc::channel(200_000);
        Self {
            dir,
            calendar,
            kind: kind.into(),
            tx,
            rx: Mutex::new(Some(rx)),
            task: Mutex::new(None),
        }
    }

    pub fn write<T: serde::Serialize>(&self, item: &T, ts: DateTime<Utc>) {
        if let Ok(line) = serde_json::to_string(item) {
            self.write_line(&line, ts);
        }
    }

    /// Append one pre-serialized line (e.g. a raw vendor message).
    pub fn write_line(&self, line: &str, ts: DateTime<Utc>) {
        let line = line.replace('\n', " ");
        if self
            .tx
            .try_send((self.calendar.trading_date(ts), line))
            .is_err()
        {
            METRICS.inc("recorder_dropped_total", 1.0, &[("kind", &self.kind)]);
        }
    }

    pub fn file_for(&self, d: NaiveDate) -> PathBuf {
        self.dir.join(format!("{d}.jsonl"))
    }

    pub fn start(&self) {
        let Some(mut rx) = self.rx.lock().take() else {
            return;
        };
        let _ = std::fs::create_dir_all(&self.dir);
        let dir = self.dir.clone();
        let handle = tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut batch = vec![first];
                while batch.len() < 5000 {
                    match rx.try_recv() {
                        Ok(x) => batch.push(x),
                        Err(_) => break,
                    }
                }
                let dir = dir.clone();
                let _ = tokio::task::spawn_blocking(move || flush(&dir, &batch)).await;
            }
        });
        *self.task.lock() = Some(handle);
    }

    /// Give queued lines a moment to reach disk, then stop the writer.
    pub async fn close(&self) {
        for _ in 0..20 {
            if self.tx.capacity() == self.tx.max_capacity() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Some(t) = self.task.lock().take() {
            t.abort();
        }
    }

    /// Delete files older than `keep_days` (by mtime). Returns the count removed.
    pub fn cleanup(&self, keep_days: u64) -> usize {
        let cutoff = SystemTime::now() - Duration::from_secs(keep_days * 86_400);
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return 0;
        };
        entries
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "jsonl"))
            .filter(|e| {
                e.metadata()
                    .and_then(|m| m.modified())
                    .is_ok_and(|m| m < cutoff)
            })
            .filter(|e| std::fs::remove_file(e.path()).is_ok())
            .count()
    }
}
