//! Scan universe (VN100) loaded from a file and reloadable at runtime.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::core::models::Instrument;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UniverseMeta {
    pub name: String,
    #[serde(default)]
    pub as_of: Option<serde_json::Value>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub verified: bool,
}

#[derive(Debug, Deserialize)]
struct UniverseFile {
    #[serde(flatten)]
    meta: UniverseMeta,
    #[serde(default)]
    instruments: Vec<Instrument>,
}

#[derive(Debug, Clone)]
pub struct Universe {
    pub path: PathBuf,
    pub meta: UniverseMeta,
    order: Vec<String>,
    by_symbol: HashMap<String, Instrument>,
    mtime: Option<SystemTime>,
}

impl Universe {
    pub fn load(path: &Path) -> Result<Self> {
        let mut u = Universe {
            path: path.to_path_buf(),
            meta: UniverseMeta::default(),
            order: vec![],
            by_symbol: HashMap::new(),
            mtime: None,
        };
        u.reload()?;
        Ok(u)
    }

    pub fn from_instruments(name: &str, instruments: Vec<Instrument>) -> Self {
        let order = instruments.iter().map(|i| i.symbol.clone()).collect();
        let by_symbol = instruments
            .into_iter()
            .map(|i| (i.symbol.clone(), i))
            .collect();
        Universe {
            path: PathBuf::from("/dev/null"),
            meta: UniverseMeta {
                name: name.into(),
                ..Default::default()
            },
            order,
            by_symbol,
            mtime: None,
        }
    }

    fn reload(&mut self) -> Result<()> {
        let text = std::fs::read_to_string(&self.path)
            .with_context(|| format!("reading {}", self.path.display()))?;
        let f: UniverseFile = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {}", self.path.display()))?;
        self.order.clear();
        self.by_symbol.clear();
        for mut i in f.instruments {
            i.symbol = i.symbol.to_ascii_uppercase();
            if !self.by_symbol.contains_key(&i.symbol) {
                self.order.push(i.symbol.clone());
            }
            self.by_symbol.insert(i.symbol.clone(), i);
        }
        self.meta = f.meta;
        self.mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        if !self.meta.verified {
            tracing::warn!(
                "universe {} loaded from {} is NOT verified against the official list",
                self.meta.name,
                self.path.display()
            );
        }
        tracing::info!(
            "universe {}: {} instruments",
            self.meta.name,
            self.order.len()
        );
        Ok(())
    }

    /// Reload when the file changed. Returns true if membership changed.
    pub fn maybe_reload(&mut self) -> bool {
        let Ok(mtime) = std::fs::metadata(&self.path).and_then(|m| m.modified()) else {
            return false;
        };
        if Some(mtime) == self.mtime {
            return false;
        }
        let before: Vec<String> = self.order.clone();
        if let Err(e) = self.reload() {
            tracing::warn!("universe reload failed: {e:#}");
            return false;
        }
        let mut a = before;
        let mut b = self.order.clone();
        a.sort();
        b.sort();
        a != b
    }

    pub fn symbols(&self) -> Vec<String> {
        self.order.clone()
    }
    pub fn len(&self) -> usize {
        self.order.len()
    }
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
    pub fn get(&self, symbol: &str) -> Option<&Instrument> {
        self.by_symbol.get(symbol)
    }
    pub fn contains(&self, symbol: &str) -> bool {
        self.by_symbol.contains_key(symbol)
    }
    pub fn instruments(&self) -> Vec<Instrument> {
        self.order
            .iter()
            .filter_map(|s| self.by_symbol.get(s).cloned())
            .collect()
    }
}
