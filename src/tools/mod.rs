//! Tool registry. Adding a tool = a new module + one line in `build_tool`.

pub mod large_order_radar;
pub mod tick_counter;

use anyhow::{bail, Result};

use crate::core::config::ToolEntry;
use crate::core::tool::Tool;

/// Build a tool by id from its config entry (validated at startup).
pub fn build_tool(id: &str, entry: &ToolEntry) -> Result<Box<dyn Tool>> {
    let tool: Box<dyn Tool> = match id {
        large_order_radar::LargeOrderRadar::ID => Box::new(large_order_radar::LargeOrderRadar::from_config(&entry.config)?),
        tick_counter::TickCounter::ID => Box::new(tick_counter::TickCounter::from_config(&entry.config)?),
        other => bail!("unknown tool '{other}' in config (available: large_order_radar, tick_counter)"),
    };
    Ok(tool)
}
