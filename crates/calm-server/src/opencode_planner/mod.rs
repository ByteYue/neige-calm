//! OpenCode's native HTTP session protocol as a Planner backend. Each installed Planner
//! owns a separate authenticated serve process; its MCP configuration cannot enter another
//! Planner's instance cache. Native message IDs correlate the durable admission journal,
//! never authorize retransmission of a prompt.
mod client;
pub mod config;
mod driver;
pub mod lifecycle;
pub mod models;
mod process;
pub mod session;
pub mod stop;
mod translate;
pub mod wiring;

#[cfg(all(test, target_os = "linux"))]
mod process_tests;
#[cfg(test)]
mod tests;

#[cfg(all(test, target_os = "linux"))]
mod session_tests;
