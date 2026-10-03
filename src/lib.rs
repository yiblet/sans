#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
// Core modules (essential types)
mod init;
mod sans;
mod step;

// Capability modules
pub mod build;
pub mod compose;
pub mod concurrent;
pub mod iter;
pub mod poll;
pub mod result;
pub mod run;
pub mod sequential;

// Convenience
pub mod prelude;

// Re-export essential types at root
pub use init::InitSans;
pub use sans::{PoisonError, Sans};
pub use step::Step;
