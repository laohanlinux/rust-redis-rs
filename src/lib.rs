//! Redis client for Rust - async implementation with Tokio.
//!
//! Implements the full functionality of [go-redis v2](https://github.com/redis/go-redis/tree/v2).
//! Uses RESP (Redis Serialization Protocol) over TCP.
//!
//! ## Features
//!
//! - Connection pool with configurable size
//! - All Redis 2.8 commands (except QUIT, MONITOR, SLOWLOG, SYNC)
//! - Pipelining
//! - Transactions (MULTI/EXEC)
//! - Pub/Sub
//! - Lua scripting
//! - Timeouts
//!
//! ## Example
//!
//! ```no_run
//! use rust_redis_rs::{Client, ClientOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // tracing_subscriber::fmt::init();  // uncomment to enable trace logs
//!     let client = Client::new(ClientOptions::default());
//!     client.set("key", "value").await?;
//!     let val: Option<String> = client.get("key").await?;
//!     println!("{:?}", val);
//!     client.close().await?;
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod commands;
pub mod connection;
pub mod error;
pub mod multi;
pub mod parser;
pub mod pipeline;
pub mod pool;
pub mod pubsub;
pub mod script;
pub mod sentinel;

pub use client::{Client, ClientOptions, DEFAULT_ADDR};
pub use error::{Error, Result};
pub use multi::Multi;
pub use parser::Z;
pub use pipeline::Pipeline;
pub use pubsub::{PubSub, PubSubMessage};
pub use script::Script;
pub use sentinel::{FailoverClient, FailoverOptions, DEFAULT_SENTINEL_ADDR};
