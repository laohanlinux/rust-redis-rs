//! Example with tracing enabled.
//!
//! Run with: RUST_LOG=rust_redis_rs=debug cargo run --example with_tracing
//! Trace events include connection creation, command execution, and pool operations.

use rust_redis_rs::{Client, ClientOptions};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "rust_redis_rs=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let client = Client::new(ClientOptions::default());

    client.set("trace_key", "trace_value").await?;
    let val = client.get("trace_key").await?;
    tracing::info!(?val, "got value");

    client.close().await?;
    Ok(())
}
