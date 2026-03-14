//! Basic usage example.
//!
//! Demonstrates connect, set/get, and close.

use rust_redis_rs::{Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientOptions::default());

    client.set("hello", "world").await?;
    let val: Option<String> = client.get("hello").await?;
    println!("GET hello = {:?}", val);

    let count = client.incr("counter").await?;
    println!("counter = {}", count);

    client.close().await?;
    Ok(())
}
