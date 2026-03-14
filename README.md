# rust-redis-rs

Redis client for Rust - async implementation with Tokio. Implements the full functionality of [go-redis v2](https://github.com/redis/go-redis/tree/v2).

See [doc/architecture.md](doc/architecture.md) for architecture diagram and business process flowcharts.

## Features

- **Connection pool** - Configurable pool size and idle timeout
- **Redis 2.8 commands** - All commands except QUIT, MONITOR, SLOWLOG, SYNC
- **Pipelining** - Batch multiple commands for reduced latency
- **Transactions** - MULTI/EXEC with WATCH support
- **Pub/Sub** - Subscribe to channels and patterns
- **Lua scripting** - EVAL, EVALSHA with automatic fallback
- **Redis Sentinel** - High availability failover support
- **Timeouts** - Configurable dial, read, and write timeouts

## Installation

```toml
[dependencies]
rust-redis-rs = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "net", "io-util", "time", "sync", "macros"] }
```

Or use `features = ["full"]` for all Tokio features (larger binary).

## Usage

```rust
use rust_redis_rs::{Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(ClientOptions::default());
    
    client.set("key", "value").await?;
    let val: Option<String> = client.get("key").await?;
    println!("{:?}", val);
    
    client.close().await?;
    Ok(())
}
```

### With password and database

```rust
use rust_redis_rs::{Client, ClientOptions, DEFAULT_ADDR};

let client = Client::new(ClientOptions {
    addr: DEFAULT_ADDR.to_string(),
    password: Some("secret".to_string()),
    db: 1,
    ..Default::default()
});
```

### Pipeline

```rust
let pipeline = client.pipeline();
pipeline.set("key1", "value1").await;
pipeline.set("key2", "value2").await;
let results = pipeline.execute().await?;
```

### Transaction

```rust
let multi = client.multi();
multi.cmd(vec!["SET", "key", "value"]).await;
multi.cmd(vec!["INCR", "counter"]).await;
let results = multi.exec().await?;
```

### Pub/Sub

```rust
let pubsub = client.pubsub();
pubsub.subscribe(&["channel1"]).await?;
let msg = pubsub.receive().await?;
```

### Lua Script

```rust
let script = Script::new("return redis.call('GET', KEYS[1])");
let result = script.run(&client, &["mykey"], &[]).await?;
```

## Tracing

The crate uses [tracing](https://docs.rs/tracing) for observability. Enable logs with a subscriber:

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

tracing_subscriber::registry()
    .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "rust_redis_rs=debug".into()))
    .with(tracing_subscriber::fmt::layer())
    .init();
```

Then run with `RUST_LOG=rust_redis_rs=trace` for verbose output. Trace events include:
- Connection creation and pool operations
- Command execution
- Pipeline and transaction execution
- Pub/Sub subscribe/receive
- Lua script execution
- Sentinel master resolution

## License

BSD-2-Clause
