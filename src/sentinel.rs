//! Redis Sentinel support for high availability.
//!
//! Resolves master address from Sentinel and creates clients connected to the current master.

use crate::client::{Client, ClientOptions};
use crate::error::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::instrument;

/// Failover options for Sentinel.
#[derive(Clone)]
pub struct FailoverOptions {
    pub master_name: String,
    pub sentinel_addrs: Vec<String>,
    pub password: Option<String>,
    pub db: i64,
    pub pool_size: usize,
    pub dial_timeout: std::time::Duration,
    pub read_timeout: Option<std::time::Duration>,
    pub write_timeout: Option<std::time::Duration>,
    pub idle_timeout: Option<std::time::Duration>,
}

impl Default for FailoverOptions {
    fn default() -> Self {
        Self {
            master_name: "mymaster".to_string(),
            sentinel_addrs: vec!["127.0.0.1:26379".to_string()],
            password: None,
            db: 0,
            pool_size: 10,
            dial_timeout: std::time::Duration::from_secs(5),
            read_timeout: None,
            write_timeout: None,
            idle_timeout: None,
        }
    }
}

/// Client that uses Redis Sentinel for failover.
pub struct FailoverClient {
    opts: FailoverOptions,
    master_addr: Arc<RwLock<String>>,
}

impl FailoverClient {
    /// Create a new failover client.
    pub fn new(opts: FailoverOptions) -> Self {
        Self {
            master_addr: Arc::new(RwLock::new(String::new())),
            opts,
        }
    }

    /// Get the current master address from Sentinel.
    #[instrument(skip(self))]
    async fn get_master_addr(&self) -> Result<String> {
        tracing::trace!(master = %self.opts.master_name, "resolving master from sentinel");
        // Try each Sentinel until one responds
        for addr in &self.opts.sentinel_addrs {
            let client = Client::new(ClientOptions {
                addr: addr.clone(),
                password: None,
                db: 0,
                pool_size: 1,
                dial_timeout: self.opts.dial_timeout,
                read_timeout: self.opts.read_timeout.clone(),
                write_timeout: self.opts.write_timeout.clone(),
                idle_timeout: None,
            });
            match client
                .process_cmd(vec![
                    "SENTINEL".into(),
                    "get-master-addr-by-name".into(),
                    self.opts.master_name.clone(),
                ])
                .await
            {
                // SENTINEL get-master-addr-by-name returns [host, port]
                Ok(crate::parser::Value::Array(arr)) if arr.len() >= 2 => {
                    let host = match &arr[0] {
                        crate::parser::Value::BulkString(s) | crate::parser::Value::Status(s) => s.clone(),
                        _ => continue,
                    };
                    let port = match &arr[1] {
                        crate::parser::Value::BulkString(s) | crate::parser::Value::Status(s) => s.clone(),
                        _ => continue,
                    };
                    let master = format!("{host}:{port}");
                    *self.master_addr.write().await = master.clone();
                    return Ok(master);
                }
                _ => continue,
            }
        }
        Err(crate::error::Error::Other(
            "redis: all sentinels are unreachable".into(),
        ))
    }

    /// Get a client connected to the current master.
    pub async fn get_client(&self) -> Result<Client> {
        let addr = self.get_master_addr().await?;
        Ok(Client::new(ClientOptions {
            addr,
            password: self.opts.password.clone(),
            db: self.opts.db,
            pool_size: self.opts.pool_size,
            dial_timeout: self.opts.dial_timeout,
            read_timeout: self.opts.read_timeout.clone(),
            write_timeout: self.opts.write_timeout.clone(),
            idle_timeout: self.opts.idle_timeout.clone(),
        }))
    }
}
