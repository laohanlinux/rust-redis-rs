//! Redis pipeline for batch operations.
//!
//! Buffers multiple commands and sends them in one write, then reads all replies.
//! Reduces round-trips when issuing many commands.

use crate::client::Client;
use crate::error::Result;
use crate::parser::{self, Value};
use tracing::instrument;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Pipeline for batching multiple commands.
pub struct Pipeline {
    client: Client,
    cmds: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Pipeline {
    /// Create a new pipeline.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cmds: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add a command to the pipeline.
    pub async fn cmd(&self, args: impl Into<Vec<String>>) {
        self.cmds.lock().await.push(args.into());
    }

    /// Add SET command.
    pub async fn set(&self, key: &str, value: &str) {
        self.cmd(vec!["SET".into(), key.into(), value.into()]).await;
    }

    /// Add GET command.
    pub async fn get(&self, key: &str) {
        self.cmd(vec!["GET".into(), key.into()]).await;
    }

    /// Add DEL command.
    pub async fn del(&self, keys: &[impl AsRef<str>]) {
        let mut args = vec!["DEL".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        self.cmd(args).await;
    }

    /// Execute all commands in the pipeline and return results in order.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rust_redis_rs::{Client, ClientOptions};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new(ClientOptions::default());
    /// let pipeline = client.pipeline();
    /// pipeline.set("k1", "v1").await;
    /// pipeline.get("k1").await;
    /// let results = pipeline.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self))]
    pub async fn execute(&self) -> Result<Vec<Value>> {
        let args_list: Vec<Vec<String>> = {
            let mut cmds = self.cmds.lock().await;
            std::mem::take(&mut *cmds)
        };
        tracing::trace!(cmd_count = args_list.len(), "executing pipeline");
        if args_list.is_empty() {
            return Ok(vec![]);
        }

        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();

        // Serialize all commands into a single buffer and send
        let total_size: usize = args_list.iter().map(|a| parser::estimate_resp_size(a)).sum();
        let mut buf = Vec::with_capacity(total_size);
        for args in &args_list {
            parser::append_args(&mut buf, args);
        }
        conn.write_all(&buf).await?;

        // Read one reply per command (order preserved)
        let mut results = Vec::new();
        for _ in &args_list {
            let v = conn.parse_reply().await?;
            results.push(v);
        }
        Ok(results)
    }

    /// Clear all commands without executing.
    pub async fn clear(&self) {
        self.cmds.lock().await.clear();
    }
}
