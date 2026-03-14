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
    #[instrument(skip(self))]
    pub async fn execute(&self) -> Result<Vec<Value>> {
        let cmd_count = self.cmds.lock().await.len();
        tracing::trace!(cmd_count, "executing pipeline");
        let cmds = self.cmds.lock().await.clone();
        if cmds.is_empty() {
            return Ok(vec![]);
        }
        drop(cmds);

        let cmds = self.cmds.lock().await;
        let args_list = cmds.clone();
        drop(cmds);

        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();

        // Serialize all commands into a single buffer and send
        let mut buf = Vec::new();
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
