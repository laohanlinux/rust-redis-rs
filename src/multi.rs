//! Redis transactions (MULTI/EXEC).
//!
//! Queues commands and executes them atomically. Returns TxFailed if WATCH was triggered.

use crate::client::Client;
use crate::error::{Error, Result};
use crate::parser::{self, Value};
use tracing::instrument;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Transaction for MULTI/EXEC.
pub struct Multi {
    client: Client,
    cmds: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Multi {
    /// Create a new transaction.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            cmds: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add a command to the transaction.
    pub async fn cmd(&self, args: impl Into<Vec<String>>) {
        self.cmds.lock().await.push(args.into());
    }

    /// Execute the transaction.
    #[instrument(skip(self))]
    pub async fn exec(&self) -> Result<Vec<Value>> {
        let cmds: Vec<Vec<String>> = {
            let mut guard = self.cmds.lock().await;
            std::mem::take(&mut *guard)
        };
        tracing::trace!(cmd_count = cmds.len(), "executing transaction");
        if cmds.is_empty() {
            return Ok(vec![]);
        }

        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();

        // Send MULTI, all queued commands, then EXEC
        let total_size = parser::estimate_resp_size(&["MULTI"])
            + cmds.iter().map(|a| parser::estimate_resp_size(a)).sum::<usize>()
            + parser::estimate_resp_size(&["EXEC"]);
        let mut buf = Vec::with_capacity(total_size);
        parser::append_args(&mut buf, &["MULTI"]);
        for args in &cmds {
            parser::append_args(&mut buf, args);
        }
        parser::append_args(&mut buf, &["EXEC"]);

        conn.write_all(&buf).await?;

        // Consume MULTI OK, then one QUEUED per command
        let _multi_ok = conn.parse_reply().await?;
        for _ in &cmds {
            let _queued = conn.parse_reply().await?;
        }
        // EXEC returns array of results, or nil/empty if WATCH triggered
        match conn.parse_reply().await {
            Ok(Value::Array(arr)) => {
                if arr.is_empty() {
                    Err(Error::TxFailed)
                } else {
                    Ok(arr)
                }
            }
            Ok(Value::Nil) | Err(Error::Nil) => Err(Error::TxFailed),
            Ok(v) => Err(Error::Other(format!("unexpected reply: {v:?}"))),
            Err(e) => Err(e),
        }
    }

    /// Discard the transaction.
    pub async fn discard(&self) {
        self.cmds.lock().await.clear();
    }
}
