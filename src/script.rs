//! Lua script support.
//!
//! Run Lua scripts via EVAL/EVALSHA. Scripts are hashed at creation;
//! run() tries EVALSHA first and falls back to EVAL on NOSCRIPT.

use crate::client::Client;
use crate::error::Result;
use crate::parser::Value;
use sha1::{Digest, Sha1};
use tracing::instrument;

/// Lua script with cached SHA1 hash.
pub struct Script {
    src: String,
    hash: String,
}

impl Script {
    /// Create a new script from source.
    /// Computes SHA1 hash for EVALSHA.
    pub fn new(src: impl Into<String>) -> Self {
        let src = src.into();
        let hash = {
            let mut hasher = Sha1::new();
            hasher.update(src.as_bytes());
            hex::encode(hasher.finalize())
        };
        Self { src, hash }
    }

    /// Load the script on the server.
    pub async fn load(&self, client: &Client) -> Result<String> {
        let v = client
            .process_cmd(vec!["SCRIPT".into(), "LOAD".into(), self.src.clone()])
            .await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => Ok(s),
            _ => Err(crate::error::Error::Other("expected string".into())),
        }
    }

    /// Check if script exists.
    pub async fn exists(&self, client: &Client) -> Result<Vec<bool>> {
        let v = client
            .process_cmd(vec!["SCRIPT".into(), "EXISTS".into(), self.hash.clone()])
            .await?;
        match v {
            Value::Array(arr) => {
                let mut r = Vec::new();
                for item in arr {
                    if let Value::Int(i) = item {
                        r.push(i == 1);
                    }
                }
                Ok(r)
            }
            _ => Err(crate::error::Error::Other("expected array".into())),
        }
    }

    /// Run the script (tries EVALSHA first, falls back to EVAL).
    #[instrument(skip(self, client, keys, args))]
    pub async fn run(
        &self,
        client: &Client,
        keys: &[impl AsRef<str>],
        args: &[impl AsRef<str>],
    ) -> Result<Value> {
        let mut cmd_args = vec![
            "EVALSHA".to_string(),
            self.hash.clone(),
            keys.len().to_string(),
        ];
        for k in keys {
            cmd_args.push(k.as_ref().to_string());
        }
        for a in args {
            cmd_args.push(a.as_ref().to_string());
        }

        match client.process_cmd(cmd_args).await {
            Ok(v) => Ok(v),
            // Script not loaded; fall back to EVAL with full source
            Err(e) if e.to_string().contains("NOSCRIPT") => {
                let mut cmd_args = vec![
                    "EVAL".to_string(),
                    self.src.clone(),
                    keys.len().to_string(),
                ];
                for k in keys {
                    cmd_args.push(k.as_ref().to_string());
                }
                for a in args {
                    cmd_args.push(a.as_ref().to_string());
                }
                client.process_cmd(cmd_args).await
            }
            Err(e) => Err(e),
        }
    }

    /// Run with EVAL (always sends full script).
    pub async fn eval(
        &self,
        client: &Client,
        keys: &[impl AsRef<str>],
        args: &[impl AsRef<str>],
    ) -> Result<Value> {
        let mut cmd_args = vec![
            "EVAL".to_string(),
            self.src.clone(),
            keys.len().to_string(),
        ];
        for k in keys {
            cmd_args.push(k.as_ref().to_string());
        }
        for a in args {
            cmd_args.push(a.as_ref().to_string());
        }
        client.process_cmd(cmd_args).await
    }

    /// Run with EVALSHA.
    pub async fn evalsha(
        &self,
        client: &Client,
        keys: &[impl AsRef<str>],
        args: &[impl AsRef<str>],
    ) -> Result<Value> {
        let mut cmd_args = vec![
            "EVALSHA".to_string(),
            self.hash.clone(),
            keys.len().to_string(),
        ];
        for k in keys {
            cmd_args.push(k.as_ref().to_string());
        }
        for a in args {
            cmd_args.push(a.as_ref().to_string());
        }
        client.process_cmd(cmd_args).await
    }
}
