//! Redis Pub/Sub.
//!
//! Subscribe to channels or patterns and receive messages.
//! Uses a dedicated connection (not returned to pool while subscribed).

use crate::client::Client;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::parser::{self, Value};
use tokio::sync::Mutex;
use tracing::instrument;

/// Pub/Sub message.
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Subscription confirmation.
    Subscription {
        kind: String,
        channel: String,
        count: i64,
    },
    /// Regular message.
    Message { channel: String, payload: String },
    /// Pattern message.
    PMessage {
        pattern: String,
        channel: String,
        payload: String,
    },
}

/// Pub/Sub subscriber.
/// Holds a dedicated connection for the subscription session.
pub struct PubSub {
    client: Client,
    conn: Mutex<Option<Connection>>,
}

impl PubSub {
    /// Create a new Pub/Sub subscriber.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            conn: Mutex::new(None),
        }
    }

    /// Ensure we have a dedicated connection; acquire from pool if needed.
    async fn ensure_conn(&self) -> Result<()> {
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            let pool_guard = self.client.pool().get().await?;
            let conn = pool_guard.remove();
            *guard = Some(conn);
        }
        Ok(())
    }

    /// Send a subscribe/unsubscribe command.
    async fn send_sub_cmd(&self, cmd: &str, items: &[impl AsRef<str>]) -> Result<()> {
        self.ensure_conn().await?;
        let mut args = vec![cmd.to_string()];
        for item in items {
            args.push(item.as_ref().to_string());
        }
        let mut guard = self.conn.lock().await;
        let conn = guard.as_mut().expect("connection established by ensure_conn");
        let mut buf = Vec::with_capacity(parser::estimate_resp_size(&args));
        parser::append_args(&mut buf, &args);
        conn.write_all(&buf).await?;
        Ok(())
    }

    /// Subscribe to channels.
    #[instrument(skip(self, channels))]
    pub async fn subscribe(&self, channels: &[impl AsRef<str>]) -> Result<()> {
        tracing::trace!(channel_count = channels.len(), "subscribing");
        self.send_sub_cmd("SUBSCRIBE", channels).await
    }

    /// Subscribe to patterns.
    pub async fn psubscribe(&self, patterns: &[impl AsRef<str>]) -> Result<()> {
        self.send_sub_cmd("PSUBSCRIBE", patterns).await
    }

    /// Unsubscribe from channels.
    pub async fn unsubscribe(&self, channels: &[impl AsRef<str>]) -> Result<()> {
        self.send_sub_cmd("UNSUBSCRIBE", channels).await
    }

    /// Unsubscribe from patterns.
    pub async fn punsubscribe(&self, patterns: &[impl AsRef<str>]) -> Result<()> {
        self.send_sub_cmd("PUNSUBSCRIBE", patterns).await
    }

    /// Receive the next message.
    #[instrument(skip(self))]
    pub async fn receive(&self) -> Result<PubSubMessage> {
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| Error::from("not subscribed; call subscribe or psubscribe first"))?;
        let v = conn.parse_reply().await?;
        // Pub/Sub messages are arrays: [kind, channel/pattern, payload/count]
        match v {
            Value::Array(arr) if arr.len() >= 3 => {
                let kind = match &arr[0] {
                    Value::BulkString(s) | Value::Status(s) => s.clone(),
                    _ => return Err("expected string".into()),
                };
                match kind.as_str() {
                    // Subscription confirmation: [kind, channel, count]
                    "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" => {
                        let channel = match &arr[1] {
                            Value::BulkString(s) | Value::Status(s) => s.clone(),
                            _ => String::new(),
                        };
                        let count = match &arr[2] {
                            Value::Int(i) => *i,
                            _ => 0,
                        };
                        Ok(PubSubMessage::Subscription {
                            kind,
                            channel,
                            count,
                        })
                    }
                    // Regular message: [message, channel, payload]
                    "message" => {
                        let channel = match &arr[1] {
                            Value::BulkString(s) | Value::Status(s) => s.clone(),
                            _ => String::new(),
                        };
                        let payload = match &arr[2] {
                            Value::BulkString(s) | Value::Status(s) => s.clone(),
                            _ => String::new(),
                        };
                        Ok(PubSubMessage::Message { channel, payload })
                    }
                    // Pattern message: [pmessage, pattern, channel, payload]
                    "pmessage" if arr.len() >= 4 => {
                        let pattern = match &arr[1] {
                            Value::BulkString(s) | Value::Status(s) => s.clone(),
                            _ => String::new(),
                        };
                        let channel = match &arr[2] {
                            Value::BulkString(s) | Value::Status(s) => s.clone(),
                            _ => String::new(),
                        };
                        let payload = match &arr[3] {
                            Value::BulkString(s) | Value::Status(s) => s.clone(),
                            _ => String::new(),
                        };
                        Ok(PubSubMessage::PMessage {
                            pattern,
                            channel,
                            payload,
                        })
                    }
                    _ => Err(Error::Other(format!(
                        "unsupported message: {kind}"
                    ))),
                }
            }
            _ => Err("expected array".into()),
        }
    }
}
