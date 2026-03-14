//! Redis Pub/Sub.
//!
//! Subscribe to channels or patterns and receive messages.
//! Uses a dedicated connection (not returned to pool while subscribed).

use crate::client::Client;
use crate::error::Result;
use crate::parser::{self, Value};
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
pub struct PubSub {
    client: Client,
}

impl PubSub {
    /// Create a new Pub/Sub subscriber.
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Subscribe to channels.
    #[instrument(skip(self, channels))]
    pub async fn subscribe(&self, channels: &[impl AsRef<str>]) -> Result<()> {
        tracing::trace!(channel_count = channels.len(), "subscribing");
        let mut args = vec!["SUBSCRIBE".to_string()];
        for c in channels {
            args.push(c.as_ref().to_string());
        }
        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();
        let mut buf = Vec::new();
        parser::append_args(&mut buf, &args);
        conn.write_all(&buf).await?;
        Ok(())
    }

    /// Subscribe to patterns.
    pub async fn psubscribe(&self, patterns: &[impl AsRef<str>]) -> Result<()> {
        let mut args = vec!["PSUBSCRIBE".to_string()];
        for p in patterns {
            args.push(p.as_ref().to_string());
        }
        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();
        let mut buf = Vec::new();
        parser::append_args(&mut buf, &args);
        conn.write_all(&buf).await?;
        Ok(())
    }

    /// Unsubscribe from channels.
    pub async fn unsubscribe(&self, channels: &[impl AsRef<str>]) -> Result<()> {
        let mut args = vec!["UNSUBSCRIBE".to_string()];
        for c in channels {
            args.push(c.as_ref().to_string());
        }
        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();
        let mut buf = Vec::new();
        parser::append_args(&mut buf, &args);
        conn.write_all(&buf).await?;
        Ok(())
    }

    /// Unsubscribe from patterns.
    pub async fn punsubscribe(&self, patterns: &[impl AsRef<str>]) -> Result<()> {
        let mut args = vec!["PUNSUBSCRIBE".to_string()];
        for p in patterns {
            args.push(p.as_ref().to_string());
        }
        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();
        let mut buf = Vec::new();
        parser::append_args(&mut buf, &args);
        conn.write_all(&buf).await?;
        Ok(())
    }

    /// Receive the next message.
    #[instrument(skip(self))]
    pub async fn receive(&self) -> Result<PubSubMessage> {
        let mut guard = self.client.pool().get().await?;
        let conn = guard.conn();
        let v = conn.parse_reply().await?;
        // Pub/Sub messages are arrays: [kind, channel/pattern, payload/count]
        match v {
            Value::Array(arr) if arr.len() >= 3 => {
                let kind = match &arr[0] {
                    Value::BulkString(s) | Value::Status(s) => s.clone(),
                    _ => return Err(crate::error::Error::Other("expected string".into())),
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
                    _ => Err(crate::error::Error::Other(format!(
                        "unsupported message: {kind}"
                    ))),
                }
            }
            _ => Err(crate::error::Error::Other("expected array".into())),
        }
    }
}
