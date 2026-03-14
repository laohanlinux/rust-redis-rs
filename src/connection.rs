//! Redis connection handling.
//!
//! Wraps TCP stream with BufReader for efficient line-based reads.
//! Supports configurable read/write timeouts for operations.

use crate::error::Result;
use crate::parser;
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{instrument, trace};

/// A Redis connection with read/write timeout support.
pub struct Connection {
    stream: BufReader<TcpStream>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl Connection {
    /// Create a new connection from a TCP stream.
    #[instrument(skip(stream))]
    pub fn new(stream: TcpStream) -> Self {
        let addr = stream.peer_addr().ok();
        trace!(?addr, "new connection");
        Self {
            stream: BufReader::with_capacity(16 * 1024, stream),
            read_timeout: None,
            write_timeout: None,
        }
    }

    /// Set read timeout.
    pub fn set_read_timeout(&mut self, d: Option<Duration>) {
        self.read_timeout = d;
    }

    /// Set write timeout.
    pub fn set_write_timeout(&mut self, d: Option<Duration>) {
        self.write_timeout = d;
    }

    /// Write data to the connection.
    #[instrument(skip(self, buf))]
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        trace!(bytes = buf.len(), "writing");
        // Apply write timeout if configured
        if let Some(d) = self.write_timeout {
            timeout(d, self.stream.write_all(buf)).await??;
        } else {
            self.stream.write_all(buf).await?;
        }
        self.stream.flush().await?;
        Ok(())
    }

    /// Parse a reply from the connection.
    #[instrument(skip(self))]
    pub async fn parse_reply(&mut self) -> Result<parser::Value> {
        // Apply read timeout if configured; Elapsed becomes Error::Timeout
        let result = if let Some(d) = self.read_timeout {
            match timeout(d, parser::parse_reply(&mut self.stream)).await {
                Ok(r) => r,
                Err(_) => Err(crate::error::Error::Timeout),
            }
        } else {
            parser::parse_reply(&mut self.stream).await
        };
        if let Ok(ref v) = result {
            trace!(reply_type = ?std::mem::discriminant(v), "parsed reply");
        }
        result
    }

    /// Get buffer size (for checking unread data).
    pub fn buffer_size(&self) -> usize {
        self.stream.buffer().len()
    }

    /// Get the underlying stream's peer address.
    pub fn peer_addr(&self) -> Result<std::net::SocketAddr> {
        self.stream.get_ref().peer_addr().map_err(Into::into)
    }
}
