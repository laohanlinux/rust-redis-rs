//! Error types for the Redis client.
//!
//! Provides error types for the Redis client including:
//! - Protocol errors (nil, invalid reply)
//! - Connection errors (closed, rate limited, timeout)
//! - Transaction errors (WATCH triggered)

use std::fmt;

/// Redis nil reply - key does not exist or similar.
#[derive(Debug, Clone)]
pub struct NilError;

impl fmt::Display for NilError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "redis: nil")
    }
}

impl std::error::Error for NilError {}

/// Redis transaction failed (WATCH was triggered).
#[derive(Debug, Clone)]
pub struct TxFailedError;

impl fmt::Display for TxFailedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "redis: transaction failed")
    }
}

impl std::error::Error for TxFailedError {}

/// Redis client is closed.
#[derive(Debug, Clone)]
pub struct ClosedError;

impl fmt::Display for ClosedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "redis: client is closed")
    }
}

impl std::error::Error for ClosedError {}

/// Rate limited - opening connections too fast.
#[derive(Debug, Clone)]
pub struct RateLimitedError;

impl fmt::Display for RateLimitedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "redis: you open connections too fast")
    }
}

impl std::error::Error for RateLimitedError {}

/// Redis protocol/server error.
#[derive(Debug, Clone)]
pub struct RedisError(pub String);

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RedisError {}

/// Main error type for the Redis client.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("redis: nil")]
    Nil,

    #[error("redis: transaction failed")]
    TxFailed,

    #[error("redis: client is closed")]
    Closed,

    #[error("redis: you open connections too fast")]
    RateLimited,

    #[error("{0}")]
    Redis(#[from] RedisError),

    #[error("IO error: {0}")]
    Io(String),

    #[error("{0}")]
    Other(String),

    #[error("timeout")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Error::Timeout
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

/// Create a Redis error from format string.
pub fn redis_error(s: &str) -> RedisError {
    RedisError(s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(Error::Nil.to_string(), "redis: nil");
        assert_eq!(Error::TxFailed.to_string(), "redis: transaction failed");
        assert_eq!(Error::Closed.to_string(), "redis: client is closed");
        assert_eq!(Error::RateLimited.to_string(), "redis: you open connections too fast");
        assert_eq!(Error::Timeout.to_string(), "timeout");
        assert_eq!(Error::Other("custom".into()).to_string(), "custom");
    }

    #[test]
    fn test_redis_error_from() {
        let re: Error = RedisError("ERR foo".into()).into();
        assert!(matches!(re, Error::Redis(_)));
        assert!(re.to_string().contains("foo"));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("refused"));
    }

    #[test]
    fn test_redis_error_helper() {
        let re = redis_error("test message");
        assert_eq!(re.0, "test message");
    }
}
