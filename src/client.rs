//! Main Redis client.

use crate::error::{Error, Result};
use crate::parser::{self, Value, Z};
use crate::pool::{ConnPool, PoolOptions};
use std::sync::Arc;
use tracing::instrument;
use std::time::Duration;
use tokio::net::TcpStream;

/// Default Redis server address.
pub const DEFAULT_ADDR: &str = "127.0.0.1:6379";

/// Client options for connecting to Redis.
#[derive(Clone)]
pub struct ClientOptions {
    /// Redis server address (e.g. "127.0.0.1:6379").
    pub addr: String,
    /// Optional AUTH password.
    pub password: Option<String>,
    /// Database index (0-15). SELECT is sent on each new connection.
    pub db: i64,
    /// Maximum connections in the pool.
    pub pool_size: usize,
    /// Timeout for establishing TCP connection.
    pub dial_timeout: Duration,
    /// Timeout for read operations; None means no timeout.
    pub read_timeout: Option<Duration>,
    /// Timeout for write operations; None means no timeout.
    pub write_timeout: Option<Duration>,
    /// Idle connection eviction; connections unused longer than this are dropped.
    pub idle_timeout: Option<Duration>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            addr: DEFAULT_ADDR.to_string(),
            password: None,
            db: 0,
            pool_size: 10,
            dial_timeout: Duration::from_secs(5),
            read_timeout: None,
            write_timeout: None,
            idle_timeout: None,
        }
    }
}

/// Redis client.
pub struct Client {
    pool: ConnPool,
    opts: Arc<ClientOptions>,
}

impl Client {
    /// Create a new TCP client.
    ///
    /// # Example
    ///
    /// ```
    /// use rust_redis_rs::{Client, ClientOptions};
    ///
    /// let client = Client::new(ClientOptions::default());
    /// ```
    #[instrument(skip(opts))]
    pub fn new(opts: ClientOptions) -> Self {
        tracing::trace!(addr = %opts.addr, db = opts.db, pool_size = opts.pool_size, "creating client");
        let addr = opts.addr.clone();
        let dial_timeout = opts.dial_timeout;
        let password = opts.password.clone();
        let db = opts.db;
        let pool_opts = PoolOptions {
            pool_size: opts.pool_size,
            idle_timeout: opts.idle_timeout,
            dial_timeout: opts.dial_timeout,
            read_timeout: opts.read_timeout,
            write_timeout: opts.write_timeout,
            // Run AUTH and/or SELECT on each new connection when needed
            init_conn: if password.is_some() || db != 0 {
                Some(Arc::new(move |mut conn: crate::connection::Connection| {
                    let password = password.clone();
                    let db = db;
                    Box::pin(async move {
                        // Authenticate if password is set
                        if let Some(ref pwd) = password {
                            let args = ["AUTH", pwd.as_str()];
                            let mut buf = Vec::with_capacity(parser::estimate_resp_size(&args));
                            parser::append_args(&mut buf, &args);
                            conn.write_all(&buf).await?;
                            let v = conn.parse_reply().await?;
                            if let Value::Status(s) = v {
                                if !s.eq_ignore_ascii_case("OK") {
                                    return Err(Error::Redis(crate::error::RedisError(s)));
                                }
                            }
                        }
                        // Select database if not 0
                        if db > 0 {
                            let db_str = db.to_string();
                            let args = ["SELECT", db_str.as_str()];
                            let mut buf = Vec::with_capacity(parser::estimate_resp_size(&args));
                            parser::append_args(&mut buf, &args);
                            conn.write_all(&buf).await?;
                            let v = conn.parse_reply().await?;
                            if let Value::Status(s) = v {
                                if !s.eq_ignore_ascii_case("OK") {
                                    return Err(Error::Redis(crate::error::RedisError(s)));
                                }
                            }
                        }
                        Ok(conn)
                    })
                }))
            } else {
                None
            },
        };
        let pool = ConnPool::new(
            move || {
                let addr = addr.clone();
                Box::pin(async move {
                    let stream = tokio::time::timeout(
                        dial_timeout,
                        TcpStream::connect(&addr),
                    )
                    .await??;
                    Ok(stream)
                })
            },
            pool_opts,
        );
        Self {
            pool,
            opts: Arc::new(opts),
        }
    }

    /// Execute a raw command (for internal use).
    /// Acquires a connection from the pool, serializes args to RESP, writes, and parses reply.
    #[instrument(skip(self, args), fields(cmd = args.first().map(|s| s.as_str()).unwrap_or("")))]
    pub(crate) async fn process_cmd(&self, args: Vec<String>) -> Result<Value> {
        tracing::trace!(args_len = args.len(), "executing command");
        let mut guard = self.pool.get().await?;
        let conn = guard.conn();
        let mut buf = Vec::with_capacity(parser::estimate_resp_size(&args));
        parser::append_args(&mut buf, &args);
        conn.write_all(&buf).await?;
        conn.parse_reply().await
    }

    // Connection commands
    pub async fn auth(&self, password: &str) -> Result<String> {
        let v = self.process_cmd(vec!["AUTH".into(), password.into()]).await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn echo(&self, message: &str) -> Result<String> {
        let v = self.process_cmd(vec!["ECHO".into(), message.into()]).await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => Ok(s),
            _ => Err("expected string".into()),
        }
    }

    pub async fn ping(&self) -> Result<String> {
        let v = self.process_cmd(vec!["PING".into()]).await?;
        match v {
            Value::Status(s) | Value::BulkString(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn select(&self, index: i64) -> Result<String> {
        let v = self
            .process_cmd(vec!["SELECT".into(), index.to_string()])
            .await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    // Key commands
    pub async fn del(&self, keys: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["DEL".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let v = self.process_cmd(vec!["EXISTS".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn expire(&self, key: &str, dur: Duration) -> Result<bool> {
        let v = self
            .process_cmd(vec![
                "EXPIRE".into(),
                key.into(),
                (dur.as_secs() as i64).to_string(),
            ])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn expire_at(&self, key: &str, tm: std::time::SystemTime) -> Result<bool> {
        let secs = tm.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        let v = self
            .process_cmd(vec!["EXPIREAT".into(), key.into(), secs.to_string()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let v = self.process_cmd(vec!["KEYS".into(), pattern.into()]).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn persist(&self, key: &str) -> Result<bool> {
        let v = self.process_cmd(vec!["PERSIST".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn pexpire(&self, key: &str, dur: Duration) -> Result<bool> {
        let v = self
            .process_cmd(vec![
                "PEXPIRE".into(),
                key.into(),
                (dur.as_millis() as i64).to_string(),
            ])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn pexpire_at(&self, key: &str, tm: std::time::SystemTime) -> Result<bool> {
        let ms = tm.duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        let v = self
            .process_cmd(vec!["PEXPIREAT".into(), key.into(), ms.to_string()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn pttl(&self, key: &str) -> Result<Duration> {
        let v = self.process_cmd(vec!["PTTL".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(Duration::from_millis(if i < 0 { 0 } else { i as u64 })),
            _ => Err("expected int".into()),
        }
    }

    pub async fn random_key(&self) -> Result<Option<String>> {
        match self.process_cmd(vec!["RANDOMKEY".into()]).await {
            Ok(Value::BulkString(s)) => Ok(Some(s)),
            Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn rename(&self, key: &str, newkey: &str) -> Result<String> {
        let v = self
            .process_cmd(vec!["RENAME".into(), key.into(), newkey.into()])
            .await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn rename_nx(&self, key: &str, newkey: &str) -> Result<bool> {
        let v = self
            .process_cmd(vec!["RENAMENX".into(), key.into(), newkey.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn ttl(&self, key: &str) -> Result<Duration> {
        let v = self.process_cmd(vec!["TTL".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(Duration::from_secs(if i < 0 { 0 } else { i as u64 })),
            _ => Err("expected int".into()),
        }
    }

    pub async fn r#type(&self, key: &str) -> Result<String> {
        let v = self.process_cmd(vec!["TYPE".into(), key.into()]).await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    // String commands
    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        match self.process_cmd(vec!["GET".into(), key.into()]).await {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<String> {
        let v = self
            .process_cmd(vec!["SET".into(), key.into(), value.into()])
            .await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn set_ex(&self, key: &str, dur: Duration, value: &str) -> Result<String> {
        let v = self
            .process_cmd(vec![
                "SETEX".into(),
                key.into(),
                (dur.as_secs() as i64).to_string(),
                value.into(),
            ])
            .await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn set_nx(&self, key: &str, value: &str) -> Result<bool> {
        let v = self
            .process_cmd(vec!["SETNX".into(), key.into(), value.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn mget(&self, keys: &[impl AsRef<str>]) -> Result<Vec<Option<String>>> {
        let mut args = vec!["MGET".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Array(arr) => Ok(array_to_option_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn mset(&self, pairs: &[(impl AsRef<str>, impl AsRef<str>)]) -> Result<String> {
        let mut args = vec!["MSET".to_string()];
        for (k, v) in pairs {
            args.push(k.as_ref().to_string());
            args.push(v.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn mset_nx(&self, pairs: &[(impl AsRef<str>, impl AsRef<str>)]) -> Result<bool> {
        let mut args = vec!["MSETNX".to_string()];
        for (k, v) in pairs {
            args.push(k.as_ref().to_string());
            args.push(v.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn incr(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["INCR".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn incr_by(&self, key: &str, value: i64) -> Result<i64> {
        let v = self
            .process_cmd(vec!["INCRBY".into(), key.into(), value.to_string()])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn incr_by_float(&self, key: &str, value: f64) -> Result<f64> {
        let v = self
            .process_cmd(vec![
                "INCRBYFLOAT".into(),
                key.into(),
                format_float(value),
            ])
            .await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => {
                s.parse().map_err(|_| "invalid float".into())
            }
            _ => Err("expected string".into()),
        }
    }

    pub async fn decr(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["DECR".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn decr_by(&self, key: &str, decrement: i64) -> Result<i64> {
        let v = self
            .process_cmd(vec!["DECRBY".into(), key.into(), decrement.to_string()])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn append(&self, key: &str, value: &str) -> Result<i64> {
        let v = self
            .process_cmd(vec!["APPEND".into(), key.into(), value.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn get_range(&self, key: &str, start: i64, end: i64) -> Result<String> {
        let v = self
            .process_cmd(vec![
                "GETRANGE".into(),
                key.into(),
                start.to_string(),
                end.to_string(),
            ])
            .await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => Ok(s),
            _ => Err("expected string".into()),
        }
    }

    pub async fn get_set(&self, key: &str, value: &str) -> Result<Option<String>> {
        match self
            .process_cmd(vec!["GETSET".into(), key.into(), value.into()])
            .await
        {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn str_len(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["STRLEN".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    // Hash commands
    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>> {
        match self
            .process_cmd(vec!["HGET".into(), key.into(), field.into()])
            .await
        {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool> {
        let v = self
            .process_cmd(vec!["HSET".into(), key.into(), field.into(), value.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn hget_all(&self, key: &str) -> Result<std::collections::HashMap<String, String>> {
        let v = self.process_cmd(vec!["HGETALL".into(), key.into()]).await?;
        match v {
            Value::Array(arr) => {
                let mut m = std::collections::HashMap::new();
                let mut i = 0;
                while i + 1 < arr.len() {
                    let k = match &arr[i] {
                        Value::BulkString(s) | Value::Status(s) => s.clone(),
                        _ => continue,
                    };
                    let v = match &arr[i + 1] {
                        Value::BulkString(s) | Value::Status(s) => s.clone(),
                        _ => continue,
                    };
                    m.insert(k, v);
                    i += 2;
                }
                Ok(m)
            }
            _ => Err("expected array".into()),
        }
    }

    pub async fn hdel(&self, key: &str, fields: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["HDEL".to_string(), key.into()];
        for f in fields {
            args.push(f.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn hexists(&self, key: &str, field: &str) -> Result<bool> {
        let v = self
            .process_cmd(vec!["HEXISTS".into(), key.into(), field.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn hlen(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["HLEN".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn hkeys(&self, key: &str) -> Result<Vec<String>> {
        let v = self.process_cmd(vec!["HKEYS".into(), key.into()]).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn hvals(&self, key: &str) -> Result<Vec<String>> {
        let v = self.process_cmd(vec!["HVALS".into(), key.into()]).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn hincr_by(&self, key: &str, field: &str, incr: i64) -> Result<i64> {
        let v = self
            .process_cmd(vec![
                "HINCRBY".into(),
                key.into(),
                field.into(),
                incr.to_string(),
            ])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn hincr_by_float(&self, key: &str, field: &str, incr: f64) -> Result<f64> {
        let v = self
            .process_cmd(vec![
                "HINCRBYFLOAT".into(),
                key.into(),
                field.into(),
                format_float(incr),
            ])
            .await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => {
                s.parse().map_err(|_| "invalid float".into())
            }
            _ => Err("expected string".into()),
        }
    }

    pub async fn hset_nx(&self, key: &str, field: &str, value: &str) -> Result<bool> {
        let v = self
            .process_cmd(vec!["HSETNX".into(), key.into(), field.into(), value.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn hmget(&self, key: &str, fields: &[impl AsRef<str>]) -> Result<Vec<Option<String>>> {
        let mut args = vec!["HMGET".to_string(), key.into()];
        for f in fields {
            args.push(f.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Array(arr) => Ok(array_to_option_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn hmset(
        &self,
        key: &str,
        pairs: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<String> {
        let mut args = vec!["HMSET".to_string(), key.into()];
        for (k, v) in pairs {
            args.push(k.as_ref().to_string());
            args.push(v.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    // List commands
    pub async fn lpush(&self, key: &str, values: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["LPUSH".to_string(), key.into()];
        for v in values {
            args.push(v.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn rpush(&self, key: &str, values: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["RPUSH".to_string(), key.into()];
        for v in values {
            args.push(v.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn lpop(&self, key: &str) -> Result<Option<String>> {
        match self.process_cmd(vec!["LPOP".into(), key.into()]).await {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn rpop(&self, key: &str) -> Result<Option<String>> {
        match self.process_cmd(vec!["RPOP".into(), key.into()]).await {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn llen(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["LLEN".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>> {
        let v = self
            .process_cmd(vec![
                "LRANGE".into(),
                key.into(),
                start.to_string(),
                stop.to_string(),
            ])
            .await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn lindex(&self, key: &str, index: i64) -> Result<Option<String>> {
        match self
            .process_cmd(vec!["LINDEX".into(), key.into(), index.to_string()])
            .await
        {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn lset(&self, key: &str, index: i64, value: &str) -> Result<String> {
        let v = self
            .process_cmd(vec![
                "LSET".into(),
                key.into(),
                index.to_string(),
                value.into(),
            ])
            .await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn lrem(&self, key: &str, count: i64, value: &str) -> Result<i64> {
        let v = self
            .process_cmd(vec![
                "LREM".into(),
                key.into(),
                count.to_string(),
                value.into(),
            ])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<String> {
        let v = self
            .process_cmd(vec![
                "LTRIM".into(),
                key.into(),
                start.to_string(),
                stop.to_string(),
            ])
            .await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn blpop(&self, keys: &[impl AsRef<str>], timeout_secs: i64) -> Result<Option<(String, String)>> {
        let mut args = vec!["BLPOP".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        args.push(timeout_secs.to_string());
        match self.process_cmd(args).await {
            Ok(Value::Array(arr)) if arr.len() >= 2 => {
                let key = match &arr[0] {
                    Value::BulkString(s) | Value::Status(s) => s.clone(),
                    _ => return Err("expected string".into()),
                };
                let val = match &arr[1] {
                    Value::BulkString(s) | Value::Status(s) => s.clone(),
                    _ => return Err("expected string".into()),
                };
                Ok(Some((key, val)))
            }
            Err(Error::Nil) => Ok(None),
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn brpop(&self, keys: &[impl AsRef<str>], timeout_secs: i64) -> Result<Option<(String, String)>> {
        let mut args = vec!["BRPOP".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        args.push(timeout_secs.to_string());
        match self.process_cmd(args).await {
            Ok(Value::Array(arr)) if arr.len() >= 2 => {
                let key = match &arr[0] {
                    Value::BulkString(s) | Value::Status(s) => s.clone(),
                    _ => return Err("expected string".into()),
                };
                let val = match &arr[1] {
                    Value::BulkString(s) | Value::Status(s) => s.clone(),
                    _ => return Err("expected string".into()),
                };
                Ok(Some((key, val)))
            }
            Err(Error::Nil) => Ok(None),
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // Set commands
    pub async fn sadd(&self, key: &str, members: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["SADD".to_string(), key.into()];
        for m in members {
            args.push(m.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn srem(&self, key: &str, members: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["SREM".to_string(), key.into()];
        for m in members {
            args.push(m.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let v = self.process_cmd(vec!["SMEMBERS".into(), key.into()]).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn sismember(&self, key: &str, member: &str) -> Result<bool> {
        let v = self
            .process_cmd(vec!["SISMEMBER".into(), key.into(), member.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i == 1),
            _ => Err("expected int".into()),
        }
    }

    pub async fn scard(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["SCARD".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn spop(&self, key: &str) -> Result<Option<String>> {
        match self.process_cmd(vec!["SPOP".into(), key.into()]).await {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn srandmember(&self, key: &str) -> Result<Option<String>> {
        match self.process_cmd(vec!["SRANDMEMBER".into(), key.into()]).await {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => Ok(Some(s)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn sdiff(&self, keys: &[impl AsRef<str>]) -> Result<Vec<String>> {
        let mut args = vec!["SDIFF".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn sinter(&self, keys: &[impl AsRef<str>]) -> Result<Vec<String>> {
        let mut args = vec!["SINTER".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn sunion(&self, keys: &[impl AsRef<str>]) -> Result<Vec<String>> {
        let mut args = vec!["SUNION".to_string()];
        for k in keys {
            args.push(k.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    // Sorted set commands
    pub async fn zadd(&self, key: &str, members: &[Z]) -> Result<i64> {
        let mut args = vec!["ZADD".to_string(), key.into()];
        for m in members {
            args.push(format_float(m.score));
            args.push(m.member.clone());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn zrem(&self, key: &str, members: &[impl AsRef<str>]) -> Result<i64> {
        let mut args = vec!["ZREM".to_string(), key.into()];
        for m in members {
            args.push(m.as_ref().to_string());
        }
        let v = self.process_cmd(args).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>> {
        let v = self
            .process_cmd(vec![
                "ZRANGE".into(),
                key.into(),
                start.to_string(),
                stop.to_string(),
            ])
            .await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn zrange_with_scores(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Z>> {
        let v = self
            .process_cmd(vec![
                "ZRANGE".into(),
                key.into(),
                start.to_string(),
                stop.to_string(),
                "WITHSCORES".into(),
            ])
            .await?;
        match v {
            Value::Array(arr) => array_to_z_vec(arr),
            _ => Err("expected array".into()),
        }
    }

    pub async fn zcard(&self, key: &str) -> Result<i64> {
        let v = self.process_cmd(vec!["ZCARD".into(), key.into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>> {
        match self
            .process_cmd(vec!["ZSCORE".into(), key.into(), member.into()])
            .await
        {
            Ok(Value::BulkString(s)) | Ok(Value::Status(s)) => {
                s.parse().map(Some).map_err(|_| "invalid float".into())
            }
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected string".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn zrank(&self, key: &str, member: &str) -> Result<Option<i64>> {
        match self
            .process_cmd(vec!["ZRANK".into(), key.into(), member.into()])
            .await
        {
            Ok(Value::Int(i)) => Ok(Some(i)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected int".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn zrevrank(&self, key: &str, member: &str) -> Result<Option<i64>> {
        match self
            .process_cmd(vec!["ZREVRANK".into(), key.into(), member.into()])
            .await
        {
            Ok(Value::Int(i)) => Ok(Some(i)),
            Err(Error::Nil) => Ok(None),
            Ok(_) => Err("expected int".into()),
            Err(e) => Err(e),
        }
    }

    pub async fn zrevrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>> {
        let v = self
            .process_cmd(vec![
                "ZREVRANGE".into(),
                key.into(),
                start.to_string(),
                stop.to_string(),
            ])
            .await?;
        match v {
            Value::Array(arr) => Ok(array_to_strings(arr)),
            _ => Err("expected array".into()),
        }
    }

    pub async fn zrevrange_with_scores(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Z>> {
        let v = self
            .process_cmd(vec![
                "ZREVRANGE".into(),
                key.into(),
                start.to_string(),
                stop.to_string(),
                "WITHSCORES".into(),
            ])
            .await?;
        match v {
            Value::Array(arr) => array_to_z_vec(arr),
            _ => Err("expected array".into()),
        }
    }

    pub async fn zcount(&self, key: &str, min: &str, max: &str) -> Result<i64> {
        let v = self
            .process_cmd(vec!["ZCOUNT".into(), key.into(), min.into(), max.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    pub async fn zincr_by(&self, key: &str, increment: f64, member: &str) -> Result<f64> {
        let v = self
            .process_cmd(vec![
                "ZINCRBY".into(),
                key.into(),
                format_float(increment),
                member.into(),
            ])
            .await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => {
                s.parse().map_err(|_| "invalid float".into())
            }
            _ => Err("expected string".into()),
        }
    }

    // Server commands
    pub async fn flush_db(&self) -> Result<String> {
        let v = self.process_cmd(vec!["FLUSHDB".into()]).await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn flush_all(&self) -> Result<String> {
        let v = self.process_cmd(vec!["FLUSHALL".into()]).await?;
        match v {
            Value::Status(s) => Ok(s),
            _ => Err("expected status".into()),
        }
    }

    pub async fn info(&self) -> Result<String> {
        let v = self.process_cmd(vec!["INFO".into()]).await?;
        match v {
            Value::BulkString(s) | Value::Status(s) => Ok(s),
            _ => Err("expected string".into()),
        }
    }

    pub async fn dbsize(&self) -> Result<i64> {
        let v = self.process_cmd(vec!["DBSIZE".into()]).await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }

    /// Close the client.
    pub async fn close(&self) -> Result<()> {
        self.pool.close().await
    }

    /// Get the underlying connection pool.
    pub fn pool(&self) -> &crate::pool::ConnPool {
        &self.pool
    }

    /// Get a pipeline for batch operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rust_redis_rs::{Client, ClientOptions};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::new(ClientOptions::default());
    /// let pipeline = client.pipeline();
    /// pipeline.set("key1", "value1").await;
    /// pipeline.set("key2", "value2").await;
    /// let results = pipeline.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn pipeline(&self) -> crate::pipeline::Pipeline {
        crate::pipeline::Pipeline::new(self.clone())
    }

    /// Get a transaction (MULTI/EXEC).
    pub fn multi(&self) -> crate::multi::Multi {
        crate::multi::Multi::new(self.clone())
    }

    /// Get a pub/sub subscriber.
    pub fn pubsub(&self) -> crate::pubsub::PubSub {
        crate::pubsub::PubSub::new(self.clone())
    }

    /// Publish a message to a channel.
    pub async fn publish(&self, channel: &str, message: &str) -> Result<i64> {
        let v = self
            .process_cmd(vec!["PUBLISH".into(), channel.into(), message.into()])
            .await?;
        match v {
            Value::Int(i) => Ok(i),
            _ => Err("expected int".into()),
        }
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            opts: Arc::clone(&self.opts),
        }
    }
}

/// Extract string elements from a Redis array (BulkString or Status).
fn array_to_strings(arr: Vec<Value>) -> Vec<String> {
    arr.into_iter()
        .filter_map(|item| match item {
            Value::BulkString(s) | Value::Status(s) => Some(s),
            _ => None,
        })
        .collect()
}

/// Extract Vec<Z> from a Redis array of [member, score, ...] pairs.
fn array_to_z_vec(arr: Vec<Value>) -> Result<Vec<Z>> {
    let mut zz = Vec::with_capacity(arr.len() / 2);
    let mut i = 0;
    while i + 1 < arr.len() {
        let member = match &arr[i] {
            Value::BulkString(s) | Value::Status(s) => s.clone(),
            _ => {
                i += 1;
                continue;
            }
        };
        let score_str = match &arr[i + 1] {
            Value::BulkString(s) | Value::Status(s) => s.clone(),
            _ => {
                i += 1;
                continue;
            }
        };
        let score: f64 = score_str
            .parse()
            .map_err(|_| Error::from("invalid float in zrange"))?;
        zz.push(Z { score, member });
        i += 2;
    }
    Ok(zz)
}

/// Extract Option<String> from a Redis array (BulkString/Status -> Some, Nil -> None).
fn array_to_option_strings(arr: Vec<Value>) -> Vec<Option<String>> {
    arr.into_iter()
        .map(|item| match item {
            Value::BulkString(s) | Value::Status(s) => Some(s),
            Value::Nil => None,
            _ => None,
        })
        .collect()
}

/// Format float for Redis: strip trailing zeros (e.g. 1.0 -> "1").
/// Returns "nan" or "inf"/"-inf" for special values (Redis accepts these).
fn format_float(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() { "inf" } else { "-inf" }.to_string();
    }
    let s = format!("{f}");
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_float() {
        assert_eq!(format_float(1.0), "1");
        assert_eq!(format_float(1.5), "1.5");
        assert_eq!(format_float(0.0), "0");
        assert_eq!(format_float(f64::NAN), "nan");
        assert_eq!(format_float(f64::INFINITY), "inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn test_client_options_default() {
        let opts = ClientOptions::default();
        assert_eq!(opts.addr, DEFAULT_ADDR);
        assert_eq!(opts.db, 0);
        assert_eq!(opts.pool_size, 10);
        assert!(opts.password.is_none());
    }
}
