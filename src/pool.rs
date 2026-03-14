//! Connection pool for Redis.
//!
//! Manages a pool of reusable connections with semaphore-based limiting.
//! Connections are returned to the pool on drop unless they have unread data.

use crate::connection::Connection;
use crate::error::{Error, Result};
use tracing::{instrument, trace};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};

type DialFn = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TcpStream>> + Send>> + Send + Sync>;

type InitFn = Option<
    Arc<
        dyn Fn(
                Connection,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::error::Result<Connection>> + Send>>
            + Send
            + Sync,
    >,
>;

/// Pool options.
#[derive(Clone)]
pub struct PoolOptions {
    pub pool_size: usize,
    pub idle_timeout: Option<Duration>,
    pub dial_timeout: Duration,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub init_conn: InitFn,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            pool_size: 10,
            idle_timeout: None,
            dial_timeout: Duration::from_secs(5),
            read_timeout: None,
            write_timeout: None,
            init_conn: None,
        }
    }
}

/// A connection stored in the pool with its last-use timestamp.
struct PooledConnection {
    conn: Connection,
    used_at: Instant,
}

/// Connection pool.
pub struct ConnPool {
    dial: DialFn,
    opts: PoolOptions,
    semaphore: Arc<Semaphore>,
    idle: Arc<Mutex<Vec<PooledConnection>>>,
    closed: Arc<Mutex<bool>>,
}

impl ConnPool {
    /// Create a new connection pool.
    pub fn new<F>(dial: F, opts: PoolOptions) -> Self
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TcpStream>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let dial = Arc::new(dial);
        Self {
            dial: dial.clone(),
            opts: opts.clone(),
            semaphore: Arc::new(Semaphore::new(opts.pool_size)),
            idle: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Get a connection from the pool.
    #[instrument(skip(self))]
    pub async fn get(&self) -> Result<PoolGuard<'_>> {
        loop {
            let closed = self.closed.lock().await;
            if *closed {
                return Err(Error::Closed);
            }
            drop(closed);

            // Acquire permit (blocks if pool is at capacity)
            let _permit = self
                .semaphore
                .acquire()
                .await
                .map_err(|_| Error::Closed)?;

            let mut idle = self.idle.lock().await;
            if let Some(pc) = idle.pop() {
                // Evict connection if it exceeded idle timeout
                if let Some(timeout) = self.opts.idle_timeout {
                    if pc.used_at.elapsed() > timeout {
                        trace!("reusing idle connection (evicted expired)");
                        drop(idle);
                        drop(_permit);
                        continue;
                    }
                }
                trace!(idle_count = idle.len(), "reusing idle connection");
                let mut conn = pc.conn;
                conn.set_read_timeout(self.opts.read_timeout);
                conn.set_write_timeout(self.opts.write_timeout);
                return Ok(PoolGuard {
                    pool: self,
                    conn: Some(conn),
                    _permit,
                });
            }
            drop(idle);

            // No idle connection available; create a new one
            trace!("dialing new connection");
            let stream = (self.dial)().await?;
            let mut conn = Connection::new(stream);
            conn.set_read_timeout(self.opts.read_timeout);
            conn.set_write_timeout(self.opts.write_timeout);

            // Run init callback (AUTH, SELECT) if configured
            if let Some(ref init) = self.opts.init_conn {
                conn = init(conn).await?;
            }

            return Ok(PoolGuard {
                pool: self,
                conn: Some(conn),
                _permit,
            });
        }
    }

    #[instrument(skip(self, conn))]
    async fn put(&self, conn: Connection) {
        // Don't return connections with unread data (e.g. mid-pipeline)
        let buffer_size = conn.buffer_size();
        if buffer_size > 0 {
            trace!(buffer_size, "connection has unread data, not returning to pool");
            return;
        }

        let pc = PooledConnection {
            conn,
            used_at: Instant::now(),
        };

        // Hold closed lock until after push to prevent race with close()
        let closed = self.closed.lock().await;
        if *closed {
            return;
        }
        let mut idle = self.idle.lock().await;
        idle.push(pc);
        trace!(idle_count = idle.len(), "returned connection to pool");
    }

    /// Close the pool.
    #[instrument(skip(self))]
    pub async fn close(&self) -> Result<()> {
        let mut closed = self.closed.lock().await;
        if *closed {
            return Ok(());
        }
        *closed = true;
        drop(closed);

        let mut idle = self.idle.lock().await;
        idle.clear();
        Ok(())
    }
}

/// Guard that returns connection to pool when dropped.
pub struct PoolGuard<'a> {
    pool: &'a ConnPool,
    conn: Option<Connection>,
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl PoolGuard<'_> {
    /// Get the connection.
    pub fn conn(&mut self) -> &mut Connection {
        self.conn.as_mut().unwrap()
    }

    /// Remove connection from pool (don't return it).
    pub fn remove(mut self) -> Connection {
        self.conn.take().unwrap()
    }
}

impl Drop for PoolGuard<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Don't return to pool if connection has unread data
            if conn.buffer_size() > 0 {
                return;
            }
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.put(conn).await;
            });
        }
    }
}

impl Clone for ConnPool {
    fn clone(&self) -> Self {
        Self {
            dial: self.dial.clone(),
            opts: self.opts.clone(),
            semaphore: self.semaphore.clone(),
            idle: self.idle.clone(),
            closed: self.closed.clone(),
        }
    }
}
