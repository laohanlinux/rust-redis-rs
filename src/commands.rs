//! Redis command types and result handlers.
//!
//! Typed command wrappers that parse Redis replies into Rust types.
//! This module provides an alternative, builder-style API for constructing
//! and parsing Redis commands. It is reserved for future extensibility
//! when a more composable command API is needed.
//!
//! The main [`Client`](crate::client::Client) uses a simpler method-per-command
//! API. Use this module if you need typed command construction with
//! custom parsing logic.

#![allow(dead_code)]

use crate::error::Result;
use crate::parser::{Value, Z};
use std::collections::HashMap;
use std::time::Duration;

/// Base command with arguments and optional error.
#[derive(Clone)]
pub struct BaseCmd {
    pub args: Vec<String>,
    pub err: Option<crate::error::Error>,
}

impl BaseCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            args: args.into(),
            err: None,
        }
    }

    pub fn err(&self) -> Option<&crate::error::Error> {
        self.err.as_ref()
    }

    pub fn set_err(&mut self, e: crate::error::Error) {
        self.err = Some(e);
    }
}

/// Generic command returning any value.
pub struct GenericCmd {
    pub base: BaseCmd,
    pub val: Option<Value>,
}

impl GenericCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub fn result(&self) -> Result<Option<Value>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone())
    }

    pub fn val(&self) -> Option<&Value> {
        self.val.as_ref()
    }
}

/// Status command (e.g. OK, PONG).
pub struct StatusCmd {
    pub base: BaseCmd,
    pub val: Option<String>,
}

impl StatusCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Status(s) | Value::BulkString(s) => {
                self.val = Some(s);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected status").into()),
        }
    }

    pub fn result(&self) -> Result<String> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        self.val
            .clone()
            .ok_or_else(|| crate::error::Error::Other("no value".into()))
    }
}

/// Integer command.
pub struct IntCmd {
    pub base: BaseCmd,
    pub val: Option<i64>,
}

impl IntCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Int(i) => {
                self.val = Some(i);
                Ok(())
            }
            Value::BulkString(s) | Value::Status(s) => {
                self.val = Some(s.parse().map_err(|_| crate::error::redis_error("invalid int"))?);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected int").into()),
        }
    }

    pub fn result(&self) -> Result<i64> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        self.val
            .ok_or_else(|| crate::error::Error::Other("no value".into()))
    }
}

/// Bool command (from 0/1).
pub struct BoolCmd {
    pub base: BaseCmd,
    pub val: Option<bool>,
}

impl BoolCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Int(i) => {
                self.val = Some(i == 1);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected int").into()),
        }
    }

    pub fn result(&self) -> Result<bool> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        self.val
            .ok_or_else(|| crate::error::Error::Other("no value".into()))
    }
}

/// String command.
pub struct StringCmd {
    pub base: BaseCmd,
    pub val: Option<String>,
}

impl StringCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Nil => Ok(()),
            Value::Status(s) | Value::BulkString(s) => {
                self.val = Some(s);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected string").into()),
        }
    }

    pub fn result(&self) -> Result<Option<String>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone())
    }
}

/// Float command.
pub struct FloatCmd {
    pub base: BaseCmd,
    pub val: Option<f64>,
}

impl FloatCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::BulkString(s) | Value::Status(s) => {
                self.val = Some(s.parse().map_err(|_| crate::error::redis_error("invalid float"))?);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected string").into()),
        }
    }

    pub fn result(&self) -> Result<f64> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        self.val
            .ok_or_else(|| crate::error::Error::Other("no value".into()))
    }
}

/// Duration command (TTL, PTTL).
pub struct DurationCmd {
    pub base: BaseCmd,
    pub val: Option<Duration>,
    pub precision: Duration,
}

impl DurationCmd {
    pub fn new(precision: Duration, args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
            precision,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Int(i) => {
                // TTL/PTTL return -1 if no expiry, -2 if key doesn't exist
                let nanos = if i < 0 {
                    0u64
                } else {
                    (i as u64) * self.precision.as_nanos() as u64
                };
                self.val = Some(Duration::from_nanos(nanos));
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected int").into()),
        }
    }

    pub fn result(&self) -> Result<Duration> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        self.val
            .ok_or_else(|| crate::error::Error::Other("no value".into()))
    }
}

/// String slice command.
pub struct StringSliceCmd {
    pub base: BaseCmd,
    pub val: Option<Vec<String>>,
}

impl StringSliceCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Array(arr) => {
                let mut vals = Vec::with_capacity(arr.len());
                for item in arr {
                    let s = match item {
                        Value::BulkString(s) | Value::Status(s) => s,
                        _ => return Err(crate::error::redis_error("expected string").into()),
                    };
                    vals.push(s);
                }
                self.val = Some(vals);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected array").into()),
        }
    }

    pub fn result(&self) -> Result<Vec<String>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone().unwrap_or_default())
    }
}

/// Slice command (generic interface).
pub struct SliceCmd {
    pub base: BaseCmd,
    pub val: Option<Vec<Value>>,
}

impl SliceCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Array(arr) => {
                self.val = Some(arr);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected array").into()),
        }
    }

    pub fn result(&self) -> Result<Vec<Value>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone().unwrap_or_default())
    }
}

/// Bool slice command.
pub struct BoolSliceCmd {
    pub base: BaseCmd,
    pub val: Option<Vec<bool>>,
}

impl BoolSliceCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Array(arr) => {
                let mut vals = Vec::with_capacity(arr.len());
                for item in arr {
                    let i = match item {
                        Value::Int(i) => i,
                        _ => return Err(crate::error::redis_error("expected int").into()),
                    };
                    vals.push(i == 1);
                }
                self.val = Some(vals);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected array").into()),
        }
    }

    pub fn result(&self) -> Result<Vec<bool>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone().unwrap_or_default())
    }
}

/// String map command (HGETALL).
pub struct StringMapCmd {
    pub base: BaseCmd,
    pub val: Option<HashMap<String, String>>,
}

impl StringMapCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Array(arr) => {
                let mut m = HashMap::new();
                let mut i = 0;
                while i + 1 < arr.len() {
                    let k = match &arr[i] {
                        Value::BulkString(s) | Value::Status(s) => s.clone(),
                        _ => return Err(crate::error::redis_error("expected string").into()),
                    };
                    let v = match &arr[i + 1] {
                        Value::BulkString(s) | Value::Status(s) => s.clone(),
                        _ => return Err(crate::error::redis_error("expected string").into()),
                    };
                    m.insert(k, v);
                    i += 2;
                }
                self.val = Some(m);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected array").into()),
        }
    }

    pub fn result(&self) -> Result<HashMap<String, String>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone().unwrap_or_default())
    }
}

/// Z slice command (sorted set with scores).
pub struct ZSliceCmd {
    pub base: BaseCmd,
    pub val: Option<Vec<Z>>,
}

impl ZSliceCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            val: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Array(arr) => {
                let mut zz = Vec::with_capacity(arr.len() / 2);
                let mut i = 0;
                while i + 1 < arr.len() {
                    let member = match &arr[i] {
                        Value::BulkString(s) | Value::Status(s) => s.clone(),
                        _ => return Err(crate::error::redis_error("expected string").into()),
                    };
                    let score_str = match &arr[i + 1] {
                        Value::BulkString(s) | Value::Status(s) => s.clone(),
                        _ => return Err(crate::error::redis_error("expected string").into()),
                    };
                    let score: f64 = score_str.parse().map_err(|_| crate::error::redis_error("invalid float"))?;
                    zz.push(Z { score, member });
                    i += 2;
                }
                self.val = Some(zz);
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected array").into()),
        }
    }

    pub fn result(&self) -> Result<Vec<Z>> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok(self.val.clone().unwrap_or_default())
    }
}

/// Scan command.
pub struct ScanCmd {
    pub base: BaseCmd,
    pub cursor: Option<i64>,
    pub keys: Option<Vec<String>>,
}

impl ScanCmd {
    pub fn new(args: impl Into<Vec<String>>) -> Self {
        Self {
            base: BaseCmd::new(args),
            cursor: None,
            keys: None,
        }
    }

    pub async fn parse(&mut self, v: Value) -> Result<()> {
        match v {
            Value::Array(arr) => {
                if arr.len() < 2 {
                    return Err(crate::error::redis_error("invalid scan reply").into());
                }
                self.cursor = match &arr[0] {
                    Value::BulkString(s) | Value::Status(s) => s.parse().ok(),
                    Value::Int(i) => Some(*i),
                    _ => None,
                };
                self.keys = match &arr[1] {
                    Value::Array(items) => Some(
                        items
                            .iter()
                            .filter_map(|v| match v {
                                Value::BulkString(s) | Value::Status(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
                    ),
                    _ => Some(vec![]),
                };
                Ok(())
            }
            _ => Err(crate::error::redis_error("expected array").into()),
        }
    }

    pub fn result(&self) -> Result<(i64, Vec<String>)> {
        if let Some(ref e) = self.base.err {
            return Err(e.clone());
        }
        Ok((
            self.cursor.unwrap_or(0),
            self.keys.clone().unwrap_or_default(),
        ))
    }
}
