//! RESP (Redis Serialization Protocol) parser and serializer.
//!
//! Implements Redis RESP protocol for parsing reply types:
//! - Simple strings (`+`), Errors (`-`), Integers (`:`)
//! - Bulk strings (`$`), Arrays (`*`)

use crate::error::{redis_error, Error, RedisError, Result};
use async_recursion::async_recursion;
use bytes::BufMut;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

/// Number of decimal digits in n (1 for 0). Zero allocation.
#[inline]
fn decimal_len(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    let mut n = n;
    let mut len = 0;
    while n > 0 {
        len += 1;
        n /= 10;
    }
    len
}

/// Estimate buffer size needed for RESP format of args.
pub fn estimate_resp_size(args: &[impl AsRef<str>]) -> usize {
    let mut n = 4 + decimal_len(args.len()); // *<n>\r\n
    for arg in args {
        let s = arg.as_ref();
        n += 1 + decimal_len(s.len()) + 2 + s.len() + 2; // $<len>\r\n<data>\r\n
    }
    n
}

/// Serialize command arguments to RESP format and append to buffer.
///
/// Format: `*<n>\r\n` followed by n `$<len>\r\n<data>\r\n` segments.
pub fn append_args(buf: &mut Vec<u8>, args: &[impl AsRef<str>]) {
    let mut itoa_buf = itoa::Buffer::new();
    // Write array header: *<count>\r\n
    buf.put_u8(b'*');
    buf.extend_from_slice(itoa_buf.format(args.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    // Write each argument as bulk string: $<len>\r\n<data>\r\n
    for arg in args {
        let s = arg.as_ref();
        buf.put_u8(b'$');
        buf.extend_from_slice(itoa_buf.format(s.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n")
    }
}

/// Maximum line length for RESP (prevents unbounded allocation from malformed input).
const MAX_LINE_LEN: usize = 64 * 1024;

/// Read a line from the reader (until `\r\n` or `\n`).
/// Returns error if line exceeds 64KB.
pub async fn read_line<R: AsyncBufReadExt + Unpin>(rd: &mut R) -> Result<Vec<u8>> {
    let mut line = Vec::with_capacity(256);
    loop {
        let (consume_len, found_newline) = {
            let buf = rd.fill_buf().await?;
            if buf.is_empty() {
                if line.is_empty() {
                    return Err(RedisError("unexpected EOF".into()).into());
                }
                break;
            }
            let mut consume_len = buf.len();
            let mut found_newline = false;
            for (i, &b) in buf.iter().enumerate() {
                if line.len() >= MAX_LINE_LEN {
                    return Err(RedisError("line too long".into()).into());
                }
                line.push(b);
                if b == b'\n' {
                    consume_len = i + 1;
                    found_newline = true;
                    break;
                }
            }
            (consume_len, found_newline)
        };
        rd.consume(consume_len);
        if found_newline {
            // Strip trailing \r\n or \n (Redis uses \r\n, but be tolerant)
            if line.ends_with(b"\r\n") {
                line.truncate(line.len() - 2);
            } else if line.ends_with(b"\n") {
                line.truncate(line.len() - 1);
            }
            return Ok(line);
        }
    }
    Ok(line)
}

/// Read exactly n bytes.
pub async fn read_n<R: AsyncReadExt + Unpin>(rd: &mut R, n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    let mut read = 0;
    // Loop until we've read exactly n bytes (handles partial reads)
    while read < n {
        let m = rd.read(&mut buf[read..]).await?;
        if m == 0 {
            return Err(RedisError("unexpected EOF".into()).into());
        }
        read += m;
    }
    Ok(buf)
}

/// Parsed Redis reply value.
#[derive(Debug, Clone)]
pub enum Value {
    /// Nil (key does not exist, etc.)
    Nil,
    /// Simple string, e.g. `+OK`
    Status(String),
    /// Integer, e.g. `:42`
    Int(i64),
    /// Bulk string, e.g. `$5\r\nhello`
    BulkString(String),
    /// Array, e.g. `*2\r\n$3\r\nfoo\r\n$3\r\nbar`
    Array(Vec<Value>),
}

/// Parse a Redis reply, dispatching by first byte to the appropriate parser.
#[async_recursion]
pub async fn parse_reply<R>(rd: &mut R) -> Result<Value>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin + Send,
{
    let line = read_line(rd).await?;
    if line.is_empty() {
        return Err(redis_error("empty reply").into());
    }

    // Dispatch by RESP type prefix: -, +, :, $, *
    match line[0] {
        b'-' => Err(RedisError(
            String::from_utf8_lossy(&line[1..]).into_owned(),
        )
        .into()),
        b'+' => Ok(Value::Status(String::from_utf8_lossy(&line[1..]).into_owned())),
        b':' => {
            let v: i64 = std::str::from_utf8(&line[1..])
                .map_err(|_| redis_error("invalid integer"))?
                .parse()
                .map_err(|_| redis_error("invalid integer"))?;
            Ok(Value::Int(v))
        }
        b'$' => {
            // $-1 means nil in RESP
            if line.len() == 3 && line[1] == b'-' && line[2] == b'1' {
                return Err(crate::error::Error::Nil);
            }
            let reply_len: usize = std::str::from_utf8(&line[1..])
                .map_err(|_| redis_error("invalid length"))?
                .parse()
                .map_err(|_| redis_error("invalid length"))?;
            // Read reply_len bytes plus \r\n
            let mut data = read_n(rd, reply_len + 2).await?;
            data.truncate(reply_len);
            Ok(Value::BulkString(
                String::from_utf8(data).map_err(|_| redis_error("invalid utf8"))?,
            ))
        }
        b'*' => {
            // *-1 means nil array in RESP
            if line.len() == 3 && line[1] == b'-' && line[2] == b'1' {
                return Err(crate::error::Error::Nil);
            }
            let n: i64 = std::str::from_utf8(&line[1..])
                .map_err(|_| redis_error("invalid array length"))?
                .parse()
                .map_err(|_| redis_error("invalid array length"))?;
            parse_slice_inner(rd, n).await
        }
        _ => Err(redis_error(&format!("can't parse {:?}", line)).into()),
    }
}

#[async_recursion]
async fn parse_slice_inner<R>(
    rd: &mut R,
    n: i64,
) -> Result<Value>
where
    R: AsyncBufReadExt + AsyncReadExt + Unpin + Send,
{
    let mut vals = Vec::with_capacity(n as usize);
    for _ in 0..n {
        // Treat nil errors as Nil values within arrays (e.g. SORT with GET)
        match parse_reply(rd).await {
            Ok(v) => vals.push(v),
            Err(Error::Nil) => vals.push(Value::Nil),
            Err(e) => return Err(e),
        }
    }
    Ok(Value::Array(vals))
}

/// Parse array as string slice.
pub async fn parse_string_slice<R: AsyncBufReadExt + AsyncReadExt + Unpin + Send>(
    rd: &mut R,
    n: i64,
) -> Result<Vec<String>> {
    let mut vals = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let v = parse_reply(rd).await?;
        let s = match v {
            Value::BulkString(s) | Value::Status(s) => s,
            _ => return Err(redis_error("expected string").into()),
        };
        vals.push(s);
    }
    Ok(vals)
}

/// Parse array as bool slice (from integers).
pub async fn parse_bool_slice<R: AsyncBufReadExt + AsyncReadExt + Unpin + Send>(
    rd: &mut R,
    n: i64,
) -> Result<Vec<bool>> {
    let mut vals = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let v = parse_reply(rd).await?;
        let i = match v {
            Value::Int(i) => i,
            _ => return Err(redis_error("expected integer").into()),
        };
        vals.push(i == 1);
    }
    Ok(vals)
}

/// Parse array as string->string map (for HGETALL).
pub async fn parse_string_map<R: AsyncBufReadExt + AsyncReadExt + Unpin + Send>(
    rd: &mut R,
    n: i64,
) -> Result<std::collections::HashMap<String, String>> {
    // HGETALL returns [k1,v1,k2,v2,...]; n is total element count
    let mut m = std::collections::HashMap::with_capacity((n / 2) as usize);
    for _ in 0..(n / 2) {
        let k = parse_reply(rd).await?;
        let v = parse_reply(rd).await?;
        let key = match k {
            Value::BulkString(s) | Value::Status(s) => s,
            _ => return Err(redis_error("expected string").into()),
        };
        let val = match v {
            Value::BulkString(s) | Value::Status(s) => s,
            _ => return Err(redis_error("expected string").into()),
        };
        m.insert(key, val);
    }
    Ok(m)
}

/// Sorted set member with score and name.
#[derive(Debug, Clone)]
pub struct Z {
    /// Member score
    pub score: f64,
    /// Member name
    pub member: String,
}

/// Parse array as Z slice (member, score pairs).
pub async fn parse_z_slice<R: AsyncBufReadExt + AsyncReadExt + Unpin + Send>(
    rd: &mut R,
    n: i64,
) -> Result<Vec<Z>> {
    // ZRANGE WITHSCORES returns [member1,score1,member2,score2,...]
    let mut zz = Vec::with_capacity((n / 2) as usize);
    for _ in 0..(n / 2) {
        let member = parse_reply(rd).await?;
        let score = parse_reply(rd).await?;
        let member = match member {
            Value::BulkString(s) | Value::Status(s) => s,
            _ => return Err(redis_error("expected string").into()),
        };
        let score_str = match score {
            Value::BulkString(s) | Value::Status(s) => s,
            _ => return Err(redis_error("expected string").into()),
        };
        let score: f64 = score_str.parse().map_err(|_| redis_error("invalid float"))?;
        zz.push(Z { score, member });
    }
    Ok(zz)
}

/// Parse SCAN reply (cursor, keys).
pub async fn parse_scan_reply<R: AsyncBufReadExt + AsyncReadExt + Unpin + Send>(
    rd: &mut R,
) -> Result<(i64, Vec<String>)> {
    let v = parse_reply(rd).await?;
    let arr = match v {
        Value::Array(a) => a,
        _ => return Err(redis_error("expected array").into()),
    };
    if arr.len() < 2 {
        return Err(redis_error("invalid scan reply").into());
    }
    // First element is cursor (string or int), second is array of keys
    let cursor = match &arr[0] {
        Value::BulkString(s) | Value::Status(s) => s.parse().unwrap_or(0),
        Value::Int(i) => *i,
        _ => 0,
    };
    let keys = match &arr[1] {
        Value::Array(items) => items
            .iter()
            .filter_map(|v| match v {
                Value::BulkString(s) | Value::Status(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => vec![],
    };
    Ok((cursor, keys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use futures::io::Cursor;
    use tokio::io::BufReader;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    fn make_reader(data: &[u8]) -> BufReader<impl tokio::io::AsyncRead + Unpin> {
        BufReader::new(Cursor::new(data.to_vec()).compat())
    }

    #[test]
    fn test_append_args() {
        let mut buf = Vec::new();
        append_args(&mut buf, &["SET", "key", "value"]);
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn test_append_args_empty() {
        let mut buf = Vec::new();
        append_args(&mut buf, &[] as &[&str]);
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "*0\r\n");
    }

    #[test]
    fn test_append_args_single() {
        let mut buf = Vec::new();
        append_args(&mut buf, &["PING"]);
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s, "*1\r\n$4\r\nPING\r\n");
    }

    #[tokio::test]
    async fn test_parse_reply_status() {
        let mut rd = make_reader(b"+OK\r\n");
        let v = parse_reply(&mut rd).await.unwrap();
        assert!(matches!(v, Value::Status(s) if s == "OK"));
    }

    #[tokio::test]
    async fn test_parse_reply_integer() {
        let mut rd = make_reader(b":42\r\n");
        let v = parse_reply(&mut rd).await.unwrap();
        assert!(matches!(v, Value::Int(42)));
    }

    #[tokio::test]
    async fn test_parse_reply_bulk_string() {
        let mut rd = make_reader(b"$5\r\nhello\r\n");
        let v = parse_reply(&mut rd).await.unwrap();
        assert!(matches!(v, Value::BulkString(s) if s == "hello"));
    }

    #[tokio::test]
    async fn test_parse_reply_bulk_string_empty() {
        let mut rd = make_reader(b"$0\r\n\r\n");
        let v = parse_reply(&mut rd).await.unwrap();
        assert!(matches!(v, Value::BulkString(s) if s.is_empty()));
    }

    #[tokio::test]
    async fn test_parse_reply_nil_bulk() {
        let mut rd = make_reader(b"$-1\r\n");
        let err = parse_reply(&mut rd).await.unwrap_err();
        assert!(matches!(err, Error::Nil));
    }

    #[tokio::test]
    async fn test_parse_reply_nil_array() {
        let mut rd = make_reader(b"*-1\r\n");
        let err = parse_reply(&mut rd).await.unwrap_err();
        assert!(matches!(err, Error::Nil));
    }

    #[tokio::test]
    async fn test_parse_reply_error() {
        let mut rd = make_reader(b"-ERR unknown command\r\n");
        let err = parse_reply(&mut rd).await.unwrap_err();
        assert!(err.to_string().contains("unknown command"));
    }

    #[tokio::test]
    async fn test_parse_reply_array() {
        let mut rd = make_reader(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let v = parse_reply(&mut rd).await.unwrap();
        match &v {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(&arr[0], Value::BulkString(s) if s == "foo"));
                assert!(matches!(&arr[1], Value::BulkString(s) if s == "bar"));
            }
            _ => panic!("expected array, got {:?}", v),
        }
    }

    #[tokio::test]
    async fn test_parse_reply_array_empty() {
        let mut rd = make_reader(b"*0\r\n");
        let v = parse_reply(&mut rd).await.unwrap();
        assert!(matches!(v, Value::Array(arr) if arr.is_empty()));
    }

    #[tokio::test]
    async fn test_read_line_too_long() {
        // 65537 bytes without newline exceeds MAX_LINE_LEN (64KB)
        let data: Vec<u8> = vec![b'+'; 65537];
        let mut rd = make_reader(&data);
        let err = read_line(&mut rd).await.unwrap_err();
        assert!(err.to_string().contains("line too long"));
    }
}
