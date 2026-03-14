//! Integration tests - require Redis to be running.
//!
//! Run with: cargo test --test integration_test -- --ignored

use std::fs::OpenOptions;
use rust_redis_rs::{Client, ClientOptions, Script, Z};
use std::time::Duration;

fn client() -> Client {
    Client::new(ClientOptions::default())
}

#[tokio::test]
#[ignore] // Run with: cargo test --test integration_test -- --ignored
async fn test_basic_operations() {
    let client = client();

    client.set("test_key", "test_value").await.unwrap();
    let val = client.get("test_key").await.unwrap();
    assert_eq!(val, Some("test_value".to_string()));

    client.del(&["test_key"]).await.unwrap();
    let val = client.get("test_key").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
#[ignore]
async fn test_hash_operations() {
    let client = client();
    client.del(&["test_hash"]).await.ok();

    client.hset("test_hash", "field1", "value1").await.unwrap();
    let val = client.hget("test_hash", "field1").await.unwrap();
    assert_eq!(val, Some("value1".to_string()));

    let all = client.hget_all("test_hash").await.unwrap();
    assert_eq!(all.get("field1"), Some(&"value1".to_string()));
}

#[tokio::test]
#[ignore]
async fn test_sorted_set_operations() {
    let client = client();
    client.del(&["test_zset"]).await.ok();

    client
        .zadd("test_zset", &[Z { score: 1.0, member: "a".into() }, Z { score: 2.0, member: "b".into() }])
        .await
        .unwrap();

    let members = client.zrange("test_zset", 0, -1).await.unwrap();
    assert_eq!(members, vec!["a", "b"]);
}

#[tokio::test]
#[ignore]
async fn test_connection_commands() {
    let client = client();

    let pong = client.ping().await.unwrap();
    assert_eq!(pong, "PONG");

    let echoed = client.echo("hello").await.unwrap();
    assert_eq!(echoed, "hello");
}

#[tokio::test]
#[ignore]
async fn test_list_operations() {
    let client = client();
    client.del(&["test_list"]).await.ok();

    client.lpush("test_list", &["c", "b", "a"]).await.unwrap();
    let len = client.llen("test_list").await.unwrap();
    assert_eq!(len, 3);

    let range = client.lrange("test_list", 0, -1).await.unwrap();
    assert_eq!(range, vec!["a", "b", "c"]);

    let popped = client.lpop("test_list").await.unwrap();
    assert_eq!(popped, Some("a".into()));

    client.rpush("test_list", &["d"]).await.unwrap();
    let val = client.rpop("test_list").await.unwrap();
    assert_eq!(val, Some("d".into()));
}

#[tokio::test]
#[ignore]
async fn test_set_operations() {
    let client = client();
    client.del(&["test_set"]).await.ok();

    client.sadd("test_set", &["a", "b", "c"]).await.unwrap();
    let card = client.scard("test_set").await.unwrap();
    assert_eq!(card, 3);

    let is_member = client.sismember("test_set", "b").await.unwrap();
    assert!(is_member);

    let members = client.smembers("test_set").await.unwrap();
    assert_eq!(members.len(), 3);
    assert!(members.contains(&"a".into()));
}

#[tokio::test]
#[ignore]
async fn test_string_commands() {
    let client = client();
    client.del(&["k1", "k2", "k3", "counter"]).await.ok();

    client.mset(&[("k1", "v1"), ("k2", "v2")]).await.unwrap();
    let vals = client.mget(&["k1", "k2", "k3"]).await.unwrap();
    assert_eq!(vals, vec![Some("v1".into()), Some("v2".into()), None]);

    let n = client.incr("counter").await.unwrap();
    assert_eq!(n, 1);
    let n = client.incr_by("counter", 10).await.unwrap();
    assert_eq!(n, 11);
}

#[tokio::test]
#[ignore]
async fn test_key_commands() {
    let client = client();
    client.del(&["expire_key"]).await.ok();
    client.set("expire_key", "val").await.unwrap();

    let ok = client.expire("expire_key", Duration::from_secs(60)).await.unwrap();
    assert!(ok);

    let ttl = client.ttl("expire_key").await.unwrap();
    assert!(ttl.as_secs() > 0 && ttl.as_secs() <= 60);

    let key_type = client.r#type("expire_key").await.unwrap();
    assert_eq!(key_type, "string");
}

#[tokio::test]
#[ignore]
async fn test_pipeline() {
    let client = client();
    client.del(&["p1", "p2"]).await.ok();

    let pipeline = client.pipeline();
    pipeline.set("p1", "v1").await;
    pipeline.set("p2", "v2").await;
    pipeline.get("p1").await;
    let results = pipeline.execute().await.unwrap();

    assert_eq!(results.len(), 3);
    let v1 = client.get("p1").await.unwrap();
    let v2 = client.get("p2").await.unwrap();
    assert_eq!(v1, Some("v1".into()));
    assert_eq!(v2, Some("v2".into()));
}

#[tokio::test]
#[ignore]
async fn test_transaction() {
    let client = client();
    client.del(&["tx1", "tx2"]).await.ok();

    let multi = client.multi();
    multi.cmd(vec!["SET".into(), "tx1".into(), "a".into()]).await;
    multi.cmd(vec!["SET".into(), "tx2".into(), "b".into()]).await;
    multi.cmd(vec!["GET".into(), "tx1".into()]).await;
    let results = multi.exec().await.unwrap();

    assert_eq!(results.len(), 3);
    let v1 = client.get("tx1").await.unwrap();
    let v2 = client.get("tx2").await.unwrap();
    assert_eq!(v1, Some("a".into()));
    assert_eq!(v2, Some("b".into()));
}

#[tokio::test]
#[ignore]
async fn test_lua_script() {
    let client = client();

    let script = Script::new("return redis.call('GET', KEYS[1])");
    client.set("script_key", "script_val").await.unwrap();
    let result = script.run(&client, &["script_key"], &[] as &[&str]).await.unwrap();

    let s = match result {
        rust_redis_rs::parser::Value::BulkString(s) | rust_redis_rs::parser::Value::Status(s) => s,
        _ => panic!("expected string"),
    };
    assert_eq!(s, "script_val");

    client.del(&["script_key"]).await.ok();
}

#[tokio::test]
#[ignore]
async fn test_client_close() {
    let mut option = OpenOptions::new();
    let client = client();
    client.ping().await.unwrap();
    client.close().await.unwrap();
    // Should not panic; subsequent calls would fail with Closed
}
