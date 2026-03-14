//! Redis client performance benchmarks.
//!
//! Requires Redis server running at 127.0.0.1:6379.
//! Run with: cargo bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rust_redis_rs::{Client, ClientOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn client() -> Client {
    Client::new(ClientOptions::default())
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = rt();

    // PING - minimal round-trip latency
    let mut group = c.benchmark_group("ping");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single", |b| {
        let client = client();
        b.to_async(&rt).iter(|| client.ping());
    });
    group.finish();

    // SET throughput - single key-value
    let mut group = c.benchmark_group("set");
    group.throughput(Throughput::Elements(1));
    let set_counter = AtomicU64::new(0);
    group.bench_function("single", |b| {
        let client = client();
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            let key = format!("bench:set:{}", set_counter.fetch_add(1, Ordering::Relaxed));
            async move { client.set(&key, "value").await }
        });
    });
    group.finish();

    // GET throughput - single key
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));
    rt.block_on(async {
        let client = client();
        client.set("bench:get:key", "value").await.unwrap();
    });
    group.bench_function("single", |b| {
        let client = client();
        b.to_async(&rt).iter(|| client.get("bench:get:key"));
    });
    group.finish();

    // Pipeline: batch of N SETs
    for batch_size in [10, 50, 100, 200, 500] {
        let mut group = c.benchmark_group("pipeline_set");
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &n| {
                let client = client();
                let counter = AtomicU64::new(0);
                b.to_async(&rt).iter(|| {
                    let pipeline = client.pipeline();
                    let base = counter.fetch_add(n, Ordering::Relaxed);
                    async move {
                        for i in 0..n {
                            pipeline
                                .set(&format!("bench:pipe:{}", base + i), "value")
                                .await;
                        }
                        pipeline.execute().await
                    }
                });
            },
        );
        group.finish();
    }

    // Pipeline: batch of N GETs (pre-populate keys first)
    rt.block_on(async {
        let client = client();
        for i in 0..1000u64 {
            let key = format!("bench:pipe_get:{}", i);
            client.set(&key, "v").await.unwrap();
        }
    });
    for batch_size in [10, 50, 100, 200] {
        let mut group = c.benchmark_group("pipeline_get");
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &n| {
                let client = client();
                let counter = AtomicU64::new(0);
                b.to_async(&rt).iter(|| {
                    let pipeline = client.pipeline();
                    let base = counter.fetch_add(n, Ordering::Relaxed);
                    async move {
                        for i in 0..n {
                            let key = format!("bench:pipe_get:{}", (base + i) % 1000);
                            pipeline.get(&key).await;
                        }
                        pipeline.execute().await
                    }
                });
            },
        );
        group.finish();
    }

    // Concurrent SET - measure pool throughput
    let mut group = c.benchmark_group("concurrent_set");
    group.sample_size(50);
    group.bench_function("10_concurrent", |b| {
        let client = client();
        b.to_async(&rt).iter(|| {
            let mut handles = Vec::new();
            for i in 0..10u64 {
                let c = client.clone();
                let key = format!("bench:conc:{}", i);
                handles.push(tokio::spawn(async move { c.set(&key, "v").await }));
            }
            async move {
                for h in handles {
                    let _ = h.await;
                }
            }
        });
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
