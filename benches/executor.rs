//! Benchmarks for `eneste::executor::Executor`, with `futures::executor::LocalPool`
//! as the baseline. Every scenario runs the same workload on both executors.
//!
//! Run with `cargo bench --features executor --bench executor`.

use std::{
    future::Future,
    hint::black_box,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use eneste::{
    executor::{EventLoopWaker, Executor},
    spawner::LocalSpawner,
};
use futures::{channel::oneshot, executor::LocalPool, task::LocalSpawnExt};
use goerdet::Task;

struct NoopLoopWaker;

impl EventLoopWaker for NoopLoopWaker {
    fn wake(&self) {}
}

fn new_executor() -> Executor<'static, NoopLoopWaker> {
    Executor::new(Arc::new(NoopLoopWaker))
}

fn run_to_completion(executor: &Executor<'_, NoopLoopWaker>) {
    while executor.has_tasks() {
        executor.process_tasks(usize::MAX);
    }
}

/// Returns `Pending` once and wakes itself, forcing a trip through the scheduler.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn yield_now() -> YieldNow {
    YieldNow(false)
}

async fn yield_times(count: usize) {
    for _ in 0..count {
        yield_now().await;
    }
}

/// Spawn `n` tasks that complete on their first poll.
fn spawn_ready(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn_ready");
    for n in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eneste", n), &n, |b, &n| {
            b.iter(|| {
                let executor = new_executor();
                for i in 0..n {
                    executor
                        .spawn(async move {
                            black_box(i);
                        })
                        .detach();
                }
                run_to_completion(&executor);
            })
        });

        group.bench_with_input(BenchmarkId::new("futures_local_pool", n), &n, |b, &n| {
            b.iter(|| {
                let mut pool = LocalPool::new();
                let spawner = pool.spawner();
                for i in 0..n {
                    spawner
                        .spawn_local(async move {
                            black_box(i);
                        })
                        .unwrap();
                }
                pool.run();
            })
        });
    }
    group.finish();
}

/// `tasks` tasks that each yield `yields` times, interleaving on the run queue.
fn yield_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("yield");
    for (tasks, yields) in [(1usize, 1_000usize), (100, 100), (1_000, 10)] {
        let label = format!("{tasks}x{yields}");
        group.throughput(Throughput::Elements((tasks * yields) as u64));

        group.bench_function(BenchmarkId::new("eneste", &label), |b| {
            b.iter(|| {
                let executor = new_executor();
                for _ in 0..tasks {
                    executor.spawn(yield_times(yields)).detach();
                }
                run_to_completion(&executor);
            })
        });

        group.bench_function(BenchmarkId::new("futures_local_pool", &label), |b| {
            b.iter(|| {
                let mut pool = LocalPool::new();
                let spawner = pool.spawner();
                for _ in 0..tasks {
                    spawner.spawn_local(yield_times(yields)).unwrap();
                }
                pool.run();
            })
        });
    }
    group.finish();
}

/// A chain of `n` tasks where each waits on a oneshot from the previous one,
/// so every hop is a wake from one task to another.
fn oneshot_chain(c: &mut Criterion) {
    fn build_chain(
        n: usize,
    ) -> (
        oneshot::Sender<usize>,
        Vec<impl Future<Output = ()>>,
        oneshot::Receiver<usize>,
    ) {
        let (first_tx, mut rx) = oneshot::channel::<usize>();
        let mut links = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, next_rx) = oneshot::channel::<usize>();
            let prev = rx;
            links.push(async move {
                let value = prev.await.unwrap();
                let _ = tx.send(value + 1);
            });
            rx = next_rx;
        }
        (first_tx, links, rx)
    }

    let mut group = c.benchmark_group("oneshot_chain");
    for n in [100usize, 1_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eneste", n), &n, |b, &n| {
            b.iter(|| {
                let executor = new_executor();
                let (first_tx, links, mut last_rx) = build_chain(n);
                for link in links {
                    executor.spawn(link).detach();
                }
                run_to_completion(&executor);
                first_tx.send(0).unwrap();
                run_to_completion(&executor);
                assert_eq!(last_rx.try_recv().unwrap(), Some(n));
            })
        });

        group.bench_with_input(BenchmarkId::new("futures_local_pool", n), &n, |b, &n| {
            b.iter(|| {
                let mut pool = LocalPool::new();
                let spawner = pool.spawner();
                let (first_tx, links, mut last_rx) = build_chain(n);
                for link in links {
                    spawner.spawn_local(link).unwrap();
                }
                pool.run_until_stalled();
                first_tx.send(0).unwrap();
                pool.run();
                assert_eq!(last_rx.try_recv().unwrap(), Some(n));
            })
        });
    }
    group.finish();
}

/// One task that spawns `n` children from inside the executor.
fn spawn_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn_nested");
    for n in [100usize, 1_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("eneste", n), &n, |b, &n| {
            b.iter(|| {
                let executor = new_executor();
                let spawner = executor.clone();
                executor
                    .spawn(async move {
                        for i in 0..n {
                            spawner
                                .spawn(async move {
                                    black_box(i);
                                })
                                .detach();
                        }
                    })
                    .detach();
                run_to_completion(&executor);
            })
        });

        group.bench_with_input(BenchmarkId::new("futures_local_pool", n), &n, |b, &n| {
            b.iter(|| {
                let mut pool = LocalPool::new();
                let spawner = pool.spawner();
                let inner = spawner.clone();
                spawner
                    .spawn_local(async move {
                        for i in 0..n {
                            inner
                                .spawn_local(async move {
                                    black_box(i);
                                })
                                .unwrap();
                        }
                    })
                    .unwrap();
                pool.run();
            })
        });
    }
    group.finish();
}

/// Drive a single future that yields `n` times to completion and read its output.
fn block_on(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_on");
    for n in [0usize, 1_000] {
        group.throughput(Throughput::Elements(n.max(1) as u64));

        group.bench_with_input(BenchmarkId::new("eneste", n), &n, |b, &n| {
            b.iter(|| {
                let executor = new_executor();
                executor.block_on(async move {
                    yield_times(n).await;
                    black_box(n)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("futures_local_pool", n), &n, |b, &n| {
            b.iter(|| {
                let mut pool = LocalPool::new();
                pool.run_until(async move {
                    yield_times(n).await;
                    black_box(n)
                })
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    spawn_ready,
    yield_many,
    oneshot_chain,
    spawn_nested,
    block_on
);
criterion_main!(benches);
