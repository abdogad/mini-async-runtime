//! End-to-end demo of the runtime: `run_blocking`, `spawn` + `JoinHandle`,
//! `sleep`, and the async `bounded_channel`, all on a single thread.
//!
//! Run with:  cargo run --example demo

use std::time::{Duration, Instant};

use async_runtime::{bounded_channel, run_blocking, sleep, spawn};

fn main() {
    let start = Instant::now();

    let total = run_blocking(async move {
        println!("[{:>6?}] main: starting", start.elapsed());

        // 1. Three workers sleep concurrently and return a value each. The
        //    sleeps overlap, so the slowest (30ms) dominates, not their sum.
        let mut handles = Vec::new();
        for id in 1..=3 {
            let h = spawn(async move {
                let nap = Duration::from_millis(id * 10);
                sleep(nap).await;
                println!("[{:>6?}] worker {id}: woke after {nap:?}", start.elapsed());
                id * 100
            });
            handles.push(h);
        }

        // 2. Producer/consumer over a capacity-2 channel: the producer suspends
        //    once the buffer fills until the consumer drains it.
        let (tx, mut rx) = bounded_channel::<i32>(2);

        let producer = spawn(async move {
            for n in 0..5 {
                sleep(Duration::from_millis(5)).await;
                tx.send(n).await.expect("receiver still alive");
                println!("[{:>6?}] producer: sent {n}", start.elapsed());
            }
        });

        let mut received = Vec::new();
        while let Some(n) = rx.recv().await {
            println!("[{:>6?}] consumer: got {n}", start.elapsed());
            received.push(n);
        }
        assert_eq!(received, vec![0, 1, 2, 3, 4]);
        producer.await;

        // 3. Collect each worker's result via its JoinHandle.
        let mut sum = 0;
        for h in handles {
            sum += h.await;
        }
        println!(
            "[{:>6?}] main: workers returned sum = {sum}",
            start.elapsed()
        );
        sum
    });

    println!("\nrun_blocking returned: {total}");
    println!("total wall-clock time: {:?}", start.elapsed());
    println!(
        "(note: 3 overlapping sleeps of 10/20/30ms finished in ~30ms, not 60ms \
         — proof of concurrency)"
    );
}
