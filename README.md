# Async Runtime

A single-threaded asynchronous runtime implemented from scratch in Rust, using
**only the standard library** (no external crates, no spawned threads).

```bash
cargo test                 # run the test suite (7 tests)
cargo run --example demo   # run the end-to-end demo
```

## Implemented interface

| Requirement | API | Location |
|-------------|-----|----------|
| Run a future to completion, returning its result | `run_blocking(future) -> T` | `src/executor.rs` |
| Spawn a future to run concurrently; returns immediately | `spawn(future) -> JoinHandle<T>` | `src/executor.rs` |
| Async sleep that suspends without blocking the thread | `sleep(duration).await` | `src/time.rs` |
| Async bounded MPSC channel | `bounded_channel(capacity) -> (Sender, Receiver)` | `src/channel.rs` |

`spawn` additionally returns a `JoinHandle` that can be `.await`-ed to obtain the
spawned task's result.

## How the required features are satisfied

- **Polling.** `run_blocking` drives a run queue of tasks, polling each only when
  it is ready. A `Future` returning `Poll::Pending` registers its `Waker`; the
  thing it waits on (timer or channel) later calls `wake()`, which re-queues the
  task for the next poll.

- **Spawning.** `spawn` wraps a future in a task and pushes it onto the same run
  queue, so it is scheduled concurrently without waiting for completion.

- **Sleep without polling.** A sleeping future is **not** re-polled until its
  deadline. It registers `(deadline, waker)` in a timer min-heap and suspends.
  The executor wakes it only once the deadline passes. Other tasks run freely in
  the meantime (see the demo: three sleeps of 10/20/30 ms finish in ~30 ms).

- **Async channel without polling.** A receiver waiting on an empty channel
  parks its waker and is not woken until a sender delivers a value; a sender on a
  full channel parks until the receiver frees a slot.

- **No wasted CPU when idle.** When the run queue is empty, the executor parks
  the thread (`recv` / `recv_timeout`) until either a task is queued or the
  nearest timer deadline is reached — it never busy-loops.

- **Single thread, no threads spawned.** All concurrency runs on the one thread
  that called `run_blocking`. No `std::thread::spawn` is used anywhere.

- **Fairness.** The run queue is FIFO and timers use a min-heap, so the earliest
  deadline always fires first.

## Project layout

```
src/lib.rs        public API (re-exports)
src/executor.rs   run queue, wakers, run_blocking, spawn, JoinHandle, yield_now
src/time.rs       timer min-heap + sleep
src/channel.rs    async bounded MPSC channel
examples/demo.rs  runnable demonstration of all features together
```

## Design decisions & known limitations

These are deliberate tradeoffs for a minimal single-threaded runtime:

- `run_blocking` returns as soon as the **main** future completes; spawned tasks
  that were not awaited are cancelled at that point (same semantics as a
  current-thread `block_on`). Await a task's `JoinHandle` if its completion
  matters.
- Futures must be `Send + 'static` because the runtime is built on the standard
  `Wake` trait (which requires `Send + Sync`). Avoiding this would require a
  hand-written `RawWaker` with `unsafe`.
- `JoinHandle<T>` requires `T: Unpin`, as it is layered on the channel.
- A panicking task propagates out of `run_blocking` rather than being isolated.

## Tests

`cargo test` covers each feature: returning a value, concurrent spawning,
`JoinHandle` results, sleeping for a duration, sleeps running concurrently,
in-order channel delivery with clean closure, and sender backpressure on a full
channel.
