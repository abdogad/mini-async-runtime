use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use crate::time::{clear_timers, process_due_timers};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// One unit of work the executor polls. Lives in an `Arc` shared with its
/// `Waker`; the `Mutex` provides `&mut` access to the future through that `Arc`.
struct Task {
    id: usize,
    future: Mutex<Option<BoxFuture>>,
    // Unbounded, so waking/spawning from the executor thread never blocks
    // waiting for the executor to drain (which would self-deadlock).
    sender: Sender<Arc<Task>>,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Re-queue this task for polling.
        let _ = self.sender.send(self.clone());
    }
}

/// State of the running executor, kept per-thread so the free function
/// `spawn` can reach it without an explicit handle.
struct Runtime {
    sender: Sender<Arc<Task>>,
    // Every unfinished task, so shutdown can drop them all. Without this, a
    // task parked on a channel would leak: the channel holds the task's waker,
    // and the task's future holds the channel.
    tasks: HashMap<usize, Arc<Task>>,
    next_id: usize,
}

thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// Clears the executor's thread-local state when `run_blocking` exits, whether
/// it returns normally or unwinds from a panicking task.
struct ShutdownGuard;

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        let runtime = RUNTIME.with(|cell| cell.borrow_mut().take());
        // Drop futures outside the borrow above: their destructors run user code.
        if let Some(runtime) = runtime {
            for task in runtime.tasks.into_values() {
                drop(task.future.lock().unwrap().take());
            }
        }
        clear_timers();
    }
}

fn poll_task(task: &Arc<Task>) {
    // Take the future out and drop the lock before polling, so a waker firing
    // during `poll` can re-lock the mutex without deadlocking.
    let maybe_future = task.future.lock().unwrap().take();
    if let Some(mut future) = maybe_future {
        let waker = Waker::from(task.clone());
        let mut cx = Context::from_waker(&waker);
        if future.as_mut().poll(&mut cx).is_pending() {
            *task.future.lock().unwrap() = Some(future);
        } else {
            RUNTIME.with(|cell| {
                if let Some(runtime) = cell.borrow_mut().as_mut() {
                    runtime.tasks.remove(&task.id);
                }
            });
        }
    }
}

fn schedule(future: impl Future<Output = ()> + Send + 'static) {
    RUNTIME.with(|cell| {
        let mut runtime = cell.borrow_mut();
        let runtime = runtime.as_mut().expect("spawn called outside run_blocking");
        let task = Arc::new(Task {
            id: runtime.next_id,
            future: Mutex::new(Some(Box::pin(future))),
            sender: runtime.sender.clone(),
        });
        runtime.next_id += 1;
        runtime.tasks.insert(task.id, task.clone());
        let _ = runtime.sender.send(task);
    });
}

/// Spawn `future` onto the running executor. Returns immediately with a
/// [`JoinHandle`] that resolves to the future's output once it completes.
/// Must be called from within `run_blocking`.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // One-shot channel: the task sends its result, the JoinHandle receives it.
    let (tx, rx) = crate::channel::bounded_channel::<F::Output>(1);
    schedule(async move {
        let output = future.await;
        let _ = tx.send(output).await;
    });
    JoinHandle { receiver: rx }
}

/// A handle to a spawned task. Await it to get the task's output.
pub struct JoinHandle<T> {
    receiver: crate::channel::Receiver<T>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // `RecvFuture` is stateless, so a fresh one each poll is fine.
        let mut recv = self.get_mut().receiver.recv();
        match Pin::new(&mut recv).poll(cx) {
            Poll::Ready(Some(value)) => Poll::Ready(value),
            Poll::Ready(None) => panic!("joined task was dropped before producing a result"),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Run `future` to completion on a fresh single-threaded executor, returning
/// its output. Panics if called from inside another `run_blocking`.
pub fn run_blocking<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (spawner, ready_queue): (Sender<Arc<Task>>, Receiver<Arc<Task>>) = channel();
    RUNTIME.with(|cell| {
        let mut runtime = cell.borrow_mut();
        assert!(
            runtime.is_none(),
            "run_blocking cannot be called from within run_blocking"
        );
        *runtime = Some(Runtime {
            sender: spawner,
            tasks: HashMap::new(),
            next_id: 0,
        });
    });
    let _shutdown = ShutdownGuard;

    // The main future writes its result here; we return once it's set.
    let result: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
    let result_slot = result.clone();
    schedule(async move {
        *result_slot.lock().unwrap() = Some(future.await);
    });

    loop {
        // Fire due timers on every turn, not only when idle, so tasks that
        // keep re-queueing themselves can't starve sleeping ones.
        let next_deadline = process_due_timers();
        match ready_queue.try_recv() {
            Ok(task) => poll_task(&task),
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                // Nothing ready: park until the next deadline (or until a task
                // is queued) instead of busy-waiting.
                match next_deadline {
                    Some(deadline) => {
                        let now = Instant::now();
                        if deadline > now {
                            match ready_queue.recv_timeout(deadline - now) {
                                Ok(task) => poll_task(&task),
                                Err(RecvTimeoutError::Timeout) => {}
                                Err(RecvTimeoutError::Disconnected) => break,
                            }
                        }
                    }
                    None => match ready_queue.recv() {
                        Ok(task) => poll_task(&task),
                        Err(_) => break,
                    },
                }
            }
        }
        if let Some(output) = result.lock().unwrap().take() {
            return output;
        }
    }

    panic!("executor stopped before the main future completed");
}

/// Yield control to the executor once, letting other ready tasks run before
/// this one resumes.
pub async fn yield_now() {
    struct YieldNow {
        yielded: bool,
    }
    impl Future for YieldNow {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldNow { yielded: false }.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bounded_channel, sleep};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn returns_a_ready_value() {
        let result = run_blocking(async { 1 + 2 });
        assert_eq!(result, 3);
    }

    #[test]
    fn spawned_tasks_run_to_completion() {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        run_blocking(async {
            for _ in 0..3 {
                spawn(async {
                    yield_now().await;
                    COUNTER.fetch_add(1, Ordering::SeqCst);
                });
            }
            for _ in 0..5 {
                yield_now().await;
            }
        });

        assert_eq!(COUNTER.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn join_handle_returns_spawned_result() {
        let result = run_blocking(async {
            let handle = spawn(async {
                yield_now().await;
                10 + 20
            });
            let other = spawn(async { 5 });
            handle.await + other.await
        });
        assert_eq!(result, 35);
    }

    #[test]
    fn join_handle_accepts_non_unpin_output() {
        run_blocking(async {
            spawn(async { std::marker::PhantomPinned }).await;
        });
    }

    #[test]
    fn timers_fire_while_other_tasks_stay_busy() {
        // The main task never lets the run queue go empty; the sleeper must
        // still wake up, or this loops forever.
        run_blocking(async {
            let done = Arc::new(AtomicBool::new(false));
            let flag = done.clone();
            spawn(async move {
                sleep(Duration::from_millis(10)).await;
                flag.store(true, Ordering::SeqCst);
            });
            while !done.load(Ordering::SeqCst) {
                yield_now().await;
            }
        });
    }

    #[test]
    fn unfinished_tasks_are_dropped_on_return() {
        struct SetOnDrop(Arc<AtomicBool>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let guard = SetOnDrop(dropped.clone());
        run_blocking(async move {
            // This task parks forever on a channel it holds both ends of.
            let (tx, mut rx) = bounded_channel::<()>(1);
            spawn(async move {
                let _guard = guard;
                let _tx = tx;
                rx.recv().await;
            });
            yield_now().await;
        });
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    #[should_panic(expected = "cannot be called from within run_blocking")]
    fn nested_run_blocking_panics() {
        run_blocking(async {
            run_blocking(async {});
        });
    }
}
