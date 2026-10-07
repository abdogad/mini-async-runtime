use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use crate::time::process_due_timers;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// One unit of work the executor polls. Lives in an `Arc` shared with its
/// `Waker`; the `Mutex` provides `&mut` access to the future through that `Arc`.
struct Task {
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

// Run-queue sender for the running executor, kept per-thread so the free
// function `spawn` can reach it without an explicit handle.
thread_local! {
    static SPAWNER: RefCell<Option<Sender<Arc<Task>>>> = const { RefCell::new(None) };
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
        }
    }
}

fn schedule(future: impl Future<Output = ()> + Send + 'static) {
    SPAWNER.with(|cell| {
        let spawner = cell.borrow();
        let spawner = spawner.as_ref().expect("spawn called outside run_blocking");
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            sender: spawner.clone(),
        });
        let _ = spawner.send(task);
    });
}

/// Spawn `future` onto the running executor. Returns immediately with a
/// [`JoinHandle`] that resolves to the future's output once it completes.
/// Must be called from within `run_blocking`.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + Unpin + 'static,
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

impl<T: Unpin> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // `RecvFuture` is stateless, so a fresh one each poll is fine.
        let mut recv = self.receiver.recv();
        match Pin::new(&mut recv).poll(cx) {
            Poll::Ready(Some(value)) => Poll::Ready(value),
            Poll::Ready(None) => panic!("joined task was dropped before producing a result"),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Run `future` to completion on a fresh single-threaded executor, returning
/// its output.
pub fn run_blocking<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (spawner, ready_queue): (Sender<Arc<Task>>, Receiver<Arc<Task>>) = channel();
    SPAWNER.with(|cell| *cell.borrow_mut() = Some(spawner.clone()));

    // The main future writes its result here; we return once it's set.
    let result: Arc<Mutex<Option<F::Output>>> = Arc::new(Mutex::new(None));
    let result_slot = result.clone();

    let main = Arc::new(Task {
        future: Mutex::new(Some(Box::pin(async move {
            *result_slot.lock().unwrap() = Some(future.await);
        }))),
        sender: spawner.clone(),
    });
    let _ = spawner.send(main);

    loop {
        match ready_queue.try_recv() {
            Ok(task) => poll_task(&task),
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                // Nothing ready: fire due timers, then park until the next
                // deadline (or until a task is queued) instead of busy-waiting.
                match process_due_timers() {
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
            SPAWNER.with(|cell| *cell.borrow_mut() = None);
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
}
