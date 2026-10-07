use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

struct Timer {
    deadline: Instant,
    waker: Waker,
}

// Reverse ordering so the soonest deadline sits on top of the max-heap.
impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        other.deadline.cmp(&self.deadline)
    }
}
impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}
impl Eq for Timer {}

thread_local! {
    static TIMERS: RefCell<BinaryHeap<Timer>> = const { RefCell::new(BinaryHeap::new()) };
}

fn register_timer(deadline: Instant, waker: Waker) {
    TIMERS.with(|t| t.borrow_mut().push(Timer { deadline, waker }));
}

/// Wake every timer that is due, then return the next pending deadline (if any)
/// so the executor knows how long it may park.
pub(crate) fn process_due_timers() -> Option<Instant> {
    TIMERS.with(|t| {
        let mut heap = t.borrow_mut();
        let now = Instant::now();
        while let Some(top) = heap.peek() {
            if top.deadline <= now {
                heap.pop().unwrap().waker.wake();
            } else {
                break;
            }
        }
        heap.peek().map(|t| t.deadline)
    })
}

/// Drop every registered timer, so stale wakers don't carry over into the next
/// `run_blocking` on this thread.
pub(crate) fn clear_timers() {
    // Take the heap out first: dropping a waker can drop its task's future.
    drop(TIMERS.take());
}

/// A future that resolves after `duration` has elapsed, without busy-waiting.
pub struct Sleep {
    deadline: Instant,
    // The waker last handed to the timer heap, if any.
    waker: Option<Waker>,
}

/// Create a future that completes once `duration` has passed.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + duration,
        waker: None,
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if Instant::now() >= this.deadline {
            return Poll::Ready(());
        }
        // Register once, and again only if we're now polled with a different
        // waker (the stale entry just causes one harmless spurious wake).
        if !this.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            register_timer(this.deadline, cx.waker().clone());
            this.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run_blocking, spawn};
    use std::time::Duration;

    #[test]
    fn sleep_suspends_for_about_the_duration() {
        let start = Instant::now();
        run_blocking(async {
            sleep(Duration::from_millis(50)).await;
        });
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn sleeps_run_concurrently() {
        // Two overlapping 50ms sleeps + a 10ms sleep should finish in ~60ms, not
        // ~110ms — proving the sleeps overlap rather than block each other.
        // The bound leaves headroom for slow CI machines.
        let start = Instant::now();
        run_blocking(async {
            spawn(async { sleep(Duration::from_millis(50)).await });
            sleep(Duration::from_millis(50)).await;
            sleep(Duration::from_millis(10)).await;
        });
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn sleep_wakes_the_latest_waker() {
        run_blocking(async {
            let mut nap = sleep(Duration::from_millis(10));
            // First poll with a waker that does nothing...
            let _ = Pin::new(&mut nap).poll(&mut Context::from_waker(Waker::noop()));
            // ...then await it here: this task must still be woken.
            nap.await;
        });
    }
}
