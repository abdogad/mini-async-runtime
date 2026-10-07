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

/// A future that resolves after `duration` has elapsed, without busy-waiting.
pub struct Sleep {
    deadline: Instant,
    registered: bool,
}

/// Create a future that completes once `duration` has passed.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + duration,
        registered: false,
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if Instant::now() >= this.deadline {
            Poll::Ready(())
        } else if !this.registered {
            // Register the timer once; the executor wakes us at the deadline.
            register_timer(this.deadline, cx.waker().clone());
            this.registered = true;
            Poll::Pending
        } else {
            Poll::Pending
        }
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
        let start = Instant::now();
        run_blocking(async {
            spawn(async { sleep(Duration::from_millis(50)).await });
            sleep(Duration::from_millis(50)).await;
            sleep(Duration::from_millis(10)).await;
        });
        assert!(start.elapsed() < Duration::from_millis(70));
    }
}
