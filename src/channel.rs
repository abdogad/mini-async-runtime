use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// State shared between every `Sender` and the single `Receiver`.
struct Shared<T> {
    queue: VecDeque<T>,
    capacity: usize,
    recv_waker: Option<Waker>,
    send_wakers: VecDeque<Waker>,
    senders: usize,
    receiver_alive: bool,
}

/// Create a bounded async channel with room for `capacity` buffered values.
pub fn bounded_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity >= 1, "capacity must be at least 1");
    let shared = Arc::new(Mutex::new(Shared {
        queue: VecDeque::new(),
        capacity,
        recv_waker: None,
        send_wakers: VecDeque::new(),
        senders: 1,
        receiver_alive: true,
    }));
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

pub struct Sender<T> {
    shared: Arc<Mutex<Shared<T>>>,
}

impl<T> Sender<T> {
    /// Send `value`, suspending while the channel is full. Resolves to `Ok(())`
    /// once buffered, or `Err(value)` if the receiver is gone.
    pub fn send(&self, value: T) -> SendFuture<T> {
        SendFuture {
            shared: self.shared.clone(),
            value: Some(value),
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.lock().unwrap().senders += 1;
        Sender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.senders -= 1;
        if shared.senders == 0 {
            // Last sender gone: wake the receiver so it observes closure.
            let waker = shared.recv_waker.take();
            drop(shared);
            if let Some(w) = waker {
                w.wake();
            }
        }
    }
}

pub struct SendFuture<T> {
    shared: Arc<Mutex<Shared<T>>>,
    value: Option<T>,
}

impl<T: Unpin> Future for SendFuture<T> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut shared = this.shared.lock().unwrap();

        if !shared.receiver_alive {
            return Poll::Ready(Err(this.value.take().unwrap()));
        }

        if shared.queue.len() < shared.capacity {
            shared.queue.push_back(this.value.take().unwrap());
            // Release the lock before waking, to keep the critical section small.
            let waker = shared.recv_waker.take();
            drop(shared);
            if let Some(w) = waker {
                w.wake();
            }
            Poll::Ready(Ok(()))
        } else {
            shared.send_wakers.push_back(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub struct Receiver<T> {
    shared: Arc<Mutex<Shared<T>>>,
}

impl<T> Receiver<T> {
    /// Receive the next value, suspending while the channel is empty. Resolves
    /// to `None` once all senders are gone and the buffer is drained.
    pub fn recv(&self) -> RecvFuture<T> {
        RecvFuture {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.receiver_alive = false;
        let wakers: Vec<Waker> = shared.send_wakers.drain(..).collect();
        drop(shared);
        for w in wakers {
            w.wake();
        }
    }
}

pub struct RecvFuture<T> {
    shared: Arc<Mutex<Shared<T>>>,
}

impl<T> Future for RecvFuture<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut shared = this.shared.lock().unwrap();

        if let Some(v) = shared.queue.pop_front() {
            // Freed a slot: wake one parked sender before returning the value.
            let waker = shared.send_wakers.pop_front();
            drop(shared);
            if let Some(w) = waker {
                w.wake();
            }
            Poll::Ready(Some(v))
        } else if shared.senders == 0 {
            Poll::Ready(None)
        } else {
            shared.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run_blocking, spawn};

    #[test]
    fn send_then_receive_in_order() {
        let result = run_blocking(async {
            let (tx, rx) = bounded_channel::<i32>(4);
            spawn(async move {
                for i in 0..5 {
                    tx.send(i).await.unwrap();
                }
            });

            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            got
        });
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn backpressure_suspends_sender() {
        // capacity 1, send 3 values: the sender must suspend until the receiver
        // drains, exercising the send-parking path.
        let result = run_blocking(async {
            let (tx, rx) = bounded_channel::<i32>(1);
            spawn(async move {
                for i in 0..3 {
                    tx.send(i).await.unwrap();
                }
            });
            let mut sum = 0;
            while let Some(v) = rx.recv().await {
                sum += v;
            }
            sum
        });
        assert_eq!(result, 3); // 0 + 1 + 2
    }
}
