//! A minimal single-threaded async runtime built only on the standard library.
//!
//! ```
//! use std::time::Duration;
//! use mini_async_runtime::{bounded_channel, run_blocking, sleep, spawn};
//!
//! let sum = run_blocking(async {
//!     let (tx, mut rx) = bounded_channel(2);
//!     spawn(async move {
//!         for i in 1..=3 {
//!             sleep(Duration::from_millis(5)).await;
//!             tx.send(i).await.unwrap();
//!         }
//!     });
//!
//!     let mut sum = 0;
//!     while let Some(i) = rx.recv().await {
//!         sum += i;
//!     }
//!     sum
//! });
//! assert_eq!(sum, 6);
//! ```

mod channel;
mod executor;
mod time;

pub use channel::{bounded_channel, Receiver, RecvFuture, SendFuture, Sender};
pub use executor::{run_blocking, spawn, yield_now, JoinHandle};
pub use time::{sleep, Sleep};
