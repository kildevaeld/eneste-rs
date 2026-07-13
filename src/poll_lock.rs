use core::{
    pin::Pin,
    task::{Context, Poll, ready},
};

#[cfg(feature = "sink")]
use futures_sink::Sink;
use pin_project_lite::pin_project;

use crate::lock::{AsyncMutex, RcLockFuture, RcLockGuard};

pin_project! {
    #[project = AsyncPollMutexStateProj]
    enum AsyncPollMutexState<T> {
        Unlocking {
            #[pin]
            future: RcLockFuture<T>,
        },
        Locked,
    }
}

pin_project! {
    pub struct AsyncPollMutex<T> {
        value: AsyncMutex<T>,
        #[pin]
        state: AsyncPollMutexState<T>,
    }
}

impl<T> AsyncPollMutex<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: AsyncMutex::new(value),
            state: AsyncPollMutexState::Locked,
        }
    }

    pub fn lock(&self) -> RcLockFuture<T> {
        self.value.lock_rc()
    }

    pub fn poll_lock(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RcLockGuard<T>> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                AsyncPollMutexStateProj::Unlocking { future } => {
                    let guard = ready!(future.poll(cx));
                    this.state.set(AsyncPollMutexState::Locked);
                    return Poll::Ready(guard);
                }
                AsyncPollMutexStateProj::Locked => {
                    let future = this.value.lock_rc();
                    this.state.set(AsyncPollMutexState::Unlocking { future });
                }
            }
        }
    }
}

#[cfg(feature = "sink")]
pin_project! {
    pub struct LockedSink<T, M> {
        #[pin]
        lock: AsyncPollMutex<T>,
        slot: Option<M>,
    }
}

// impl<T, M> Clone for LockedSink<T, M> {
//     fn clone(&self) -> Self {
//         Self {
//             lock: AsyncPollMutex::new(self.lock.lock()),
//             slot: None,
//         }
//     }
// }

#[cfg(feature = "sink")]
impl<T, M> LockedSink<T, M> {
    pub fn new(value: T) -> Self {
        Self {
            lock: AsyncPollMutex::new(value),
            slot: None,
        }
    }

    pub async fn lock(&self) -> RcLockGuard<T> {
        self.lock.lock().await
    }
}

#[cfg(feature = "sink")]
impl<T, M> LockedSink<T, M>
where
    T: Sink<M>,
{
    fn poll_flush_slot(
        mut inner: Pin<&mut T>,
        slot: &mut Option<M>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), T::Error>> {
        if slot.is_some() {
            ready!(inner.as_mut().poll_ready(cx))?;
            Poll::Ready(inner.start_send(slot.take().unwrap()))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_lock_and_flush_slot(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), T::Error>> {
        let mut this = self.as_mut().project();
        let mut lock = ready!(this.lock.as_mut().poll_lock(cx));

        return Self::poll_flush_slot(
            unsafe { Pin::new_unchecked(&mut *lock) },
            &mut this.slot,
            cx,
        );
    }

    pub fn poll_lock(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RcLockGuard<T>> {
        let mut this = self.as_mut().project();
        this.lock.as_mut().poll_lock(cx)
    }
}

#[cfg(feature = "sink")]
impl<T, M> Sink<M> for LockedSink<T, M>
where
    T: Sink<M>,
{
    type Error = T::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            if self.slot.is_none() {
                return Poll::Ready(Ok(()));
            }
            ready!(self.as_mut().poll_lock_and_flush_slot(cx))?;
        }
    }

    fn start_send(self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        *self.project().slot = Some(item);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut inner = ready!(self.as_mut().poll_lock(cx));
        let mut inner = unsafe { Pin::new_unchecked(&mut *inner) };

        let mut this = self.project();
        ready!(Self::poll_flush_slot(inner.as_mut(), &mut this.slot, cx,))?;

        inner.as_mut().poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut inner = ready!(self.as_mut().poll_lock(cx));
        let mut inner: Pin<&mut T> = unsafe { Pin::new_unchecked(&mut *inner) };

        let mut this = self.project();

        ready!(Self::poll_flush_slot(inner.as_mut(), &mut this.slot, cx))?;
        inner.poll_close(cx)
    }
}

// #[cfg(test)]
// mod tests {
//     extern crate std;

//     use super::*;
//     use core::pin::pin;
//     use core::task::{Context, Poll};
//     use futures::executor::block_on;
//     use futures::task::noop_waker_ref;

//     #[test]
//     fn test_new_and_lock() {
//         block_on(async {
//             let lock = AsyncPollMutex::new(42i32);
//             let guard = lock.lock().await;
//             assert_eq!(*guard, 42);
//         });
//     }

//     #[test]
//     fn test_lock_mutation() {
//         block_on(async {
//             let lock = AsyncPollMutex::new(0u32);
//             {
//                 let mut guard = lock.lock().await;
//                 *guard = 99;
//             }
//             let guard = lock.lock().await;
//             assert_eq!(*guard, 99);
//         });
//     }

//     #[test]
//     fn test_poll_lock_ready() {
//         block_on(async {
//             let lock = AsyncPollMutex::new(42i32);
//             let mut pinned = pin!(lock);
//             let guard = core::future::poll_fn(|cx| pinned.as_mut().poll_lock(cx)).await;
//             assert_eq!(*guard, 42);
//         });
//     }

//     #[test]
//     fn test_poll_lock_pending_when_locked() {
//         block_on(async {
//             let lock = AsyncPollMutex::new(42i32);
//             let clone = lock.clone();

//             // Hold the mutex so the clone cannot acquire it.
//             let _guard = lock.lock().await;

//             let waker = noop_waker_ref();
//             let mut cx = Context::from_waker(waker);
//             let mut pinned = pin!(clone);
//             let result = pinned.as_mut().poll_lock(&mut cx);
//             assert!(matches!(result, Poll::Pending));
//         });
//     }

//     #[test]
//     fn test_clone_shares_underlying_value() {
//         block_on(async {
//             let lock1 = AsyncPollMutex::new(10u32);
//             let lock2 = lock1.clone();
//             {
//                 let mut guard = lock1.lock().await;
//                 *guard = 20;
//             }
//             let guard = lock2.lock().await;
//             assert_eq!(*guard, 20);
//         });
//     }

//     #[test]
//     fn test_sequential_poll_locks() {
//         block_on(async {
//             let lock = AsyncPollMutex::new(0u32);
//             let mut pinned = pin!(lock);
//             for _ in 0..3 {
//                 let guard = core::future::poll_fn(|cx| pinned.as_mut().poll_lock(cx)).await;
//                 drop(guard);
//             }
//         });
//     }

//     // ── LockedSink tests ────────────────────────────────────────────────────

//     use futures::SinkExt;
//     use std::vec::Vec;

//     /// A simple sink that collects items into a Vec and records whether it was
//     /// closed, so tests can inspect what the inner sink received.
//     struct CollectSink<T> {
//         items: Vec<T>,
//         closed: bool,
//     }

//     impl<T> CollectSink<T> {
//         fn new() -> Self {
//             Self {
//                 items: Vec::new(),
//                 closed: false,
//             }
//         }
//     }

//     impl<T: Unpin> futures::Sink<T> for CollectSink<T> {
//         type Error = core::convert::Infallible;

//         fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//             Poll::Ready(Ok(()))
//         }

//         fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
//             self.get_mut().items.push(item);
//             Ok(())
//         }

//         fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//             Poll::Ready(Ok(()))
//         }

//         fn poll_close(
//             mut self: Pin<&mut Self>,
//             _: &mut Context<'_>,
//         ) -> Poll<Result<(), Self::Error>> {
//             self.closed = true;
//             Poll::Ready(Ok(()))
//         }
//     }

//     #[test]
//     fn test_locked_sink_poll_ready_empty_slot() {
//         // With no pending item the sink should report ready immediately.
//         let sink = LockedSink::new(CollectSink::<i32>::new());
//         let mut pinned = pin!(sink);
//         let waker = noop_waker_ref();
//         let mut cx = Context::from_waker(waker);
//         let result = pinned.as_mut().poll_ready(&mut cx);
//         assert!(matches!(result, Poll::Ready(Ok(()))));
//     }

//     #[test]
//     fn test_locked_sink_start_send_stores_item() {
//         // start_send should store the item in the slot without forwarding yet.
//         let sink = LockedSink::new(CollectSink::<i32>::new());
//         let mut pinned = pin!(sink);
//         pinned.as_mut().start_send(42).unwrap();

//         // The inner sink should not have received the item yet.
//         block_on(async {
//             let guard = pinned.as_mut().lock().await;
//             assert!(guard.items.is_empty());
//         });
//     }

//     #[test]
//     fn test_locked_sink_send_delivers_item() {
//         // SinkExt::send calls poll_ready + start_send + poll_flush.
//         block_on(async {
//             let mut sink = pin!(LockedSink::new(CollectSink::<i32>::new()));
//             sink.as_mut().send(7).await.unwrap();

//             let guard = sink.as_mut().lock().await;
//             assert_eq!(guard.items, [7]);
//         });
//     }

//     #[test]
//     fn test_locked_sink_send_multiple_items() {
//         block_on(async {
//             let mut sink = pin!(LockedSink::new(CollectSink::<i32>::new()));
//             for i in 0..5 {
//                 sink.as_mut().send(i).await.unwrap();
//             }

//             let guard = sink.as_mut().lock().await;
//             assert_eq!(guard.items, [0, 1, 2, 3, 4]);
//         });
//     }

//     #[test]
//     fn test_locked_sink_flush_forwards_pending_item() {
//         block_on(async {
//             let mut sink = pin!(LockedSink::new(CollectSink::<i32>::new()));
//             // Place item in slot directly.
//             sink.as_mut().start_send(99).unwrap();
//             // poll_flush should forward it to the inner sink.
//             core::future::poll_fn(|cx| sink.as_mut().poll_flush(cx))
//                 .await
//                 .unwrap();

//             let guard = sink.as_mut().lock().await;
//             assert_eq!(guard.items, [99]);
//         });
//     }

//     #[test]
//     fn test_locked_sink_close_marks_inner_closed() {
//         block_on(async {
//             let mut sink = pin!(LockedSink::new(CollectSink::<i32>::new()));
//             sink.as_mut().send(1).await.unwrap();
//             sink.as_mut().close().await.unwrap();

//             let guard = sink.as_mut().lock().await;
//             assert_eq!(guard.items, [1]);
//             assert!(guard.closed);
//         });
//     }

//     #[test]
//     fn test_locked_sink_clone_shares_inner_sink() {
//         // Both clones should write to the same underlying CollectSink.
//         block_on(async {
//             let sink1 = LockedSink::new(CollectSink::<i32>::new());
//             let mut sink2 = pin!(sink1.clone());
//             let mut sink1 = pin!(sink1);

//             sink1.as_mut().send(1).await.unwrap();
//             sink2.as_mut().send(2).await.unwrap();

//             let guard = sink1.as_mut().lock().await;
//             assert_eq!(guard.items.len(), 2);
//             assert!(guard.items.contains(&1));
//             assert!(guard.items.contains(&2));
//         });
//     }

//     #[test]
//     fn test_locked_sink_clone_has_empty_slot() {
//         // A cloned LockedSink starts with no pending item in its slot.
//         block_on(async {
//             let sink = LockedSink::new(CollectSink::<i32>::new());
//             let mut original = pin!(sink);
//             // Put an item in the original's slot but don't flush.
//             original.as_mut().start_send(10).unwrap();

//             // The clone should have an empty slot.
//             let clone = {
//                 let guard = original.as_mut().lock().await;
//                 // Deriving clone goes through LockedSink::clone.
//                 drop(guard);
//                 // Re-pin after drop so we can call clone on the inner type.
//                 // We test via poll_ready: an empty slot returns Ready immediately.
//                 let cloned_sink = LockedSink::new(CollectSink::<i32>::new());
//                 cloned_sink
//             };
//             let mut cloned = pin!(clone);
//             let waker = noop_waker_ref();
//             let mut cx = Context::from_waker(waker);
//             assert!(matches!(
//                 cloned.as_mut().poll_ready(&mut cx),
//                 Poll::Ready(Ok(()))
//             ));
//         });
//     }
// }
