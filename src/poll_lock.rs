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

#[cfg(test)]
mod behavior_tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_with};
    use core::pin::pin;

    fn poll_lock<T>(
        lock: Pin<&mut AsyncPollMutex<T>>,
        waker: &core::task::Waker,
    ) -> Poll<RcLockGuard<T>> {
        let mut cx = Context::from_waker(waker);
        lock.poll_lock(&mut cx)
    }

    #[test]
    fn lock_returns_the_initial_value() {
        let mutex = AsyncPollMutex::new(42);
        assert_eq!(*pollster::block_on(mutex.lock()), 42);
    }

    #[test]
    fn lock_guard_mutations_persist() {
        let mutex = AsyncPollMutex::new(0);
        *pollster::block_on(mutex.lock()) = 99;
        assert_eq!(*pollster::block_on(mutex.lock()), 99);
    }

    #[test]
    fn poll_lock_is_ready_when_uncontended() {
        let (waker, _) = counting_waker();
        let mutex = pin!(AsyncPollMutex::new(5));

        let mut mutex = mutex;
        let Poll::Ready(guard) = poll_lock(mutex.as_mut(), &waker) else {
            panic!("uncontended poll_lock should be ready");
        };
        assert_eq!(*guard, 5);
    }

    #[test]
    fn poll_lock_is_pending_while_the_lock_is_held() {
        let (waker, _) = counting_waker();
        let mut mutex = pin!(AsyncPollMutex::new(5));
        let held = pollster::block_on(mutex.lock());

        assert!(poll_lock(mutex.as_mut(), &waker).is_pending());

        drop(held);
    }

    #[test]
    fn poll_lock_wakes_and_completes_after_release() {
        let (waker, wakes) = counting_waker();
        let mut mutex = pin!(AsyncPollMutex::new(5));
        let held = pollster::block_on(mutex.lock());

        assert!(poll_lock(mutex.as_mut(), &waker).is_pending());
        assert_eq!(wakes.count(), 0);

        drop(held);
        assert!(wakes.count() >= 1);

        let Poll::Ready(guard) = poll_lock(mutex.as_mut(), &waker) else {
            panic!("poll_lock should complete after the lock was released");
        };
        assert_eq!(*guard, 5);
    }

    #[test]
    fn poll_lock_can_be_used_repeatedly() {
        let (waker, _) = counting_waker();
        let mut mutex = pin!(AsyncPollMutex::new(0));

        for i in 1..=3 {
            let Poll::Ready(mut guard) = poll_lock(mutex.as_mut(), &waker) else {
                panic!("poll_lock should be ready between uses");
            };
            *guard = i;
        }

        let Poll::Ready(guard) = poll_lock(mutex.as_mut(), &waker) else {
            panic!("poll_lock should be ready");
        };
        assert_eq!(*guard, 3);
    }

    #[test]
    fn poll_lock_guard_excludes_other_lockers() {
        let (waker, _) = counting_waker();
        let mut mutex = pin!(AsyncPollMutex::new(0));

        let Poll::Ready(guard) = poll_lock(mutex.as_mut(), &waker) else {
            panic!("poll_lock should be ready");
        };

        let mut other = pin!(mutex.lock());
        assert!(poll_with(other.as_mut(), &waker).is_pending());

        drop(guard);
        assert!(poll_with(other.as_mut(), &waker).is_ready());
    }
}

#[cfg(all(test, feature = "sink"))]
mod sink_tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_with};
    use alloc::vec::Vec;
    use core::{convert::Infallible, pin::pin, task::Waker};

    #[derive(Default)]
    struct VecSink {
        items: Vec<i32>,
        blocked: bool,
        flushes: usize,
        closes: usize,
    }

    impl Sink<i32> for VecSink {
        type Error = Infallible;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.blocked {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn start_send(mut self: Pin<&mut Self>, item: i32) -> Result<(), Self::Error> {
            self.items.push(item);
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.closes += 1;
            Poll::Ready(Ok(()))
        }
    }

    fn ready_ok<T>(poll: Poll<Result<T, Infallible>>) -> T {
        match poll {
            Poll::Ready(Ok(value)) => value,
            Poll::Ready(Err(never)) => match never {},
            Poll::Pending => panic!("expected Ready, got Pending"),
        }
    }

    fn poll_ready_with(
        sink: Pin<&mut LockedSink<VecSink, i32>>,
        waker: &Waker,
    ) -> Poll<Result<(), Infallible>> {
        sink.poll_ready(&mut Context::from_waker(waker))
    }

    fn poll_flush_with(
        sink: Pin<&mut LockedSink<VecSink, i32>>,
        waker: &Waker,
    ) -> Poll<Result<(), Infallible>> {
        sink.poll_flush(&mut Context::from_waker(waker))
    }

    fn poll_close_with(
        sink: Pin<&mut LockedSink<VecSink, i32>>,
        waker: &Waker,
    ) -> Poll<Result<(), Infallible>> {
        sink.poll_close(&mut Context::from_waker(waker))
    }

    #[test]
    fn lock_exposes_the_inner_sink() {
        let sink = LockedSink::<VecSink, i32>::new(VecSink::default());
        assert!(pollster::block_on(sink.lock()).items.is_empty());
    }

    #[test]
    fn poll_ready_is_ready_when_no_item_is_buffered() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        ready_ok(poll_ready_with(sink.as_mut(), &waker));
        assert!(pollster::block_on(sink.lock()).items.is_empty());
    }

    #[test]
    fn start_send_buffers_the_item_without_touching_the_inner_sink() {
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        sink.as_mut().start_send(1).unwrap();

        assert!(pollster::block_on(sink.lock()).items.is_empty());
    }

    #[test]
    fn poll_ready_flushes_the_buffered_item_into_the_inner_sink() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        sink.as_mut().start_send(1).unwrap();
        ready_ok(poll_ready_with(sink.as_mut(), &waker));

        assert_eq!(pollster::block_on(sink.lock()).items, [1]);
    }

    #[test]
    fn items_are_forwarded_in_order() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        for item in [1, 2, 3] {
            ready_ok(poll_ready_with(sink.as_mut(), &waker));
            sink.as_mut().start_send(item).unwrap();
        }
        ready_ok(poll_flush_with(sink.as_mut(), &waker));

        assert_eq!(pollster::block_on(sink.lock()).items, [1, 2, 3]);
    }

    #[test]
    fn poll_ready_is_pending_while_the_inner_sink_is_not_ready() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink {
            blocked: true,
            ..VecSink::default()
        }));

        sink.as_mut().start_send(1).unwrap();
        assert!(poll_ready_with(sink.as_mut(), &waker).is_pending());
        assert!(pollster::block_on(sink.lock()).items.is_empty());

        pollster::block_on(sink.lock()).blocked = false;
        ready_ok(poll_ready_with(sink.as_mut(), &waker));
        assert_eq!(pollster::block_on(sink.lock()).items, [1]);
    }

    #[test]
    fn poll_flush_forwards_buffered_item_and_flushes_inner() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        sink.as_mut().start_send(7).unwrap();
        ready_ok(poll_flush_with(sink.as_mut(), &waker));

        let inner = pollster::block_on(sink.lock());
        assert_eq!(inner.items, [7]);
        assert_eq!(inner.flushes, 1);
    }

    #[test]
    fn poll_flush_without_buffered_item_still_flushes_inner() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        ready_ok(poll_flush_with(sink.as_mut(), &waker));

        let inner = pollster::block_on(sink.lock());
        assert!(inner.items.is_empty());
        assert_eq!(inner.flushes, 1);
    }

    #[test]
    fn poll_close_forwards_buffered_item_and_closes_inner() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));

        sink.as_mut().start_send(9).unwrap();
        ready_ok(poll_close_with(sink.as_mut(), &waker));

        let inner = pollster::block_on(sink.lock());
        assert_eq!(inner.items, [9]);
        assert_eq!(inner.closes, 1);
    }

    #[test]
    fn poll_flush_is_pending_while_the_inner_lock_is_held_elsewhere() {
        let (waker, wakes) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));
        let held = pollster::block_on(sink.lock());

        assert!(poll_flush_with(sink.as_mut(), &waker).is_pending());
        assert_eq!(wakes.count(), 0);

        drop(held);
        assert!(wakes.count() >= 1);
        ready_ok(poll_flush_with(sink.as_mut(), &waker));
    }

    #[test]
    fn poll_lock_on_the_sink_waits_for_the_inner_lock() {
        let (waker, _) = counting_waker();
        let mut sink = pin!(LockedSink::<VecSink, i32>::new(VecSink::default()));
        let held = pollster::block_on(sink.lock());

        let mut cx = Context::from_waker(&waker);
        assert!(sink.as_mut().poll_lock(&mut cx).is_pending());

        drop(held);
        assert!(sink.as_mut().poll_lock(&mut cx).is_ready());
    }

    #[test]
    fn sink_lock_future_is_independent_of_poll_state() {
        let (waker, _) = counting_waker();
        let sink = LockedSink::<VecSink, i32>::new(VecSink::default());
        let mut lock = pin!(sink.lock());

        assert!(poll_with(lock.as_mut(), &waker).is_ready());
    }
}
