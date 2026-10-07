use alloc::rc::Rc;
use core::{
    cell::{Cell, UnsafeCell},
    pin::Pin,
    task::{Context, Poll},
};
use pin_project_lite::pin_project;

use crate::event::Event;

#[derive(Debug)]
struct AsyncMutexInner<T> {
    value: UnsafeCell<T>,
    event: Event,
    locked: Cell<bool>,
}

impl<T> AsyncMutexInner<T> {
    fn is_locked(&self) -> bool {
        self.locked.get()
    }

    fn lock(&self) {
        self.locked.set(true);
        self.event.notify(1);
    }

    fn unlock(&self) {
        self.locked.set(false);
        self.event.notify(1);
    }
}

pub struct AsyncMutex<T>(Rc<AsyncMutexInner<T>>);

impl<T> AsyncMutex<T> {
    pub fn new(value: T) -> Self {
        Self(Rc::new(AsyncMutexInner {
            value: UnsafeCell::new(value),
            event: Event::new(),
            locked: Cell::new(false),
        }))
    }

    pub fn lock(&self) -> LockFuture<'_, T> {
        LockFuture {
            lock: &*self.0,
            state: LockState::Idle,
        }
    }

    pub fn lock_rc(&self) -> RcLockFuture<T> {
        RcLockFuture {
            lock: Rc::clone(&self.0),
            state: LockState::Idle,
        }
    }
}

pub struct LockGuard<'a, T> {
    value: &'a AsyncMutexInner<T>,
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        self.value.unlock();
    }
}

impl<'a, T> core::ops::Deref for LockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value.value.get() }
    }
}

impl<'a, T> core::ops::DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.value.value.get() }
    }
}

pin_project! {
    #[project = LockStateProj]
    enum LockState {
        Waiting {
            #[pin]
            listener: crate::event::EventListener,
        },
        Idle,
    }
}

pin_project! {
    pub struct LockFuture<'a, T> {
        #[pin]
        lock: &'a AsyncMutexInner<T>,
        #[pin]
        state: LockState
    }
}

impl<'a, T> Future for LockFuture<'a, T> {
    type Output = LockGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();
            match this.state.as_mut().project() {
                LockStateProj::Waiting { listener } => match listener.poll(cx) {
                    Poll::Ready(_) => {
                        *this.state = LockState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LockStateProj::Idle => {
                    if !this.lock.is_locked() {
                        this.lock.lock();
                        return Poll::Ready(LockGuard { value: &this.lock });
                    } else {
                        this.state.set(LockState::Waiting {
                            listener: this.lock.event.listen(),
                        });
                    }
                }
            }
        }
    }
}

pub struct RcLockGuard<T> {
    lock: Rc<AsyncMutexInner<T>>,
}

impl<T> core::ops::Deref for RcLockGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> core::ops::DerefMut for RcLockGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for RcLockGuard<T> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

pin_project! {
    pub struct RcLockFuture< T> {
        #[pin]
        lock: Rc<AsyncMutexInner<T>>,
        #[pin]
        state: LockState
    }
}

impl<T> Future for RcLockFuture<T> {
    type Output = RcLockGuard<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();
            match this.state.as_mut().project() {
                LockStateProj::Waiting { listener } => match listener.poll(cx) {
                    Poll::Ready(_) => {
                        *this.state = LockState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LockStateProj::Idle => {
                    if !this.lock.is_locked() {
                        this.lock.lock();
                        return Poll::Ready(RcLockGuard {
                            lock: Rc::clone(&this.lock),
                        });
                    } else {
                        this.state.set(LockState::Waiting {
                            listener: this.lock.event.listen(),
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_once, poll_with};
    use core::pin::pin;

    #[test]
    fn lock_succeeds_without_contention() {
        let mutex = AsyncMutex::new(5);
        let mut fut = pin!(mutex.lock());

        let Poll::Ready(guard) = poll_once(fut.as_mut()) else {
            panic!("uncontended lock should be ready immediately");
        };
        assert_eq!(*guard, 5);
    }

    #[test]
    fn guard_allows_mutation_that_persists() {
        let mutex = AsyncMutex::new(1);
        {
            let mut guard = pollster::block_on(mutex.lock());
            *guard += 41;
        }
        assert_eq!(*pollster::block_on(mutex.lock()), 42);
    }

    #[test]
    fn lock_is_pending_while_guard_is_alive() {
        let mutex = AsyncMutex::new(0);
        let guard = pollster::block_on(mutex.lock());

        let mut second = pin!(mutex.lock());
        assert!(poll_once(second.as_mut()).is_pending());

        drop(guard);
    }

    #[test]
    fn lock_becomes_ready_and_wakes_after_guard_is_dropped() {
        let mutex = AsyncMutex::new(0);
        let guard = pollster::block_on(mutex.lock());
        let (waker, wakes) = counting_waker();

        let mut second = pin!(mutex.lock());
        assert!(poll_with(second.as_mut(), &waker).is_pending());
        assert_eq!(wakes.count(), 0);

        drop(guard);
        assert!(wakes.count() >= 1);

        let Poll::Ready(guard) = poll_with(second.as_mut(), &waker) else {
            panic!("lock should be available after the guard was dropped");
        };
        assert_eq!(*guard, 0);
    }

    #[test]
    fn lock_can_be_reacquired_repeatedly() {
        let mutex = AsyncMutex::new(0u32);
        for i in 1..=10 {
            let mut guard = pollster::block_on(mutex.lock());
            *guard = i;
        }
        assert_eq!(*pollster::block_on(mutex.lock()), 10);
    }

    #[test]
    fn dropping_a_pending_lock_future_does_not_hold_the_lock() {
        let mutex = AsyncMutex::new(0);
        let guard = pollster::block_on(mutex.lock());

        {
            let mut waiting = pin!(mutex.lock());
            assert!(poll_once(waiting.as_mut()).is_pending());
        }

        drop(guard);
        let _again = pollster::block_on(mutex.lock());
    }

    #[test]
    fn dropping_a_never_polled_lock_future_is_harmless() {
        let mutex = AsyncMutex::new(0);
        drop(mutex.lock());
        let _guard = pollster::block_on(mutex.lock());
    }

    #[test]
    fn guard_provides_exclusive_access_to_non_copy_values() {
        let mutex = AsyncMutex::new(alloc::vec![1, 2, 3]);
        {
            let mut guard = pollster::block_on(mutex.lock());
            guard.push(4);
        }
        assert_eq!(pollster::block_on(mutex.lock()).len(), 4);
    }

    #[test]
    fn rc_lock_succeeds_without_contention() {
        let mutex = AsyncMutex::new(7);
        let guard = pollster::block_on(mutex.lock_rc());
        assert_eq!(*guard, 7);
    }

    #[test]
    fn rc_guard_allows_mutation_that_persists() {
        let mutex = AsyncMutex::new(alloc::string::String::from("a"));
        {
            let mut guard = pollster::block_on(mutex.lock_rc());
            guard.push('b');
        }
        assert_eq!(&**pollster::block_on(mutex.lock_rc()), "ab");
    }

    #[test]
    fn rc_lock_is_pending_while_locked_and_ready_after_release() {
        let mutex = AsyncMutex::new(0);
        let guard = pollster::block_on(mutex.lock_rc());
        let (waker, wakes) = counting_waker();

        let mut second = pin!(mutex.lock_rc());
        assert!(poll_with(second.as_mut(), &waker).is_pending());

        drop(guard);
        assert!(wakes.count() >= 1);
        assert!(poll_with(second.as_mut(), &waker).is_ready());
    }

    #[test]
    fn rc_and_borrowed_locks_exclude_each_other() {
        let mutex = AsyncMutex::new(0);

        let rc_guard = pollster::block_on(mutex.lock_rc());
        let mut borrowed = pin!(mutex.lock());
        assert!(poll_once(borrowed.as_mut()).is_pending());
        drop(rc_guard);
        let borrowed_guard = poll_once(borrowed.as_mut());
        assert!(borrowed_guard.is_ready());

        let mut rc = pin!(mutex.lock_rc());
        assert!(poll_once(rc.as_mut()).is_pending());
    }

    #[test]
    fn rc_guard_keeps_the_mutex_state_alive_after_the_mutex_is_dropped() {
        let mutex = AsyncMutex::new(3);
        let guard = pollster::block_on(mutex.lock_rc());
        drop(mutex);

        assert_eq!(*guard, 3);
    }

    /// With several contenders, every waiter should acquire the lock as each
    /// previous holder releases it. Regression test for the `Event::notified`
    /// accounting bug, which used to swallow wake-ups for later waiters.
    #[test]
    fn waiters_acquire_the_lock_in_turn() {
        let mutex = AsyncMutex::new(0);
        let (waker, wakes) = counting_waker();

        let first = pollster::block_on(mutex.lock());
        let mut second = pin!(mutex.lock());
        let mut third = pin!(mutex.lock());
        assert!(poll_with(second.as_mut(), &waker).is_pending());
        assert!(poll_with(third.as_mut(), &waker).is_pending());

        drop(first);
        let Poll::Ready(second_guard) = poll_with(second.as_mut(), &waker) else {
            panic!("second waiter should acquire the lock first");
        };
        assert!(poll_with(third.as_mut(), &waker).is_pending());

        let wakes_before = wakes.count();
        drop(second_guard);
        assert!(wakes.count() > wakes_before, "third waiter must be woken");
        assert!(poll_with(third.as_mut(), &waker).is_ready());
    }
}
