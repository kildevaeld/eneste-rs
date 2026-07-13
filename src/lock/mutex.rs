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
