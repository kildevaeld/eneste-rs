use alloc::rc::Rc;
use core::{
    cell::{Cell, UnsafeCell},
    pin::Pin,
    task::{Context, Poll},
};
use pin_project_lite::pin_project;

use crate::event::Event;

struct RwLockInner<T> {
    value: UnsafeCell<T>,
    event: Event,
    readers: Cell<usize>,
    locked: Cell<bool>,
}

impl<T> RwLockInner<T> {
    fn can_read(&self) -> bool {
        !self.locked.get()
    }

    fn can_write(&self) -> bool {
        !self.locked.get() && self.readers.get() == 0
    }

    fn increment_readers(&self) {
        self.readers.set(self.readers.get() + 1);
        self.event.notify(1);
    }

    fn decrement_readers(&self) {
        let readers = self.readers.get();
        assert!(readers > 0, "Attempted to decrement readers below zero");
        self.readers.set(readers - 1);
        self.event.notify(1);
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

pub struct RwLock<T>(Rc<RwLockInner<T>>);

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self(Rc::new(RwLockInner {
            value: UnsafeCell::new(value),
            event: Event::new(),
            readers: Cell::new(0),
            locked: Cell::new(false),
        }))
    }

    pub fn read(&self) -> ReadLockFuture<'_, T> {
        ReadLockFuture {
            lock: &*self.0,
            state: LockState::Idle,
        }
    }

    pub fn write(&self) -> WriteLockFuture<'_, T> {
        WriteLockFuture {
            lock: &*self.0,
            state: LockState::Idle,
        }
    }

    pub fn read_rc(&self) -> RcReadLockFuture<T> {
        RcReadLockFuture {
            lock: Rc::clone(&self.0),
            state: LockState::Idle,
        }
    }

    pub fn write_rc(&self) -> RcWriteLockFuture<T> {
        RcWriteLockFuture {
            lock: Rc::clone(&self.0),
            state: LockState::Idle,
        }
    }
}

pub struct RwLockReadGuard<'a, T> {
    value: &'a RwLockInner<T>,
}

impl<'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        self.value.decrement_readers();
    }
}

impl<'a, T> core::ops::Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value.value.get() }
    }
}

pub struct RwLockWriteGuard<'a, T> {
    value: &'a RwLockInner<T>,
}

impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.value.unlock();
    }
}

impl<'a, T> core::ops::Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value.value.get() }
    }
}

impl<'a, T> core::ops::DerefMut for RwLockWriteGuard<'a, T> {
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
    pub struct ReadLockFuture<'a, T> {
        #[pin]
        lock: &'a RwLockInner<T>,
        #[pin]
        state: LockState
    }
}

impl<'a, T> Future for ReadLockFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

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
                    if this.lock.can_read() {
                        this.lock.increment_readers();
                        return Poll::Ready(RwLockReadGuard { value: &this.lock });
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

pin_project! {
    pub struct WriteLockFuture<'a, T> {
        #[pin]
        lock: &'a RwLockInner<T>,
        #[pin]
        state: LockState
    }
}

impl<'a, T> Future for WriteLockFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

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
                    if this.lock.can_write() {
                        this.lock.lock();
                        return Poll::Ready(RwLockWriteGuard { value: &this.lock });
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

pub struct RcReadLockGuard<T> {
    lock: Rc<RwLockInner<T>>,
}

impl<T> core::ops::Deref for RcReadLockGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> core::ops::DerefMut for RcReadLockGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for RcReadLockGuard<T> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

pin_project! {
    pub struct RcReadLockFuture< T> {
        #[pin]
        lock: Rc<RwLockInner<T>>,
        #[pin]
        state: LockState
    }
}

impl<T> Future for RcReadLockFuture<T> {
    type Output = RcReadLockGuard<T>;

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
                    if this.lock.can_write() {
                        this.lock.lock();
                        return Poll::Ready(RcReadLockGuard {
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

pub struct RcWriteLockGuard<T> {
    lock: Rc<RwLockInner<T>>,
}

impl<T> core::ops::Deref for RcWriteLockGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> core::ops::DerefMut for RcWriteLockGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for RcWriteLockGuard<T> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

pin_project! {
    pub struct RcWriteLockFuture< T> {
        #[pin]
        lock: Rc<RwLockInner<T>>,
        #[pin]
        state: LockState
    }
}

impl<T> Future for RcWriteLockFuture<T> {
    type Output = RcWriteLockGuard<T>;

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
                    if this.lock.can_write() {
                        this.lock.lock();
                        return Poll::Ready(RcWriteLockGuard {
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

    use alloc::rc::Rc;
    use core::{
        cell::Cell,
        future::Future,
        pin::Pin,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };

    fn noop_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(core::ptr::null(), &VTABLE)
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VTABLE)) }
    }

    fn counting_waker() -> (Waker, Rc<Cell<usize>>) {
        unsafe fn clone(data: *const ()) -> RawWaker {
            let counter = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
            let cloned = Rc::clone(&counter);
            let _ = Rc::into_raw(counter);
            RawWaker::new(Rc::into_raw(cloned).cast(), &VTABLE)
        }

        unsafe fn wake(data: *const ()) {
            let counter = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
            counter.set(counter.get() + 1);
        }

        unsafe fn wake_by_ref(data: *const ()) {
            let counter = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
            counter.set(counter.get() + 1);
            let _ = Rc::into_raw(counter);
        }

        unsafe fn release(data: *const ()) {
            let _ = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
        }

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, release);

        let counter = Rc::new(Cell::new(0));
        let raw = RawWaker::new(Rc::into_raw(Rc::clone(&counter)).cast(), &VTABLE);

        unsafe { (Waker::from_raw(raw), counter) }
    }

    fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        let mut cx = Context::from_waker(waker);
        future.poll(&mut cx)
    }

    #[test]
    fn read_and_write_succeed_without_contention() {
        let lock = RwLock::new(3usize);

        let read = pollster::block_on(lock.read());
        assert_eq!(*read, 3);
        drop(read);

        {
            let mut write = pollster::block_on(lock.write());
            *write = 8;
        }

        let read = pollster::block_on(lock.read());
        assert_eq!(*read, 8);
    }

    #[test]
    fn read_waits_for_active_writer_to_drop() {
        let lock = RwLock::new(11usize);
        let mut writer = pollster::block_on(lock.write());
        let mut reader = lock.read();
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert!(matches!(
            poll_once(Pin::new(&mut reader), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        *writer = 12;
        drop(writer);

        assert_eq!(wake_count.get(), 1);
        let reader = match poll_once(Pin::new(&mut reader), &idle_waker) {
            Poll::Ready(reader) => reader,
            Poll::Pending => panic!("reader should be ready after writer drop"),
        };
        assert_eq!(*reader, 12);
    }

    #[test]
    fn write_waits_for_active_reader_to_drop() {
        let lock = RwLock::new(21usize);
        let reader = pollster::block_on(lock.read());
        let mut writer = lock.write();
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert!(matches!(
            poll_once(Pin::new(&mut writer), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        drop(reader);

        assert_eq!(wake_count.get(), 1);
        let mut writer = match poll_once(Pin::new(&mut writer), &idle_waker) {
            Poll::Ready(writer) => writer,
            Poll::Pending => panic!("writer should be ready after reader drop"),
        };
        *writer = 34;
        drop(writer);

        let reader = pollster::block_on(lock.read());
        assert_eq!(*reader, 34);
    }

    #[test]
    fn lock_method_returns_guard_that_can_mutate_value() {
        let lock = RwLock::new(5usize);

        {
            let mut guard = pollster::block_on(lock.write_rc());
            *guard = 99;
        }

        let reader = pollster::block_on(lock.read());
        assert_eq!(*reader, 99);
    }

    #[test]
    fn lock_future_waits_until_reader_releases() {
        let lock = RwLock::new(1usize);
        let reader = pollster::block_on(lock.read());
        let mut lock_future = lock.write_rc();
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert!(matches!(
            poll_once(Pin::new(&mut lock_future), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        drop(reader);

        assert_eq!(wake_count.get(), 1);
        let mut guard = match poll_once(Pin::new(&mut lock_future), &idle_waker) {
            Poll::Ready(guard) => guard,
            Poll::Pending => panic!("lock future should be ready after reader drop"),
        };
        *guard = 7;
        drop(guard);

        let reader = pollster::block_on(lock.read());
        assert_eq!(*reader, 7);
    }

    #[test]
    fn lock_guard_blocks_writer_until_dropped() {
        let lock = RwLock::new(3usize);
        let guard = pollster::block_on(lock.write_rc());
        let mut writer = lock.write();
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert!(matches!(
            poll_once(Pin::new(&mut writer), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        drop(guard);

        assert_eq!(wake_count.get(), 1);
        let mut writer = match poll_once(Pin::new(&mut writer), &idle_waker) {
            Poll::Ready(writer) => writer,
            Poll::Pending => panic!("writer should be ready after lock guard drop"),
        };
        *writer = 10;
        drop(writer);

        let reader = pollster::block_on(lock.read());
        assert_eq!(*reader, 10);
    }
}
