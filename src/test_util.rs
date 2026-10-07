//! Shared helpers for unit tests. Only compiled with `cfg(test)`.

use alloc::{boxed::Box, rc::Rc, sync::Arc, task::Wake, vec::Vec};
use core::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};
use futures_core::Stream;
use goerdet::{LocalSpawner, Task};

/// Counts how many times a [`Waker`] built from it has been woken.
#[derive(Default)]
pub struct WakeCounter(AtomicUsize);

impl WakeCounter {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn counting_waker() -> (Waker, Arc<WakeCounter>) {
    let counter = Arc::new(WakeCounter::default());
    (Waker::from(counter.clone()), counter)
}

/// Polls a future once with a no-op waker.
pub fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    future.poll(&mut cx)
}

/// Polls a future once with the provided waker.
pub fn poll_with<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    let mut cx = Context::from_waker(waker);
    future.poll(&mut cx)
}

/// Polls a stream once with a no-op waker.
pub fn poll_next_once<S: Stream + ?Sized>(stream: Pin<&mut S>) -> Poll<Option<S::Item>> {
    let mut cx = Context::from_waker(Waker::noop());
    stream.poll_next(&mut cx)
}

/// Polls a stream once with the provided waker.
pub fn poll_next_with<S: Stream + ?Sized>(
    stream: Pin<&mut S>,
    waker: &Waker,
) -> Poll<Option<S::Item>> {
    let mut cx = Context::from_waker(waker);
    stream.poll_next(&mut cx)
}

/// A finite, always-ready stream over an iterator.
pub struct IterStream<I>(pub I);

impl<I: Iterator + Unpin> Stream for IterStream<I> {
    type Item = I::Item;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.next())
    }
}

type BoxedTask<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

/// A minimal single-threaded spawner. Tasks only make progress when
/// [`TestSpawner::run`] is called, which keeps tests fully deterministic.
#[derive(Clone, Default)]
pub struct TestSpawner<'a> {
    tasks: Rc<RefCell<Vec<BoxedTask<'a>>>>,
}

pub struct TestTask;

impl Task for TestTask {
    fn detach(self) {}
}

impl<'a> TestSpawner<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of tasks that have not completed yet.
    pub fn pending(&self) -> usize {
        self.tasks.borrow().len()
    }

    /// Polls every task once and drops those that completed.
    /// Tasks spawned while running are polled on the next call.
    pub fn run(&self) {
        let tasks = core::mem::take(&mut *self.tasks.borrow_mut());
        let mut cx = Context::from_waker(Waker::noop());
        let mut still_pending = Vec::new();
        for mut task in tasks {
            if task.as_mut().poll(&mut cx).is_pending() {
                still_pending.push(task);
            }
        }
        // New tasks may have been spawned while polling.
        let mut tasks = self.tasks.borrow_mut();
        still_pending.append(&mut tasks);
        *tasks = still_pending;
    }
}

impl<'a> LocalSpawner<'a> for TestSpawner<'a> {
    type Task = TestTask;

    fn spawn<T>(&self, work: T) -> Self::Task
    where
        T: Future<Output = ()> + 'a,
    {
        self.tasks.borrow_mut().push(Box::pin(work));
        TestTask
    }
}
