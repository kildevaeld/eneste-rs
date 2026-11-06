use alloc::rc::Rc;
use core::{cell::Cell, future::Future, ops::Deref};

use crate::{
    event::Event,
    lock::{RcReadLockGuard, RwLock},
    spawner::{LocalSpawner, Task},
};

struct Shared<T> {
    /// `None` until the future has resolved.
    value: RwLock<Option<T>>,
    /// Notified (to everyone) once `value` has been set.
    ready: Event,
}

impl<T> Shared<T> {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            value: RwLock::new(None),
            ready: Event::new(),
        })
    }

    /// Spawns `future` onto `spawner`, storing its output in `self` once it resolves.
    fn spawn<'a, S, F>(self: &Rc<Self>, spawner: &S, future: F)
    where
        S: LocalSpawner<'a> + ?Sized,
        F: Future<Output = T> + 'a,
        T: 'a,
    {
        let shared = self.clone();

        spawner
            .spawn(async move {
                let value = future.await;
                *shared.value.write().await = Some(value);
                shared.ready.notify(usize::MAX);
            })
            .detach();
    }

    async fn get(&self) -> LazyGuard<T> {
        loop {
            // Listen before checking the value so a completion between the check and the
            // wait is not missed.
            let listener = self.ready.listen();
            let guard = self.value.read_rc().await;

            if guard.is_some() {
                return LazyGuard(guard);
            }

            drop(guard);
            listener.await;
        }
    }
}

/// A value produced by a future that is spawned at most once, and shared by every caller of
/// [`AsyncLazy::get`].
///
/// The future is not spawned until the first call to [`AsyncLazy::get`] is polled. All
/// `get` calls, concurrent or later, resolve to the result of that single future.
///
/// If the future never completes, or the spawner drops the task before it completes, every
/// pending `get` waits forever.
pub struct AsyncLazy<S, F: Future> {
    spawner: S,
    /// Taken by the first `get`, which spawns it.
    future: Cell<Option<F>>,
    shared: Rc<Shared<F::Output>>,
}

impl<'a, S, F> AsyncLazy<S, F>
where
    S: LocalSpawner<'a>,
    F: Future + 'a,
    F::Output: 'a,
{
    pub fn new(spawner: S, future: F) -> Self {
        Self {
            spawner,
            future: Cell::new(Some(future)),
            shared: Shared::new(),
        }
    }

    /// Spawns the future if that has not already happened.
    fn start(&self) {
        if let Some(future) = self.future.take() {
            self.shared.spawn(&self.spawner, future);
        }
    }

    /// Waits for the future to resolve, spawning it if this is the first call, and returns a
    /// guard that dereferences to the value.
    ///
    /// The guard holds a read lock on the value, so it should not be held across points where
    /// something might wait on a writer.
    pub async fn get(&self) -> LazyGuard<F::Output> {
        self.start();
        self.shared.get().await
    }
}

/// Like [`AsyncLazy`], but the future is spawned immediately on construction.
///
/// The spawner is only borrowed for the duration of [`EagerAsyncLazy::new`], so it is not stored.
/// Every [`EagerAsyncLazy::get`] resolves to the result of that single spawned future.
///
/// If the future never completes, or the spawner drops the task before it completes, every
/// pending `get` waits forever.
pub struct EagerAsyncLazy<T> {
    shared: Rc<Shared<T>>,
}

impl<T> EagerAsyncLazy<T> {
    /// Spawns `future` onto `spawner` right away.
    pub fn new<'a, S, F>(spawner: &S, future: F) -> Self
    where
        S: LocalSpawner<'a> + ?Sized,
        F: Future<Output = T> + 'a,
        T: 'a,
    {
        let shared = Shared::new();
        shared.spawn(spawner, future);
        Self { shared }
    }

    /// Waits for the future to resolve and returns a guard that dereferences to the value.
    ///
    /// The guard holds a read lock on the value.
    pub async fn get(&self) -> LazyGuard<T> {
        self.shared.get().await
    }
}

/// Read guard returned by [`AsyncLazy::get`].
pub struct LazyGuard<T>(RcReadLockGuard<Option<T>>);

impl<T> Deref for LazyGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0
            .as_ref()
            .expect("LazyGuard is only created once the value is set")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{TestSpawner, poll_once};
    use core::pin::pin;

    fn counted(counter: &Rc<Cell<usize>>, value: usize) -> impl Future<Output = usize> + use<> {
        let counter = counter.clone();
        async move {
            counter.set(counter.get() + 1);
            value
        }
    }

    #[test]
    fn nothing_is_spawned_until_first_get() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let lazy = AsyncLazy::new(spawner.clone(), counted(&runs, 1));

        assert_eq!(spawner.pending(), 0);

        let mut get = pin!(lazy.get());
        assert!(poll_once(get.as_mut()).is_pending());
        assert_eq!(spawner.pending(), 1);
        assert_eq!(runs.get(), 0);
    }

    #[test]
    fn get_resolves_after_the_spawned_future_runs() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let lazy = AsyncLazy::new(spawner.clone(), counted(&runs, 42));

        let mut get = pin!(lazy.get());
        assert!(poll_once(get.as_mut()).is_pending());

        spawner.run();

        let guard = match poll_once(get.as_mut()) {
            core::task::Poll::Ready(guard) => guard,
            core::task::Poll::Pending => panic!("get should be ready"),
        };
        assert_eq!(*guard, 42);
        assert_eq!(runs.get(), 1);
    }

    #[test]
    fn concurrent_gets_share_one_future() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let lazy = AsyncLazy::new(spawner.clone(), counted(&runs, 7));

        let mut a = pin!(lazy.get());
        let mut b = pin!(lazy.get());
        let mut c = pin!(lazy.get());
        assert!(poll_once(a.as_mut()).is_pending());
        assert!(poll_once(b.as_mut()).is_pending());
        assert!(poll_once(c.as_mut()).is_pending());
        assert_eq!(spawner.pending(), 1);

        spawner.run();

        let (a, b, c) = (
            poll_once(a.as_mut()),
            poll_once(b.as_mut()),
            poll_once(c.as_mut()),
        );
        match (a, b, c) {
            (
                core::task::Poll::Ready(a),
                core::task::Poll::Ready(b),
                core::task::Poll::Ready(c),
            ) => assert_eq!((*a, *b, *c), (7, 7, 7)),
            _ => panic!("all gets should be ready"),
        }
        assert_eq!(runs.get(), 1);
    }

    #[test]
    fn get_after_completion_is_ready_without_spawning() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let lazy = AsyncLazy::new(spawner.clone(), counted(&runs, 3));

        let mut first = pin!(lazy.get());
        assert!(poll_once(first.as_mut()).is_pending());
        spawner.run();
        assert!(poll_once(first.as_mut()).is_ready());

        let mut second = pin!(lazy.get());
        let guard = match poll_once(second.as_mut()) {
            core::task::Poll::Ready(guard) => guard,
            core::task::Poll::Pending => panic!("get should be ready immediately"),
        };
        assert_eq!(*guard, 3);
        assert_eq!(spawner.pending(), 0);
        assert_eq!(runs.get(), 1);
    }

    #[test]
    fn non_clone_values_are_supported() {
        struct NoClone(&'static str);

        let spawner = TestSpawner::new();
        let lazy = AsyncLazy::new(spawner.clone(), async { NoClone("hello") });

        let mut get = pin!(lazy.get());
        assert!(poll_once(get.as_mut()).is_pending());
        spawner.run();

        match poll_once(get.as_mut()) {
            core::task::Poll::Ready(guard) => assert_eq!((*guard).0, "hello"),
            core::task::Poll::Pending => panic!("get should be ready"),
        }
    }

    #[test]
    fn dropping_a_waiting_get_does_not_affect_others() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let lazy = AsyncLazy::new(spawner.clone(), counted(&runs, 9));

        let mut b = pin!(lazy.get());
        {
            let mut a = pin!(lazy.get());
            assert!(poll_once(a.as_mut()).is_pending());
            assert!(poll_once(b.as_mut()).is_pending());
        }

        spawner.run();

        match poll_once(b.as_mut()) {
            core::task::Poll::Ready(guard) => assert_eq!(*guard, 9),
            core::task::Poll::Pending => panic!("remaining get should be ready"),
        }
    }

    #[test]
    fn eager_spawns_immediately() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let _lazy = EagerAsyncLazy::new(&spawner, counted(&runs, 5));

        assert_eq!(spawner.pending(), 1);
        spawner.run();
        assert_eq!(runs.get(), 1);
        assert_eq!(spawner.pending(), 0);
    }

    #[test]
    fn eager_gets_share_one_future() {
        let spawner = TestSpawner::new();
        let runs = Rc::new(Cell::new(0));
        let lazy = EagerAsyncLazy::new(&spawner, counted(&runs, 11));

        let mut a = pin!(lazy.get());
        let mut b = pin!(lazy.get());
        assert!(poll_once(a.as_mut()).is_pending());
        assert!(poll_once(b.as_mut()).is_pending());

        spawner.run();

        match (poll_once(a.as_mut()), poll_once(b.as_mut())) {
            (core::task::Poll::Ready(a), core::task::Poll::Ready(b)) => {
                assert_eq!((*a, *b), (11, 11));
            }
            _ => panic!("all gets should be ready"),
        }

        let mut late = pin!(lazy.get());
        assert!(poll_once(late.as_mut()).is_ready());
        assert_eq!(runs.get(), 1);
        assert_eq!(spawner.pending(), 0);
    }
}
