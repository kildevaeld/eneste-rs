use core::{
    cell::{Cell, RefCell, UnsafeCell},
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    task::{Context, Waker},
};

use alloc::{
    boxed::Box,
    collections::{BTreeMap, VecDeque},
    rc::{Rc, Weak},
    sync::Arc,
    task::Wake,
    vec::Vec,
};

use goerdet::{LocalSpawner, Task};

/// A trait for waking up the event loop when a task is scheduled.
///
/// Task wakers may be used from any thread, so the event loop waker must be
/// `Send + Sync`. Spawned futures themselves never leave the executor's thread.
pub trait EventLoopWaker: Send + Sync {
    fn wake(&self);
}

// Tasks stay boxed so the executor can keep polling futures that borrow from `'a`.
type BoxFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

// Header state flags.
// Prevents the same task from being enqueued multiple times before it is polled.
const QUEUED: u8 = 1 << 0;
// Canceled tasks drop their future and ignore any later wake-ups.
const CANCELED: u8 = 1 << 1;
// Completed tasks should never be queued again.
const COMPLETED: u8 = 1 << 2;

/// A minimal spin lock, since `no_std` has no `Mutex`.
/// Critical sections only push to or drain the inbox, so contention is short.
struct SpinLock<V> {
    locked: AtomicBool,
    value: UnsafeCell<V>,
}

// SAFETY: access to `value` is serialized by `locked`.
unsafe impl<V: Send> Sync for SpinLock<V> {}

impl<V> SpinLock<V> {
    fn new(value: V) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    fn lock(&self) -> SpinGuard<'_, V> {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        SpinGuard { lock: self }
    }
}

struct SpinGuard<'l, V> {
    lock: &'l SpinLock<V>,
}

impl<V> Deref for SpinGuard<'_, V> {
    type Target = V;

    fn deref(&self) -> &V {
        // SAFETY: the guard holds the lock.
        unsafe { &*self.lock.value.get() }
    }
}

impl<V> DerefMut for SpinGuard<'_, V> {
    fn deref_mut(&mut self) -> &mut V {
        // SAFETY: the guard holds the lock.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<V> Drop for SpinGuard<'_, V> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

/// The thread-safe part of the executor, reachable from any waker.
struct Shared<T> {
    inbox: SpinLock<VecDeque<Arc<Header<T>>>>,
    notifier: Arc<T>,
    // Set once the executor is gone so late wake-ups become no-ops.
    closed: AtomicBool,
}

/// The thread-safe part of a task. Wakers only ever hold this, never the future.
struct Header<T> {
    state: AtomicU8,
    id: u64,
    shared: Arc<Shared<T>>,
}

impl<T> Header<T> {
    fn is_finished(&self) -> bool {
        self.state.load(Ordering::Acquire) & (CANCELED | COMPLETED) != 0
    }
}

impl<T> Header<T>
where
    T: EventLoopWaker,
{
    fn schedule(self: &Arc<Self>) {
        if self.is_finished() || self.shared.closed.load(Ordering::Acquire) {
            return;
        }

        // Queue the task exactly once until it gets polled again.
        if self.state.fetch_or(QUEUED, Ordering::AcqRel) & QUEUED != 0 {
            return;
        }

        self.shared.inbox.lock().push_back(self.clone());
        self.shared.notifier.wake();
    }
}

impl<T> Wake for Header<T>
where
    T: EventLoopWaker + 'static,
{
    fn wake(self: Arc<Self>) {
        self.schedule();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
    }
}

/// The thread-local part of a task: the future itself, owned by the executor.
struct Slot<'a, T> {
    // The future is taken out while polling so wake-ups cannot alias the active borrow.
    future: Option<BoxFuture<'a>>,
    header: Arc<Header<T>>,
}

struct ExecutorState<'a, T> {
    // Futures live here, so they are only ever polled and dropped on this thread.
    slots: RefCell<BTreeMap<u64, Slot<'a, T>>>,
    // Ids are never reused, so a stale inbox entry can never hit another task.
    next_id: Cell<u64>,
    shared: Arc<Shared<T>>,
}

impl<'a, T> Drop for ExecutorState<'a, T> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        let inbox = core::mem::take(&mut *self.shared.inbox.lock());
        drop(inbox);

        // Drop every future here, while the data they borrow for `'a` is still alive.
        let slots = core::mem::take(self.slots.get_mut());
        for slot in slots.values() {
            slot.header.state.fetch_or(CANCELED, Ordering::AcqRel);
        }
        drop(slots);
    }
}

/// A single-threaded executor.
///
/// Spawned futures need not be `Send`: they are only ever polled and dropped
/// on the executor's thread. Their wakers are thread-safe and may be woken
/// from anywhere.
///
/// The executor owns every spawned future until it completes or is canceled.
/// A pending task that holds a clone of its own executor therefore keeps the
/// executor alive, and both are leaked.
pub struct Executor<'lifetime, T> {
    state: Rc<ExecutorState<'lifetime, T>>,
}

impl<'lifetime, T> Clone for Executor<'lifetime, T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<'a, T> Executor<'a, T>
where
    T: EventLoopWaker + 'static,
{
    pub fn new(waker: Arc<T>) -> Self {
        Self {
            state: Rc::new(ExecutorState {
                slots: RefCell::new(BTreeMap::new()),
                next_id: Cell::new(0),
                shared: Arc::new(Shared {
                    inbox: SpinLock::new(VecDeque::new()),
                    notifier: waker,
                    closed: AtomicBool::new(false),
                }),
            }),
        }
    }

    /// Process a number of tasks in the executor's queue.
    /// This will run the tasks in the order they were scheduled.
    pub fn process_tasks(&self, count: usize) {
        let tasks_to_run = {
            let mut inbox = self.state.shared.inbox.lock();
            let count = count.min(inbox.len());
            inbox.drain(..count).collect::<Vec<_>>()
        };

        for header in tasks_to_run {
            self.run(header);
        }
    }

    fn run(&self, header: Arc<Header<T>>) {
        if header.is_finished() {
            return;
        }

        header.state.fetch_and(!QUEUED, Ordering::AcqRel);

        let future = self
            .state
            .slots
            .borrow_mut()
            .get_mut(&header.id)
            .and_then(|slot| slot.future.take());
        let Some(mut future) = future else {
            return;
        };

        let waker = Waker::from(header.clone());
        let mut cx = Context::from_waker(&waker);

        if future.as_mut().poll(&mut cx).is_ready() {
            header.state.fetch_or(COMPLETED, Ordering::AcqRel);
            let slot = self.state.slots.borrow_mut().remove(&header.id);
            // Drop outside the borrow so drop code may spawn or cancel tasks.
            drop(slot);
            drop(future);
            return;
        }

        // Put the future back unless the task canceled itself while running.
        let leftover = match self.state.slots.borrow_mut().get_mut(&header.id) {
            Some(slot) if !header.is_finished() => {
                slot.future = Some(future);
                None
            }
            _ => Some(future),
        };
        drop(leftover);
    }

    /// Check if there are any tasks in the executor's queue.
    pub fn has_tasks(&self) -> bool {
        !self.state.shared.inbox.lock().is_empty()
    }

    /// Drive the executor until `future` completes and return its output.
    /// Other queued tasks keep running until the queue is empty.
    ///
    /// # Panics
    ///
    /// Panics if the queue runs dry while `future` is still pending. Nothing can
    /// wake it at that point, since this thread is busy inside `block_on`.
    pub fn block_on<'b: 'a, F>(&self, future: F) -> F::Output
    where
        F: Future + 'b,
        F::Output: 'a,
    {
        let output = Rc::new(RefCell::new(None));

        let slot = output.clone();
        let task = self.spawn(async move {
            let value = future.await;
            *slot.borrow_mut() = Some(value);
        });
        task.detach();

        while self.has_tasks() {
            self.process_tasks(usize::MAX);
        }

        output.borrow_mut().take().expect(
            "block_on: future is still pending but no tasks are left to drive it (deadlock)",
        )
    }
}

impl<'a, T> LocalSpawner<'a> for Executor<'a, T>
where
    T: EventLoopWaker + 'static,
{
    type Task = ExcutorTask<'a, T>;
    fn spawn<F>(&self, task: F) -> Self::Task
    where
        F: Future<Output = ()> + 'a,
    {
        let id = self.state.next_id.get();
        self.state.next_id.set(id + 1);

        let header = Arc::new(Header {
            state: AtomicU8::new(0),
            id,
            shared: self.state.shared.clone(),
        });

        self.state.slots.borrow_mut().insert(
            id,
            Slot {
                future: Some(Box::pin(task)),
                header: header.clone(),
            },
        );

        header.schedule();

        ExcutorTask {
            task: Some(header),
            state: Rc::downgrade(&self.state),
        }
    }
}

impl<'a, T> crate::spawner::DriverableSpawner<'a> for Executor<'a, T>
where
    T: EventLoopWaker + 'static,
{
    fn tick(&self) -> bool {
        if self.has_tasks() {
            self.process_tasks(usize::MAX);
            true
        } else {
            false
        }
    }
}

pub struct ExcutorTask<'a, T> {
    // Detached tasks keep running even if their handle is dropped.
    task: Option<Arc<Header<T>>>,
    state: Weak<ExecutorState<'a, T>>,
}

impl<'a, T> Task for ExcutorTask<'a, T> {
    fn detach(mut self) {
        self.task.take();
    }
}

impl<'a, T> Drop for ExcutorTask<'a, T> {
    fn drop(&mut self) {
        let Some(header) = self.task.take() else {
            return;
        };

        if header.state.fetch_or(CANCELED, Ordering::AcqRel) & (CANCELED | COMPLETED) != 0 {
            return;
        }

        let Some(state) = self.state.upgrade() else {
            return;
        };

        let slot = state.slots.borrow_mut().remove(&header.id);
        // Drop outside the borrow so drop code may spawn or cancel tasks.
        drop(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::emitter::{EventEmitter, EventTargetExt};
    use alloc::{rc::Rc, sync::Arc, vec, vec::Vec};
    use core::{
        cell::{Cell, RefCell},
        future,
    };

    extern crate std;

    #[derive(Default)]
    struct TestWaker {
        wake_count: core::sync::atomic::AtomicUsize,
    }

    impl TestWaker {
        fn count(&self) -> usize {
            self.wake_count.load(core::sync::atomic::Ordering::SeqCst)
        }
    }

    impl EventLoopWaker for TestWaker {
        fn wake(&self) {
            self.wake_count
                .fetch_add(1, core::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn spawn_schedules_task_and_wakes_event_loop() {
        let waker = Arc::new(TestWaker::default());
        let executor = Executor::new(waker.clone());
        let task = executor.spawn(future::ready(()));

        assert_eq!(waker.count(), 1);
        assert!(executor.has_tasks());

        task.detach();
        executor.process_tasks(1);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn process_tasks_respects_count_limit() {
        let waker = Arc::new(TestWaker::default());
        let executor = Executor::new(waker);
        let output = Rc::new(RefCell::new(Vec::new()));

        let first_output = output.clone();
        executor
            .spawn(async move {
                first_output.borrow_mut().push(1);
            })
            .detach();

        let second_output = output.clone();
        executor
            .spawn(async move {
                second_output.borrow_mut().push(2);
            })
            .detach();

        executor.process_tasks(1);
        assert_eq!(output.borrow().as_slice(), &[1]);
        assert!(executor.has_tasks());

        executor.process_tasks(1);
        assert_eq!(output.borrow().as_slice(), &[1, 2]);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn tasks_run_in_spawn_order() {
        let waker = Arc::new(TestWaker::default());
        let executor = Executor::new(waker);
        let output = Rc::new(RefCell::new(vec![]));

        for value in [1, 2, 3] {
            let output = output.clone();
            executor
                .spawn(async move {
                    output.borrow_mut().push(value);
                })
                .detach();
        }

        executor.process_tasks(3);

        assert_eq!(output.borrow().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn processing_tasks_allows_rescheduling_while_running() {
        let waker = Arc::new(TestWaker::default());
        let executor = Executor::new(waker.clone());
        let output = Rc::new(RefCell::new(Vec::new()));

        let emitter = crate::emitter::Emitter::new();
        let captured = output.clone();
        let _task = emitter.listen(&executor, move |value| {
            captured.borrow_mut().push(value);
            value < 2
        });

        executor.process_tasks(1);
        assert!(output.borrow().is_empty());
        assert!(!executor.has_tasks());

        emitter.emit(1usize);
        assert!(executor.has_tasks());

        executor.process_tasks(1);
        assert_eq!(output.borrow().as_slice(), &[1]);
        assert!(!executor.has_tasks());

        emitter.emit(2usize);
        assert!(executor.has_tasks());

        executor.process_tasks(1);
        assert_eq!(output.borrow().as_slice(), &[1, 2]);
        assert!(!executor.has_tasks());

        emitter.emit(3usize);
        assert!(!executor.has_tasks());
        assert_eq!(output.borrow().as_slice(), &[1, 2]);
        assert_eq!(waker.count(), 3);
    }
    /// Sets a flag when dropped, so tests can observe when a future is released.
    struct DropFlag(Rc<Cell<bool>>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    /// Returns `Pending` once and wakes itself, mimicking a cooperative yield.
    struct YieldNow(bool);

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> core::task::Poll<()> {
            if self.0 {
                core::task::Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                core::task::Poll::Pending
            }
        }
    }

    fn yield_now() -> YieldNow {
        YieldNow(false)
    }

    /// Stays pending until `ready` is set, storing the latest waker in `slot`.
    struct WaitFor {
        ready: Rc<Cell<bool>>,
        slot: Rc<RefCell<Option<Waker>>>,
    }

    impl Future for WaitFor {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> core::task::Poll<()> {
            if self.ready.get() {
                core::task::Poll::Ready(())
            } else {
                *self.slot.borrow_mut() = Some(cx.waker().clone());
                core::task::Poll::Pending
            }
        }
    }

    fn new_executor() -> (Executor<'static, TestWaker>, Arc<TestWaker>) {
        let waker = Arc::new(TestWaker::default());
        (Executor::new(waker.clone()), waker)
    }

    fn queue_len<T>(executor: &Executor<'_, T>) -> usize {
        executor.state.shared.inbox.lock().len()
    }

    #[test]
    fn spawn_does_not_poll_eagerly() {
        let (executor, _) = new_executor();
        let ran = Rc::new(Cell::new(false));

        let flag = ran.clone();
        executor.spawn(async move { flag.set(true) }).detach();

        assert!(!ran.get());
        executor.process_tasks(usize::MAX);
        assert!(ran.get());
    }

    #[test]
    fn new_executor_has_no_tasks() {
        let (executor, waker) = new_executor();

        assert!(!executor.has_tasks());
        assert_eq!(waker.count(), 0);
        executor.process_tasks(usize::MAX);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn process_tasks_with_zero_count_does_nothing() {
        let (executor, _) = new_executor();
        let ran = Rc::new(Cell::new(false));

        let flag = ran.clone();
        executor.spawn(async move { flag.set(true) }).detach();

        executor.process_tasks(0);
        assert!(!ran.get());
        assert!(executor.has_tasks());
    }

    #[test]
    fn process_tasks_with_count_above_queue_len_runs_everything() {
        let (executor, _) = new_executor();
        let counter = Rc::new(Cell::new(0));

        for _ in 0..3 {
            let counter = counter.clone();
            executor
                .spawn(async move { counter.set(counter.get() + 1) })
                .detach();
        }

        executor.process_tasks(10);
        assert_eq!(counter.get(), 3);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn each_spawn_wakes_event_loop_once() {
        let (executor, waker) = new_executor();

        for expected in 1..=5 {
            executor.spawn(future::ready(())).detach();
            assert_eq!(waker.count(), expected);
        }

        assert_eq!(queue_len(&executor), 5);
    }

    #[test]
    fn dropping_handle_before_poll_cancels_task() {
        let (executor, _) = new_executor();
        let ran = Rc::new(Cell::new(false));
        let dropped = Rc::new(Cell::new(false));

        let flag = ran.clone();
        let guard = DropFlag(dropped.clone());
        let task = executor.spawn(async move {
            let _guard = guard;
            flag.set(true);
        });

        drop(task);
        // The future is released as soon as the handle goes away.
        assert!(dropped.get());

        executor.process_tasks(usize::MAX);
        assert!(!ran.get());
        assert!(!executor.has_tasks());
    }

    #[test]
    fn dropping_handle_of_pending_task_drops_future() {
        let (executor, _) = new_executor();
        let dropped = Rc::new(Cell::new(false));
        let ready = Rc::new(Cell::new(false));
        let slot = Rc::new(RefCell::new(None));

        let guard = DropFlag(dropped.clone());
        let wait = WaitFor {
            ready: ready.clone(),
            slot: slot.clone(),
        };
        let task = executor.spawn(async move {
            let _guard = guard;
            wait.await;
        });

        executor.process_tasks(usize::MAX);
        assert!(!dropped.get());
        assert!(slot.borrow().is_some());

        drop(task);
        assert!(dropped.get());

        // Waking a canceled task must not requeue it.
        ready.set(true);
        slot.borrow_mut().take().unwrap().wake();
        assert!(!executor.has_tasks());
    }

    #[test]
    fn detached_task_keeps_running_after_handle_is_gone() {
        let (executor, _) = new_executor();
        let ready = Rc::new(Cell::new(false));
        let slot = Rc::new(RefCell::new(None));
        let done = Rc::new(Cell::new(false));

        let wait = WaitFor {
            ready: ready.clone(),
            slot: slot.clone(),
        };
        let flag = done.clone();
        executor
            .spawn(async move {
                wait.await;
                flag.set(true);
            })
            .detach();

        executor.process_tasks(usize::MAX);
        assert!(!done.get());

        ready.set(true);
        slot.borrow_mut().take().unwrap().wake();
        assert!(executor.has_tasks());

        executor.process_tasks(usize::MAX);
        assert!(done.get());
    }

    #[test]
    fn held_handle_keeps_task_alive() {
        let (executor, _) = new_executor();
        let counter = Rc::new(Cell::new(0));

        let count = counter.clone();
        let task = executor.spawn(async move {
            for _ in 0..3 {
                count.set(count.get() + 1);
                yield_now().await;
            }
        });

        while executor.has_tasks() {
            executor.process_tasks(usize::MAX);
        }

        assert_eq!(counter.get(), 3);
        drop(task);
    }

    #[test]
    fn future_is_dropped_on_completion() {
        let (executor, _) = new_executor();
        let dropped = Rc::new(Cell::new(false));

        let guard = DropFlag(dropped.clone());
        let task = executor.spawn(async move {
            let _guard = guard;
        });

        executor.process_tasks(usize::MAX);
        assert!(dropped.get());
        drop(task);
    }

    #[test]
    fn multiple_wakes_before_poll_queue_task_once() {
        let (executor, waker) = new_executor();
        let ready = Rc::new(Cell::new(false));
        let slot = Rc::new(RefCell::new(None));
        let polls = Rc::new(Cell::new(0));

        let wait_ready = ready.clone();
        let wait_slot = slot.clone();
        let poll_count = polls.clone();
        executor
            .spawn(future::poll_fn(move |cx| {
                poll_count.set(poll_count.get() + 1);
                let mut wait = WaitFor {
                    ready: wait_ready.clone(),
                    slot: wait_slot.clone(),
                };
                Pin::new(&mut wait).poll(cx)
            }))
            .detach();

        executor.process_tasks(usize::MAX);
        assert_eq!(polls.get(), 1);
        assert_eq!(waker.count(), 1);

        let stored = slot.borrow().clone().unwrap();
        stored.wake_by_ref();
        stored.wake_by_ref();
        let cloned = stored.clone();
        cloned.wake();

        assert_eq!(queue_len(&executor), 1);
        assert_eq!(waker.count(), 2);

        executor.process_tasks(usize::MAX);
        assert_eq!(polls.get(), 2);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn waking_completed_task_is_ignored() {
        let (executor, waker) = new_executor();
        let slot: Rc<RefCell<Option<Waker>>> = Rc::new(RefCell::new(None));

        let captured = slot.clone();
        executor
            .spawn(future::poll_fn(move |cx| {
                *captured.borrow_mut() = Some(cx.waker().clone());
                core::task::Poll::Ready(())
            }))
            .detach();

        executor.process_tasks(usize::MAX);
        assert_eq!(waker.count(), 1);

        let stored = slot.borrow_mut().take().unwrap();
        stored.wake_by_ref();
        stored.wake();

        assert!(!executor.has_tasks());
        assert_eq!(waker.count(), 1);
    }

    #[test]
    fn waker_outliving_executor_is_harmless() {
        let slot: Rc<RefCell<Option<Waker>>> = Rc::new(RefCell::new(None));
        let ready = Rc::new(Cell::new(false));

        {
            let (executor, _) = new_executor();
            executor
                .spawn(WaitFor {
                    ready: ready.clone(),
                    slot: slot.clone(),
                })
                .detach();
            executor.process_tasks(usize::MAX);
        }

        let stored = slot.borrow_mut().take().unwrap();
        stored.wake_by_ref();
        let cloned = stored.clone();
        stored.wake();
        drop(cloned);
    }

    #[test]
    fn dropping_executor_releases_queued_futures() {
        let dropped = Rc::new(Cell::new(false));

        {
            let (executor, _) = new_executor();
            let guard = DropFlag(dropped.clone());
            executor
                .spawn(async move {
                    let _guard = guard;
                })
                .detach();
            assert!(executor.has_tasks());
        }

        assert!(dropped.get());
    }

    #[test]
    fn dropping_executor_releases_pending_futures_even_with_live_wakers() {
        let dropped = Rc::new(Cell::new(false));
        let slot = Rc::new(RefCell::new(None));

        {
            let (executor, _) = new_executor();
            let guard = DropFlag(dropped.clone());
            let wait = WaitFor {
                ready: Rc::new(Cell::new(false)),
                slot: slot.clone(),
            };
            executor
                .spawn(async move {
                    let _guard = guard;
                    wait.await;
                })
                .detach();
            executor.process_tasks(usize::MAX);
            assert!(!dropped.get());
        }

        // The executor owns the future, so a surviving waker does not keep it alive.
        assert!(dropped.get());
        slot.borrow_mut().take().unwrap().wake();
    }

    #[test]
    fn dropping_wakers_does_not_release_detached_task() {
        let (executor, _) = new_executor();
        let slot = Rc::new(RefCell::new(None));
        let dropped = Rc::new(Cell::new(false));

        let guard = DropFlag(dropped.clone());
        let wait = WaitFor {
            ready: Rc::new(Cell::new(false)),
            slot: slot.clone(),
        };
        executor
            .spawn(async move {
                let _guard = guard;
                wait.await;
            })
            .detach();
        executor.process_tasks(usize::MAX);

        let clones: Vec<Waker> = (0..4).map(|_| slot.borrow().clone().unwrap()).collect();
        slot.borrow_mut().take();
        drop(clones);

        // Wakers no longer own the task; the executor releases it on drop.
        assert!(!dropped.get());
        drop(executor);
        assert!(dropped.get());
    }

    #[test]
    fn executor_drops_borrowing_futures_before_borrowed_data() {
        let slot = Rc::new(RefCell::new(None));
        let log = RefCell::new(Vec::new());

        struct LogOnDrop<'l>(&'l RefCell<Vec<&'static str>>);

        impl Drop for LogOnDrop<'_> {
            fn drop(&mut self) {
                self.0.borrow_mut().push("future dropped");
            }
        }

        {
            let waker = Arc::new(TestWaker::default());
            let executor = Executor::new(waker);
            let guard = LogOnDrop(&log);
            let wait = WaitFor {
                ready: Rc::new(Cell::new(false)),
                slot: slot.clone(),
            };
            executor
                .spawn(async move {
                    let _guard = guard;
                    wait.await;
                })
                .detach();
            executor.process_tasks(usize::MAX);
            log.borrow_mut().push("executor dropping");
        }

        assert_eq!(*log.borrow(), ["executor dropping", "future dropped"]);
        // The waker outlives `log`'s borrow without owning anything that refers to it.
        drop(slot.borrow_mut().take());
    }

    #[test]
    fn wakers_are_send_and_sync_while_futures_are_not() {
        fn assert_send_sync<S: Send + Sync>(_: &S) {}

        let (executor, _) = new_executor();
        let slot = Rc::new(RefCell::new(None));
        // `Rc` makes this future `!Send`; spawn must still accept it.
        let not_send = Rc::new(());
        let wait = WaitFor {
            ready: Rc::new(Cell::new(false)),
            slot: slot.clone(),
        };
        let task = executor.spawn(async move {
            let _not_send = not_send;
            wait.await;
        });
        executor.process_tasks(usize::MAX);

        let waker = slot.borrow_mut().take().unwrap();
        assert_send_sync(&waker);
        drop(task);
    }

    #[test]
    fn waking_from_another_thread_runs_task_on_executor_thread() {
        let (executor, notifier) = new_executor();
        let ready = Rc::new(Cell::new(false));
        let slot = Rc::new(RefCell::new(None));
        let done = Rc::new(Cell::new(false));

        let wait = WaitFor {
            ready: ready.clone(),
            slot: slot.clone(),
        };
        let flag = done.clone();
        let owner = std::thread::current().id();
        executor
            .spawn(async move {
                wait.await;
                assert_eq!(std::thread::current().id(), owner);
                flag.set(true);
            })
            .detach();
        executor.process_tasks(usize::MAX);
        assert!(!executor.has_tasks());

        ready.set(true);
        let waker = slot.borrow_mut().take().unwrap();
        std::thread::spawn(move || waker.wake()).join().unwrap();

        // The foreign wake both queues the task and notifies the event loop.
        assert!(executor.has_tasks());
        assert_eq!(notifier.count(), 2);

        executor.process_tasks(usize::MAX);
        assert!(done.get());
    }

    #[test]
    fn concurrent_wakes_from_many_threads_queue_task_once() {
        let (executor, notifier) = new_executor();
        let slot = Rc::new(RefCell::new(None));

        executor
            .spawn(WaitFor {
                ready: Rc::new(Cell::new(false)),
                slot: slot.clone(),
            })
            .detach();
        executor.process_tasks(usize::MAX);

        let waker = slot.borrow_mut().take().unwrap();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let waker = waker.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        waker.wake_by_ref();
                        let clone = waker.clone();
                        drop(clone);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        assert_eq!(queue_len(&executor), 1);
        assert_eq!(notifier.count(), 2);
    }

    #[test]
    fn waker_dropped_on_another_thread_after_executor_is_gone() {
        let slot = Rc::new(RefCell::new(None));

        {
            let (executor, _) = new_executor();
            executor
                .spawn(WaitFor {
                    ready: Rc::new(Cell::new(false)),
                    slot: slot.clone(),
                })
                .detach();
            executor.process_tasks(usize::MAX);
        }

        let waker = slot.borrow_mut().take().unwrap();
        std::thread::spawn(move || {
            waker.wake_by_ref();
            let cloned = waker.clone();
            cloned.wake();
        })
        .join()
        .unwrap();
    }

    #[test]
    fn stale_queue_entry_after_cancel_is_never_polled() {
        let (executor, _) = new_executor();
        let polls = Rc::new(Cell::new(0));

        let count = polls.clone();
        let task = executor.spawn(future::poll_fn(move |_| {
            count.set(count.get() + 1);
            core::task::Poll::<()>::Pending
        }));
        assert_eq!(queue_len(&executor), 1);

        drop(task);
        // The entry is still queued, but processing it must skip the canceled task.
        assert_eq!(queue_len(&executor), 1);
        executor.process_tasks(usize::MAX);
        assert_eq!(polls.get(), 0);
        assert!(!executor.has_tasks());

        // Later spawns get fresh ids and are unaffected.
        let count = polls.clone();
        executor
            .spawn(async move { count.set(count.get() + 10) })
            .detach();
        executor.process_tasks(usize::MAX);
        assert_eq!(polls.get(), 10);
    }

    #[test]
    fn yielding_task_is_requeued_for_next_batch() {
        let (executor, _) = new_executor();
        let output = Rc::new(RefCell::new(Vec::new()));

        let captured = output.clone();
        executor
            .spawn(async move {
                captured.borrow_mut().push("a1");
                yield_now().await;
                captured.borrow_mut().push("a2");
            })
            .detach();

        let captured = output.clone();
        executor
            .spawn(async move {
                captured.borrow_mut().push("b1");
            })
            .detach();

        executor.process_tasks(usize::MAX);
        assert_eq!(output.borrow().as_slice(), &["a1", "b1"]);
        assert!(executor.has_tasks());

        executor.process_tasks(usize::MAX);
        assert_eq!(output.borrow().as_slice(), &["a1", "b1", "a2"]);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn yielding_tasks_interleave_round_robin() {
        let (executor, _) = new_executor();
        let output = Rc::new(RefCell::new(Vec::new()));

        for id in 0..3 {
            let output = output.clone();
            executor
                .spawn(async move {
                    for step in 0..3 {
                        output.borrow_mut().push((id, step));
                        yield_now().await;
                    }
                })
                .detach();
        }

        while executor.has_tasks() {
            executor.process_tasks(usize::MAX);
        }

        let expected: Vec<_> = (0..3)
            .flat_map(|step| (0..3).map(move |id| (id, step)))
            .collect();
        assert_eq!(*output.borrow(), expected);
    }

    #[test]
    fn task_spawned_while_processing_runs_in_next_batch() {
        let (executor, _) = new_executor();
        let output = Rc::new(RefCell::new(Vec::new()));

        let inner_executor = executor.clone();
        let captured = output.clone();
        executor
            .spawn(async move {
                captured.borrow_mut().push("outer");
                let nested = captured.clone();
                inner_executor
                    .spawn(async move {
                        nested.borrow_mut().push("inner");
                    })
                    .detach();
            })
            .detach();

        executor.process_tasks(usize::MAX);
        assert_eq!(output.borrow().as_slice(), &["outer"]);
        assert!(executor.has_tasks());

        executor.process_tasks(usize::MAX);
        assert_eq!(output.borrow().as_slice(), &["outer", "inner"]);
    }

    #[test]
    fn cancelling_queued_task_from_another_task_skips_it() {
        let (executor, _) = new_executor();
        let ran = Rc::new(Cell::new(false));
        let handle = Rc::new(RefCell::new(None));

        let canceler = handle.clone();
        executor
            .spawn(async move {
                canceler.borrow_mut().take();
            })
            .detach();

        let flag = ran.clone();
        *handle.borrow_mut() = Some(executor.spawn(async move { flag.set(true) }));

        executor.process_tasks(usize::MAX);
        assert!(!ran.get());
        assert!(handle.borrow().is_none());
        assert!(!executor.has_tasks());
    }

    #[test]
    fn task_can_cancel_itself_while_running() {
        let (executor, _) = new_executor();
        let handle: Rc<RefCell<Option<ExcutorTask<'static, TestWaker>>>> =
            Rc::new(RefCell::new(None));
        let dropped = Rc::new(Cell::new(false));
        let after_cancel = Rc::new(Cell::new(false));

        let own_handle = handle.clone();
        let guard = DropFlag(dropped.clone());
        let flag = after_cancel.clone();
        let task = executor.spawn(async move {
            let _guard = guard;
            own_handle.borrow_mut().take();
            yield_now().await;
            flag.set(true);
        });
        *handle.borrow_mut() = Some(task);

        executor.process_tasks(usize::MAX);
        assert!(dropped.get());
        assert!(!executor.has_tasks());
        assert!(!after_cancel.get());
    }

    #[test]
    fn cloned_executors_share_the_queue() {
        let (executor, waker) = new_executor();
        let other = executor.clone();
        let counter = Rc::new(Cell::new(0));

        let count = counter.clone();
        other
            .spawn(async move { count.set(count.get() + 1) })
            .detach();
        assert!(executor.has_tasks());

        executor.process_tasks(usize::MAX);
        assert_eq!(counter.get(), 1);
        assert!(!other.has_tasks());
        assert_eq!(waker.count(), 1);
    }

    #[test]
    fn tick_reports_whether_work_was_done() {
        use crate::spawner::DriverableSpawner;

        let (executor, _) = new_executor();
        assert!(!executor.tick());

        let counter = Rc::new(Cell::new(0));
        let count = counter.clone();
        executor
            .spawn(async move {
                count.set(count.get() + 1);
                yield_now().await;
                count.set(count.get() + 1);
            })
            .detach();

        assert!(executor.tick());
        assert_eq!(counter.get(), 1);
        assert!(executor.tick());
        assert_eq!(counter.get(), 2);
        assert!(!executor.tick());
    }

    #[test]
    fn block_on_runs_future_to_completion() {
        let (executor, _) = new_executor();
        let counter = Rc::new(Cell::new(0));

        let count = counter.clone();
        executor.block_on(async move {
            for _ in 0..5 {
                count.set(count.get() + 1);
                yield_now().await;
            }
        });

        assert_eq!(counter.get(), 5);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn block_on_drives_nested_spawns() {
        let (executor, _) = new_executor();
        let output = Rc::new(RefCell::new(Vec::new()));

        let spawner = executor.clone();
        let captured = output.clone();
        executor.block_on(async move {
            for value in 0..3 {
                let captured = captured.clone();
                spawner
                    .spawn(async move {
                        yield_now().await;
                        captured.borrow_mut().push(value);
                    })
                    .detach();
            }
        });

        assert_eq!(output.borrow().as_slice(), &[0, 1, 2]);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn block_on_also_drives_previously_spawned_tasks() {
        let (executor, _) = new_executor();
        let output = Rc::new(RefCell::new(Vec::new()));

        let captured = output.clone();
        executor
            .spawn(async move { captured.borrow_mut().push("before") })
            .detach();

        let captured = output.clone();
        executor.block_on(async move { captured.borrow_mut().push("block_on") });

        assert_eq!(output.borrow().as_slice(), &["before", "block_on"]);
    }

    #[test]
    fn block_on_returns_future_output() {
        let (executor, _) = new_executor();

        let value = executor.block_on(async {
            yield_now().await;
            21 * 2
        });

        assert_eq!(value, 42);
    }

    #[test]
    fn block_on_waits_for_wake_from_another_task() {
        let (executor, _) = new_executor();
        let ready = Rc::new(Cell::new(false));
        let slot: Rc<RefCell<Option<Waker>>> = Rc::new(RefCell::new(None));

        let wake_ready = ready.clone();
        let wake_slot = slot.clone();
        executor
            .spawn(async move {
                // Let the blocked future register its waker first.
                yield_now().await;
                wake_ready.set(true);
                if let Some(waker) = wake_slot.borrow_mut().take() {
                    waker.wake();
                }
            })
            .detach();

        let value = executor.block_on(async move {
            WaitFor { ready, slot }.await;
            "done"
        });

        assert_eq!(value, "done");
        assert!(!executor.has_tasks());
    }

    #[test]
    #[should_panic(expected = "deadlock")]
    fn block_on_panics_when_future_can_never_complete() {
        let (executor, _) = new_executor();

        executor.block_on(WaitFor {
            ready: Rc::new(Cell::new(false)),
            slot: Rc::new(RefCell::new(None)),
        });
    }

    #[test]
    fn executor_can_run_futures_borrowing_local_data() {
        let waker = Arc::new(TestWaker::default());
        let mut values = Vec::new();
        let total = Cell::new(0);

        {
            let executor = Executor::new(waker);
            let values = &mut values;
            let total = &total;
            executor.block_on(async move {
                for value in 1..=4 {
                    values.push(value);
                    total.set(total.get() + value);
                    yield_now().await;
                }
            });
        }

        assert_eq!(values, [1, 2, 3, 4]);
        assert_eq!(total.get(), 10);
    }

    #[test]
    fn oneshot_channel_wakes_waiting_task() {
        let (executor, _) = new_executor();
        let (tx, rx) = crate::channel::oneshot::channel::<u32>();
        let received = Rc::new(Cell::new(None));

        let slot = received.clone();
        executor
            .spawn(async move {
                slot.set(rx.await.ok());
            })
            .detach();

        executor.process_tasks(usize::MAX);
        assert!(!executor.has_tasks());
        assert_eq!(received.get(), None);

        tx.send(42).unwrap();
        while executor.has_tasks() {
            executor.process_tasks(usize::MAX);
        }
        assert_eq!(received.get(), Some(42));
    }

    #[test]
    fn many_tasks_complete() {
        let (executor, waker) = new_executor();
        let counter = Rc::new(Cell::new(0usize));

        for _ in 0..1000 {
            let count = counter.clone();
            executor
                .spawn(async move {
                    yield_now().await;
                    count.set(count.get() + 1);
                })
                .detach();
        }

        executor.process_tasks(usize::MAX);
        assert_eq!(counter.get(), 0);
        executor.process_tasks(usize::MAX);
        assert_eq!(counter.get(), 1000);
        assert!(!executor.has_tasks());
        // One wake per spawn and one per yield.
        assert_eq!(waker.count(), 2000);
    }
}
