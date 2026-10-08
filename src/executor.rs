use core::{
    cell::{Cell, RefCell},
    pin::Pin,
    task::{Context, RawWaker, RawWakerVTable, Waker},
};

use alloc::{
    boxed::Box,
    rc::{Rc, Weak},
    vec::Vec,
};

use goerdet::{LocalSpawner, Task};

/// A trait for waking up the event loop when a task is scheduled.
pub trait EventLoopWaker {
    fn wake(&self);
}

// Tasks stay boxed so the executor can keep polling futures that borrow from `'a`.
type BoxFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

struct ExecutorState<'a, T> {
    // The queue stores task cells so a future can reschedule itself safely.
    tasks: RefCell<Vec<Rc<TaskCell<'a, T>>>>,
    waker: Rc<T>,
}

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
    T: EventLoopWaker,
{
    pub fn new(waker: Rc<T>) -> Self {
        Self {
            state: Rc::new(ExecutorState {
                tasks: RefCell::new(Vec::new()),
                waker,
            }),
        }
    }

    /// Process a number of tasks in the executor's queue.
    /// This will run the tasks in the order they were spawned.
    pub fn process_tasks(&self, count: usize) {
        let tasks_to_run = {
            let mut tasks = self.state.tasks.borrow_mut();
            let count = count.min(tasks.len());
            tasks.drain(..count).collect::<Vec<_>>()
        };

        for task in tasks_to_run {
            task.poll();
        }
    }

    /// Check if there are any tasks in the executor's queue.
    pub fn has_tasks(&self) -> bool {
        !self.state.tasks.borrow().is_empty()
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

        output
            .borrow_mut()
            .take()
            .expect("block_on: future is still pending but no tasks are left to drive it (deadlock)")
    }
}

impl<'a, T> LocalSpawner<'a> for Executor<'a, T>
where
    T: EventLoopWaker,
{
    type Task = ExcutorTask<'a, T>;
    fn spawn<F>(&self, task: F) -> Self::Task
    where
        F: Future<Output = ()> + 'a,
    {
        // Each spawned future lives in a task cell so its waker can requeue it later.
        let task = Rc::new(TaskCell {
            future: RefCell::new(Some(Box::pin(task))),
            state: Rc::downgrade(&self.state),
            queued: Cell::new(false),
            detached: Cell::new(false),
            canceled: Cell::new(false),
            completed: Cell::new(false),
        });

        task.schedule();

        ExcutorTask { task: Some(task) }
    }
}

impl<'a, T> crate::spawner::DriverableSpawner<'a> for Executor<'a, T>
where
    T: EventLoopWaker,
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

struct TaskCell<'a, T> {
    // The future is taken out while polling so wake-ups cannot alias the active borrow.
    future: RefCell<Option<BoxFuture<'a>>>,
    // Weak access lets queued wake-ups stop cleanly once the executor state is gone.
    state: Weak<ExecutorState<'a, T>>,
    // Prevents the same task from being enqueued multiple times before it is polled.
    queued: Cell<bool>,
    // Detached tasks keep running even if their handle is dropped.
    detached: Cell<bool>,
    // Canceled tasks drop their future and ignore any later wake-ups.
    canceled: Cell<bool>,
    // Completed tasks should never be queued again.
    completed: Cell<bool>,
}

impl<'a, T> TaskCell<'a, T>
where
    T: EventLoopWaker,
{
    fn schedule(self: &Rc<Self>) {
        if self.queued.get() || self.canceled.get() || self.completed.get() {
            return;
        }

        let Some(state) = self.state.upgrade() else {
            return;
        };

        // Queue the task exactly once until it gets polled again.
        self.queued.set(true);
        state.tasks.borrow_mut().push(self.clone());
        state.waker.wake();
    }

    fn poll(self: Rc<Self>) {
        if self.canceled.get() || self.completed.get() {
            return;
        }

        self.queued.set(false);

        let Some(mut future) = self.future.borrow_mut().take() else {
            return;
        };

        // Build a waker that routes wake-ups back into this executor queue.
        let waker = task_waker(self.clone());
        let mut cx = Context::from_waker(&waker);

        if future.as_mut().poll(&mut cx).is_ready() {
            self.completed.set(true);
            return;
        }

        if self.canceled.get() {
            return;
        }

        *self.future.borrow_mut() = Some(future);
    }

    fn cancel(&self) {
        if self.canceled.replace(true) || self.completed.get() {
            return;
        }

        self.future.borrow_mut().take();
    }
}

struct TaskWakerData {
    ptr: *const (),
    clone_ptr: unsafe fn(*const ()) -> *const (),
    wake_ptr: unsafe fn(*const ()),
    wake_by_ref_ptr: unsafe fn(*const ()),
    drop_ptr: unsafe fn(*const ()),
}

static TASK_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    task_waker_clone,
    task_waker_wake,
    task_waker_wake_by_ref,
    task_waker_drop,
);

fn task_waker<'a, T>(task: Rc<TaskCell<'a, T>>) -> Waker
where
    T: EventLoopWaker,
{
    // Store task-specific function pointers once so the RawWaker callbacks stay monomorphic.
    let data = Box::new(TaskWakerData {
        ptr: Rc::into_raw(task).cast(),
        clone_ptr: clone_task_ptr::<T>,
        wake_ptr: wake_task_ptr::<T>,
        wake_by_ref_ptr: wake_task_ptr_by_ref::<T>,
        drop_ptr: drop_task_ptr::<T>,
    });

    unsafe {
        Waker::from_raw(RawWaker::new(
            Box::into_raw(data).cast(),
            &TASK_WAKER_VTABLE,
        ))
    }
}

unsafe fn task_waker_clone(data: *const ()) -> RawWaker {
    let data = unsafe { &*(data as *const TaskWakerData) };
    let cloned = Box::new(TaskWakerData {
        ptr: unsafe { (data.clone_ptr)(data.ptr) },
        clone_ptr: data.clone_ptr,
        wake_ptr: data.wake_ptr,
        wake_by_ref_ptr: data.wake_by_ref_ptr,
        drop_ptr: data.drop_ptr,
    });

    RawWaker::new(Box::into_raw(cloned).cast(), &TASK_WAKER_VTABLE)
}

unsafe fn task_waker_wake(data: *const ()) {
    let data = unsafe { Box::from_raw(data as *mut TaskWakerData) };
    unsafe { (data.wake_ptr)(data.ptr) };
}

unsafe fn task_waker_wake_by_ref(data: *const ()) {
    let data = unsafe { &*(data as *const TaskWakerData) };
    unsafe { (data.wake_by_ref_ptr)(data.ptr) };
}

unsafe fn task_waker_drop(data: *const ()) {
    let data = unsafe { Box::from_raw(data as *mut TaskWakerData) };
    unsafe { (data.drop_ptr)(data.ptr) };
}

unsafe fn clone_task_ptr<'a, T>(ptr: *const ()) -> *const ()
where
    T: EventLoopWaker,
{
    // Rebuild the Rc temporarily to clone it without changing the original ownership model.
    let task = unsafe { Rc::<TaskCell<'a, T>>::from_raw(ptr.cast()) };
    let cloned = task.clone();
    let _ = Rc::into_raw(task);
    Rc::into_raw(cloned).cast()
}

unsafe fn wake_task_ptr<'a, T>(ptr: *const ())
where
    T: EventLoopWaker,
{
    // `wake` consumes the waker, so the Rc rebuilt from the raw pointer is dropped here.
    let task = unsafe { Rc::<TaskCell<'a, T>>::from_raw(ptr.cast()) };
    task.schedule();
}

unsafe fn wake_task_ptr_by_ref<'a, T>(ptr: *const ())
where
    T: EventLoopWaker,
{
    // `wake_by_ref` must leave ownership unchanged, so the Rc is converted back into a raw pointer.
    let task = unsafe { Rc::<TaskCell<'a, T>>::from_raw(ptr.cast()) };
    task.schedule();
    let _ = Rc::into_raw(task);
}

unsafe fn drop_task_ptr<'a, T>(ptr: *const ())
where
    T: EventLoopWaker,
{
    let _ = unsafe { Rc::<TaskCell<'a, T>>::from_raw(ptr.cast()) };
}

pub struct ExcutorTask<'a, T>
where
    T: EventLoopWaker,
{
    task: Option<Rc<TaskCell<'a, T>>>,
}

impl<'a, T> Task for ExcutorTask<'a, T>
where
    T: EventLoopWaker,
{
    fn detach(mut self) {
        if let Some(task) = self.task.take() {
            task.detached.set(true);
        }
    }
}

impl<'a, T> Drop for ExcutorTask<'a, T>
where
    T: EventLoopWaker,
{
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            if !task.detached.get() {
                task.cancel();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::emitter::{EventEmitter, EventTargetExt};
    use alloc::{rc::Rc, vec, vec::Vec};
    use core::{
        cell::{Cell, RefCell},
        future,
    };

    #[derive(Default)]
    struct TestWaker {
        wake_count: Cell<usize>,
    }

    impl EventLoopWaker for TestWaker {
        fn wake(&self) {
            self.wake_count.set(self.wake_count.get() + 1);
        }
    }

    #[test]
    fn spawn_schedules_task_and_wakes_event_loop() {
        let waker = Rc::new(TestWaker::default());
        let executor = Executor::new(waker.clone());
        let task = executor.spawn(future::ready(()));

        assert_eq!(waker.wake_count.get(), 1);
        assert!(executor.has_tasks());

        task.detach();
        executor.process_tasks(1);
        assert!(!executor.has_tasks());
    }

    #[test]
    fn process_tasks_respects_count_limit() {
        let waker = Rc::new(TestWaker::default());
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
        let waker = Rc::new(TestWaker::default());
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
        let waker = Rc::new(TestWaker::default());
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
        assert_eq!(waker.wake_count.get(), 3);
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

    fn new_executor() -> (Executor<'static, TestWaker>, Rc<TestWaker>) {
        let waker = Rc::new(TestWaker::default());
        (Executor::new(waker.clone()), waker)
    }

    fn queue_len<T>(executor: &Executor<'_, T>) -> usize {
        executor.state.tasks.borrow().len()
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
        assert_eq!(waker.wake_count.get(), 0);
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
            assert_eq!(waker.wake_count.get(), expected);
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
        assert_eq!(waker.wake_count.get(), 1);

        let stored = slot.borrow().clone().unwrap();
        stored.wake_by_ref();
        stored.wake_by_ref();
        stored.clone().wake();

        assert_eq!(queue_len(&executor), 1);
        assert_eq!(waker.wake_count.get(), 2);

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
        assert_eq!(waker.wake_count.get(), 1);

        let stored = slot.borrow_mut().take().unwrap();
        stored.wake_by_ref();
        stored.wake();

        assert!(!executor.has_tasks());
        assert_eq!(waker.wake_count.get(), 1);
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
    fn dropping_executor_releases_pending_futures_without_wakers() {
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

        // The stored waker still owns the task, so the future lives on until the waker goes.
        assert!(!dropped.get());
        slot.borrow_mut().take();
        assert!(dropped.get());
    }

    #[test]
    fn waker_clones_keep_task_alive_and_release_it_when_dropped() {
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

        // Detached and no wakers left: nothing can resume the task, so it is freed.
        assert!(dropped.get());
        drop(executor);
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
        assert_eq!(waker.wake_count.get(), 1);
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
        let waker = Rc::new(TestWaker::default());
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
        assert_eq!(waker.wake_count.get(), 2000);
    }
}
