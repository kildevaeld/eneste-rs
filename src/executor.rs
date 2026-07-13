use core::{
    cell::{Cell, RefCell},
    pin::Pin,
    task::{Context, RawWaker, RawWakerVTable, Waker},
    usize,
};

use alloc::{
    boxed::Box,
    rc::{Rc, Weak},
    vec::Vec,
};

use crate::spawner::{SpawnTask, Spawner};

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

    pub fn block_on<'b: 'a, F>(&self, future: F)
    where
        F: Future<Output = ()> + 'b,
    {
        let task = self.spawn(future);
        task.detach();

        while self.has_tasks() {
            self.process_tasks(usize::MAX);
        }
    }
}

impl<'a, T> Spawner<'a> for Executor<'a, T>
where
    T: EventLoopWaker,
{
    type Task = ExcutorTask<'a, T>;
    fn spawn<'b: 'a, F>(&self, task: F) -> Self::Task
    where
        F: Future<Output = ()> + 'b,
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

impl<'a, T> SpawnTask for ExcutorTask<'a, T>
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
}
