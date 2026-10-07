use core::{
    pin::Pin,
    task::{Context, Poll},
};

use alloc::rc::{Rc, Weak};
use futures_core::Stream;
use goerdet::LocalSpawner;
use pin_project_lite::pin_project;

use crate::{
    Downgrade,
    event::{Event, EventListener, NotificationExt},
    upgrade::Upgrade,
};

pub trait EventTarget<T> {
    type Stream: Stream<Item = T>;
    fn subscribe(&self) -> Self::Stream;
    fn subscribe_once(&self) -> SubscribeOnce<Self::Stream> {
        SubscribeOnce {
            stream: self.subscribe(),
        }
    }
}

pub trait EventEmitter<T> {
    fn emit(&self, value: T);
}

pub struct Emitter<T> {
    event: Rc<Event<T>>,
}

impl<T: Clone> Emitter<T> {
    pub fn new() -> Self {
        Self {
            event: Rc::new(Event::new_with()),
        }
    }
}

impl<T> EventTarget<T> for Emitter<T> {
    type Stream = EmitterStream<T>;

    fn subscribe(&self) -> Self::Stream {
        EmitterStream {
            listener: Rc::downgrade(&self.event),
            state: EmitterStreamState::Listening {
                listener: self.event.listen(),
            },
        }
    }
}

impl<T: Clone> EventEmitter<T> for Emitter<T> {
    fn emit(&self, value: T) {
        self.event.notify(usize::MAX.with(value));
    }
}

pin_project! {
    #[project = EmitterStreamProj]
    enum EmitterStreamState<T> {
        Listening {
            #[pin]
            listener: EventListener<T>,
        },
        Next,
        Done,
    }
}

pin_project! {
    pub struct EmitterStream<T> {
        #[pin]
        listener: Weak<Event<T>>,
        #[pin]
        state: EmitterStreamState<T>
    }
}

impl<T> Stream for EmitterStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                EmitterStreamProj::Listening { mut listener } => match listener.as_mut().poll(cx) {
                    Poll::Ready(data) => {
                        *this.state = EmitterStreamState::Next;
                        return Poll::Ready(Some(data));
                    }
                    Poll::Pending => {
                        if listener.is_closed() {
                            *this.state = EmitterStreamState::Done;
                            continue;
                        }
                        return Poll::Pending;
                    }
                },
                EmitterStreamProj::Next => {
                    let Some(event) = this.listener.upgrade() else {
                        *this.state = EmitterStreamState::Done;
                        continue;
                    };
                    *this.state = EmitterStreamState::Listening {
                        listener: event.listen(),
                    };
                }
                EmitterStreamProj::Done => return Poll::Ready(None),
            }
        }
    }
}

pin_project! {
    pub struct Listener<T, F> {
        #[pin]
        stream: T,
        map: F,
        done: bool
    }
}

impl<T, F> Listener<T, F> {
    pub fn new(stream: T, map: F) -> Self {
        Self {
            stream,
            map,
            done: false,
        }
    }
}

impl<T, F> Future for Listener<T, F>
where
    T: Stream,
    F: FnMut(T::Item) -> bool,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();

            if *this.done {
                return Poll::Ready(());
            }

            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(data)) => {
                    if !(this.map)(data) {
                        *this.done = true;
                    }
                }
                Poll::Ready(None) => {
                    *this.done = true;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    pub struct SubscribeOnce<T> {
        #[pin]
        stream: T,
    }
}

impl<T> Future for SubscribeOnce<T>
where
    T: Stream,
{
    type Output = T::Item;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(data)) => Poll::Ready(data),
            Poll::Ready(None) => panic!("stream ended before receiving a value"),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub trait EventTargetExt<T>: EventTarget<T> + Sized {
    fn listen<'a, F, S>(&self, spawner: &S, map: F) -> S::Task
    where
        S: LocalSpawner<'a>,
        F: FnMut(T) -> bool + 'a,
        Self::Stream: 'a,
    {
        spawner.spawn(Listener::new(self.subscribe(), map))
    }

    fn listen_with<'a, F, W, S>(&self, spawner: &S, value: &W, mut map: F) -> S::Task
    where
        S: LocalSpawner<'a>,
        W: Downgrade,
        W::Target: Upgrade + 'a,
        F: FnMut(<W::Target as Upgrade>::Target, T) -> bool + 'a,
        Self::Stream: 'a,
    {
        let downgraded_value = value.downgrade();
        self.listen(spawner, move |event| {
            let Some(strong_self) = downgraded_value.upgrade() else {
                return false;
            };

            map(strong_self, event)
        })
    }

    fn listen_with_this<'a, F, S>(&self, spawner: &S, mut map: F) -> S::Task
    where
        Self: Downgrade,
        Self::Target: 'a,
        S: LocalSpawner<'a>,
        F: FnMut(<Self::Target as Upgrade>::Target, T) -> bool + 'a,
        Self::Stream: 'a,
    {
        let downgraded_value = self.downgrade();
        self.listen(spawner, move |event| {
            let Some(strong_self) = downgraded_value.upgrade() else {
                return false;
            };

            map(strong_self, event)
        })
    }

    fn listen_with_this_and<'a, F, W, S>(&self, spawner: &S, value: &W, mut map: F) -> S::Task
    where
        Self: Downgrade,
        Self::Target: 'a,
        W: Downgrade,
        W::Target: Upgrade + 'a,
        S: LocalSpawner<'a>,
        F: FnMut(<Self::Target as Upgrade>::Target, <W::Target as Upgrade>::Target, T) -> bool + 'a,
        Self::Stream: 'a,
    {
        let downgraded_value = value.downgrade();
        self.listen_with_this(spawner, move |this, event| {
            let Some(strong) = downgraded_value.upgrade() else {
                return false;
            };

            map(this, strong, event)
        })
    }
}

impl<T, E> EventTargetExt<T> for E where E: EventTarget<T> {}

#[cfg(all(feature = "executor", test))]
mod tests {
    use super::*;

    use crate::executor::{EventLoopWaker, Executor};
    use alloc::{rc::Rc, vec::Vec};
    use core::{
        cell::{Cell, RefCell},
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

    fn poll_stream_once<S: Stream>(stream: Pin<&mut S>) -> Poll<Option<S::Item>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        stream.poll_next(&mut cx)
    }

    #[test]
    fn emitted_value_reaches_single_subscriber() {
        let emitter = Emitter::new();
        let mut stream = emitter.subscribe();

        assert!(matches!(
            poll_stream_once(Pin::new(&mut stream)),
            Poll::Pending
        ));

        emitter.emit(5usize);

        assert_eq!(
            poll_stream_once(Pin::new(&mut stream)),
            Poll::Ready(Some(5))
        );
    }

    #[test]
    fn emitted_value_is_broadcast_to_all_subscribers() {
        let emitter = Emitter::new();
        let mut first = emitter.subscribe();
        let mut second = emitter.subscribe();

        emitter.emit(8usize);

        assert_eq!(poll_stream_once(Pin::new(&mut first)), Poll::Ready(Some(8)));
        assert_eq!(
            poll_stream_once(Pin::new(&mut second)),
            Poll::Ready(Some(8))
        );
    }

    #[test]
    fn listener_stops_once_callback_returns_false() {
        let emitter = Emitter::new();
        let seen = Rc::new(RefCell::new(Vec::new()));
        let captured = seen.clone();
        let mut listener = Listener::new(emitter.subscribe(), move |value| {
            captured.borrow_mut().push(value);
            value < 2
        });

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut listener).poll(&mut cx),
            Poll::Pending
        ));

        emitter.emit(1usize);
        assert!(matches!(
            Pin::new(&mut listener).poll(&mut cx),
            Poll::Pending
        ));

        emitter.emit(2usize);
        assert_eq!(Pin::new(&mut listener).poll(&mut cx), Poll::Ready(()));
        assert_eq!(seen.borrow().as_slice(), &[1, 2]);
    }

    #[test]
    fn stream_finishes_after_emitter_is_dropped() {
        let emitter = Emitter::new();
        let mut stream = emitter.subscribe();

        emitter.emit(13usize);
        drop(emitter);

        assert_eq!(
            poll_stream_once(Pin::new(&mut stream)),
            Poll::Ready(Some(13))
        );
        assert_eq!(poll_stream_once(Pin::new(&mut stream)), Poll::Ready(None));
    }

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
    fn listen_on_processes_events_and_stops_when_callback_returns_false() {
        let emitter = Emitter::new();
        let waker = Rc::new(TestWaker::default());
        let executor = Executor::new(waker.clone());
        let seen = Rc::new(RefCell::new(Vec::new()));

        let captured = seen.clone();
        let _task = emitter.listen(&executor, move |value| {
            captured.borrow_mut().push(value);
            value < 2
        });

        assert_eq!(waker.wake_count.get(), 1);
        assert!(executor.has_tasks());

        executor.process_tasks(1);
        assert!(!executor.has_tasks());
        assert!(seen.borrow().is_empty());

        emitter.emit(1usize);
        assert_eq!(waker.wake_count.get(), 2);
        assert!(executor.has_tasks());

        executor.process_tasks(1);
        assert_eq!(seen.borrow().as_slice(), &[1]);
        assert!(!executor.has_tasks());

        emitter.emit(2usize);
        assert_eq!(waker.wake_count.get(), 3);
        assert!(executor.has_tasks());

        executor.process_tasks(1);
        assert_eq!(seen.borrow().as_slice(), &[1, 2]);
        assert!(!executor.has_tasks());

        emitter.emit(3usize);
        assert_eq!(waker.wake_count.get(), 3);
        assert!(!executor.has_tasks());
        assert_eq!(seen.borrow().as_slice(), &[1, 2]);
    }
}

/// Feature-independent tests (the `tests` module above needs the `executor` feature).
#[cfg(test)]
mod behavior_tests {
    use super::*;

    use crate::{
        ref_cell::ObservableRefCell,
        test_util::{
            IterStream, TestSpawner, counting_waker, poll_next_once, poll_next_with, poll_once,
        },
    };
    use alloc::{rc::Rc, string::String, vec, vec::Vec};
    use core::{cell::RefCell, pin::pin};

    fn log<T>() -> Rc<RefCell<Vec<T>>> {
        Rc::new(RefCell::new(Vec::new()))
    }

    // ---- Emitter / EmitterStream ----

    #[test]
    fn subscriber_is_pending_until_a_value_is_emitted() {
        let emitter = Emitter::new();
        let mut stream = emitter.subscribe();

        assert!(poll_next_once(Pin::new(&mut stream)).is_pending());

        emitter.emit(1);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(1)));
    }

    #[test]
    fn emit_wakes_waiting_subscriber() {
        let emitter = Emitter::new();
        let mut stream = emitter.subscribe();
        let (waker, wakes) = counting_waker();

        let _ = poll_next_with(Pin::new(&mut stream), &waker);
        emitter.emit("hi");

        assert_eq!(wakes.count(), 1);
    }

    #[test]
    fn every_subscriber_receives_a_clone_of_the_value() {
        let emitter = Emitter::new();
        let mut first = emitter.subscribe();
        let mut second = emitter.subscribe();

        emitter.emit(String::from("shared"));

        assert_eq!(
            poll_next_once(Pin::new(&mut first)),
            Poll::Ready(Some(String::from("shared")))
        );
        assert_eq!(
            poll_next_once(Pin::new(&mut second)),
            Poll::Ready(Some(String::from("shared")))
        );
    }

    #[test]
    fn emit_without_subscribers_is_a_noop() {
        let emitter = Emitter::new();
        emitter.emit(1);

        let mut late = emitter.subscribe();
        assert!(poll_next_once(Pin::new(&mut late)).is_pending());
    }

    #[test]
    fn values_emitted_before_subscribing_are_not_replayed() {
        let emitter = Emitter::new();
        emitter.emit(1);
        let mut stream = emitter.subscribe();
        emitter.emit(2);

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(2)));
    }

    #[test]
    fn stream_yields_successive_values_in_order() {
        let emitter = Emitter::new();
        let mut stream = emitter.subscribe();

        for value in 0..5 {
            emitter.emit(value);
            assert_eq!(
                poll_next_once(Pin::new(&mut stream)),
                Poll::Ready(Some(value))
            );
            assert!(poll_next_once(Pin::new(&mut stream)).is_pending());
        }
    }

    #[test]
    fn dropped_subscriber_does_not_disturb_the_others() {
        let emitter = Emitter::new();
        let dropped = emitter.subscribe();
        let mut alive = emitter.subscribe();
        drop(dropped);

        emitter.emit(7);

        assert_eq!(poll_next_once(Pin::new(&mut alive)), Poll::Ready(Some(7)));
    }

    #[test]
    fn stream_ends_after_emitter_is_dropped() {
        let emitter = Emitter::new();
        let mut stream = emitter.subscribe();

        emitter.emit(1);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(1)));

        drop(emitter);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(None));
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(None));
    }

    #[test]
    fn waiting_stream_ends_when_emitter_is_dropped() {
        let emitter = Emitter::<u8>::new();
        let mut stream = emitter.subscribe();
        assert!(poll_next_once(Pin::new(&mut stream)).is_pending());

        drop(emitter);

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(None));
    }

    // ---- SubscribeOnce ----

    #[test]
    fn subscribe_once_resolves_with_the_first_value() {
        let emitter = Emitter::new();
        let mut once = pin!(emitter.subscribe_once());

        assert!(poll_once(once.as_mut()).is_pending());
        emitter.emit(10);
        assert_eq!(poll_once(once.as_mut()), Poll::Ready(10));
    }

    #[test]
    fn subscribe_once_works_with_block_on() {
        let emitter = Emitter::new();
        let once = emitter.subscribe_once();
        emitter.emit(String::from("value"));

        assert_eq!(pollster::block_on(once), "value");
    }

    #[test]
    #[should_panic(expected = "stream ended before receiving a value")]
    fn subscribe_once_panics_if_the_stream_ends_early() {
        let once = SubscribeOnce {
            stream: IterStream(core::iter::empty::<u8>()),
        };
        pollster::block_on(once);
    }

    #[test]
    fn subscribe_once_works_for_other_event_targets() {
        let cell = ObservableRefCell::new(0);
        let mut once = pin!(cell.subscribe_once());
        assert!(poll_once(once.as_mut()).is_pending());

        *cell.borrow_mut() = 1;

        assert_eq!(poll_once(once.as_mut()), Poll::Ready(()));
    }

    // ---- Listener ----

    #[test]
    fn listener_invokes_map_for_each_item_and_finishes_at_stream_end() {
        let seen = log();
        let sink = seen.clone();
        let mut listener = pin!(Listener::new(IterStream(1..=3), move |v| {
            sink.borrow_mut().push(v);
            true
        }));

        assert_eq!(poll_once(listener.as_mut()), Poll::Ready(()));
        assert_eq!(*seen.borrow(), [1, 2, 3]);
    }

    #[test]
    fn listener_stops_when_map_returns_false() {
        let seen = log();
        let sink = seen.clone();
        let mut listener = pin!(Listener::new(IterStream(1..=10), move |v| {
            sink.borrow_mut().push(v);
            v < 3
        }));

        assert_eq!(poll_once(listener.as_mut()), Poll::Ready(()));
        assert_eq!(*seen.borrow(), [1, 2, 3]);
    }

    #[test]
    fn listener_does_not_call_map_again_after_finishing() {
        let seen = log();
        let sink = seen.clone();
        let mut listener = pin!(Listener::new(IterStream(1..=10), move |v| {
            sink.borrow_mut().push(v);
            false
        }));

        assert_eq!(poll_once(listener.as_mut()), Poll::Ready(()));
        assert_eq!(poll_once(listener.as_mut()), Poll::Ready(()));
        assert_eq!(*seen.borrow(), [1]);
    }

    #[test]
    fn listener_is_pending_while_the_stream_is_idle() {
        let emitter = Emitter::new();
        let seen = log();
        let sink = seen.clone();
        let mut listener = pin!(Listener::new(emitter.subscribe(), move |v: i32| {
            sink.borrow_mut().push(v);
            true
        }));

        assert!(poll_once(listener.as_mut()).is_pending());

        emitter.emit(1);
        assert!(poll_once(listener.as_mut()).is_pending());
        emitter.emit(2);
        assert!(poll_once(listener.as_mut()).is_pending());

        assert_eq!(*seen.borrow(), [1, 2]);
    }

    #[test]
    fn listener_finishes_when_the_emitter_goes_away() {
        let emitter = Emitter::new();
        let mut listener = pin!(Listener::new(emitter.subscribe(), |_: i32| true));

        emitter.emit(1);
        assert!(poll_once(listener.as_mut()).is_pending());

        drop(emitter);
        assert_eq!(poll_once(listener.as_mut()), Poll::Ready(()));
    }

    // ---- EventTargetExt ----

    #[test]
    fn listen_runs_callback_for_each_event_until_it_returns_false() {
        let spawner = TestSpawner::new();
        let emitter = Emitter::new();
        let seen = log();
        let sink = seen.clone();

        emitter.listen(&spawner, move |v: i32| {
            sink.borrow_mut().push(v);
            v < 2
        });

        spawner.run();
        assert_eq!(spawner.pending(), 1);

        emitter.emit(1);
        spawner.run();
        assert_eq!(spawner.pending(), 1);

        emitter.emit(2);
        spawner.run();
        assert_eq!(spawner.pending(), 0);

        emitter.emit(3);
        spawner.run();
        assert_eq!(*seen.borrow(), [1, 2]);
    }

    #[test]
    fn listen_task_completes_when_emitter_is_dropped() {
        let spawner = TestSpawner::new();
        let emitter = Emitter::new();

        emitter.listen(&spawner, |_: i32| true);
        spawner.run();
        emitter.emit(1);
        spawner.run();
        assert_eq!(spawner.pending(), 1);

        drop(emitter);
        spawner.run();
        assert_eq!(spawner.pending(), 0);
    }

    #[test]
    fn listen_works_on_event_targets_other_than_emitter() {
        let spawner = TestSpawner::new();
        let cell = ObservableRefCell::new(0);
        let count = Rc::new(core::cell::Cell::new(0));
        let counter = count.clone();

        cell.listen(&spawner, move |()| {
            counter.set(counter.get() + 1);
            true
        });
        spawner.run();

        *cell.borrow_mut() = 1;
        spawner.run();
        *cell.borrow_mut() = 2;
        spawner.run();

        assert_eq!(count.get(), 2);
    }

    #[test]
    fn listen_with_passes_the_upgraded_value_to_the_callback() {
        let spawner = TestSpawner::new();
        let emitter = Emitter::new();
        let owner = Rc::new(RefCell::new(Vec::<i32>::new()));

        emitter.listen_with(&spawner, &owner, |owner, value: i32| {
            owner.borrow_mut().push(value);
            true
        });
        spawner.run();

        emitter.emit(1);
        spawner.run();
        emitter.emit(2);
        spawner.run();

        assert_eq!(*owner.borrow(), [1, 2]);
        assert_eq!(spawner.pending(), 1);
    }

    #[test]
    fn listen_with_stops_once_the_value_is_dropped() {
        let spawner = TestSpawner::new();
        let emitter = Emitter::new();
        let owner = Rc::new(RefCell::new(Vec::<i32>::new()));
        let calls = Rc::new(core::cell::Cell::new(0));
        let counter = calls.clone();

        emitter.listen_with(&spawner, &owner, move |owner, value: i32| {
            counter.set(counter.get() + 1);
            owner.borrow_mut().push(value);
            true
        });
        spawner.run();

        emitter.emit(1);
        spawner.run();
        assert_eq!(calls.get(), 1);

        drop(owner);
        emitter.emit(2);
        spawner.run();

        assert_eq!(calls.get(), 1, "callback must not run for a dead owner");
        assert_eq!(spawner.pending(), 0);
    }

    #[test]
    fn listen_with_does_not_keep_the_value_alive() {
        let spawner = TestSpawner::new();
        let emitter = Emitter::<i32>::new();
        let owner = Rc::new(0);

        emitter.listen_with(&spawner, &owner, |_, _| true);
        spawner.run();

        assert_eq!(Rc::strong_count(&owner), 1);
    }

    #[test]
    fn listen_with_this_passes_the_upgraded_target() {
        let spawner = TestSpawner::new();
        let cell = ObservableRefCell::new(0);
        let observed = log();
        let sink = observed.clone();

        cell.listen_with_this(&spawner, move |cell, ()| {
            sink.borrow_mut().push(*cell.borrow());
            true
        });
        spawner.run();

        *cell.borrow_mut() = 5;
        spawner.run();
        *cell.borrow_mut() = 6;
        spawner.run();

        assert_eq!(*observed.borrow(), [5, 6]);
    }

    #[test]
    fn listen_with_this_and_passes_both_upgraded_values() {
        let spawner = TestSpawner::new();
        let cell = ObservableRefCell::new(1);
        let other = Rc::new(RefCell::new(String::new()));
        let observed = log();
        let sink = observed.clone();

        cell.listen_with_this_and(&spawner, &other, move |cell, other, ()| {
            other.borrow_mut().push('x');
            sink.borrow_mut()
                .push((*cell.borrow(), other.borrow().len()));
            true
        });
        spawner.run();

        *cell.borrow_mut() = 2;
        spawner.run();
        *cell.borrow_mut() = 3;
        spawner.run();

        assert_eq!(*observed.borrow(), [(2, 1), (3, 2)]);
    }

    #[test]
    fn listen_with_this_and_stops_once_the_other_value_is_dropped() {
        let spawner = TestSpawner::new();
        let cell = ObservableRefCell::new(1);
        let other = Rc::new(());
        let calls = Rc::new(core::cell::Cell::new(0));
        let counter = calls.clone();

        cell.listen_with_this_and(&spawner, &other, move |_, _, ()| {
            counter.set(counter.get() + 1);
            true
        });
        spawner.run();

        *cell.borrow_mut() = 2;
        spawner.run();
        assert_eq!(calls.get(), 1);

        drop(other);
        *cell.borrow_mut() = 3;
        spawner.run();

        assert_eq!(calls.get(), 1);
        assert_eq!(spawner.pending(), 0);
    }

    #[test]
    fn multiple_listeners_all_observe_the_same_events() {
        let spawner = TestSpawner::new();
        let emitter = Emitter::new();
        let a = log();
        let b = log();
        let (sink_a, sink_b) = (a.clone(), b.clone());

        emitter.listen(&spawner, move |v: u8| {
            sink_a.borrow_mut().push(v);
            true
        });
        emitter.listen(&spawner, move |v: u8| {
            sink_b.borrow_mut().push(v * 10);
            true
        });
        spawner.run();

        emitter.emit(1);
        spawner.run();
        emitter.emit(2);
        spawner.run();

        assert_eq!(*a.borrow(), vec![1, 2]);
        assert_eq!(*b.borrow(), vec![10, 20]);
    }
}
