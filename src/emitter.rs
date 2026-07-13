use core::{
    pin::Pin,
    task::{Context, Poll},
};

use alloc::rc::{Rc, Weak};
use futures_core::Stream;
use pin_project_lite::pin_project;

use crate::{
    Downgrade,
    event::{Event, EventListener, NotificationExt},
    spawner::Spawner,
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
                EmitterStreamProj::Listening { listener } => match listener.poll(cx) {
                    Poll::Ready(data) => {
                        *this.state = EmitterStreamState::Next;
                        return Poll::Ready(Some(data));
                    }
                    Poll::Pending => return Poll::Pending,
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
        S: Spawner<'a>,
        F: FnMut(T) -> bool + 'a,
        Self::Stream: 'a,
    {
        spawner.spawn(Listener::new(self.subscribe(), map))
    }

    fn listen_with<'a, F, W, S>(&self, spawner: &S, value: W, mut map: F) -> S::Task
    where
        S: Spawner<'a>,
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
