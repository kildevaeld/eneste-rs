use alloc::fmt;

use crate::{emitter::EventTarget, event::Event};

pub struct ObservableCell<T> {
    value: T,
    event: Event<()>,
}

impl<T: Clone> Clone for ObservableCell<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            event: Event::new(),
        }
    }
}

impl<T: PartialEq> ObservableCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            event: Event::new(),
        }
    }

    pub fn get(&self) -> &T {
        // SAFETY: We ensure exclusive access through the event system.
        &self.value
    }

    pub fn set(&mut self, value: T) {
        // SAFETY: We ensure exclusive access through the event system.
        if self.value != value {
            self.value = value;
            self.event.notify(usize::MAX);
        }
    }
}

impl<T> EventTarget<()> for ObservableCell<T> {
    type Stream = crate::event::EventStream<()>;

    fn subscribe(&self) -> Self::Stream {
        self.event.stream()
    }
}

// pub struct WeakObservableCell<T>(Weak<Inner<T>>);

impl<T: fmt::Debug> fmt::Debug for ObservableCell<T>
where
    T: Clone + PartialEq,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservableCell")
            .field("value", &self.get())
            .finish()
    }
}

impl<T: PartialEq> PartialEq for ObservableCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

// impl<T> Clone for WeakObservableCell<T> {
//     fn clone(&self) -> Self {
//         Self(self.0.clone())
//     }
// }

// impl<T> Upgrade for WeakObservableCell<T> {
//     type Target = ObservableCell<T>;

//     fn upgrade(&self) -> Option<Self::Target> {
//         self.0.upgrade().map(ObservableCell)
//     }
// }

// impl<T> Downgrade for ObservableCell<T> {
//     type Target = WeakObservableCell<T>;

//     fn downgrade(&self) -> Self::Target {
//         WeakObservableCell(Rc::downgrade(&self.0))
//     }
// }

// impl<T> Downgrade for WeakObservableCell<T> {
//     type Target = WeakObservableCell<T>;

//     fn downgrade(&self) -> Self::Target {
//         WeakObservableCell(self.0.clone())
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    use core::{
        pin::Pin,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };
    use futures_core::Stream;

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

    fn poll_next_once<S: Stream>(stream: Pin<&mut S>) -> Poll<Option<S::Item>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        stream.poll_next(&mut cx)
    }

    #[test]
    fn new_starts_with_initial_value() {
        let cell = ObservableCell::new(42);
        assert_eq!(cell.get(), &42);
    }

    #[test]
    fn set_updates_stored_value() {
        let mut cell = ObservableCell::new(10);
        cell.set(20);
        assert_eq!(cell.get(), &20);
    }

    #[test]
    fn subscriber_is_pending_until_value_changes() {
        let mut cell = ObservableCell::new(0);
        let mut stream = cell.subscribe();

        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));

        cell.set(1);

        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Ready(Some(()))
        ));
    }

    #[test]
    fn multiple_subscribers_are_notified_on_change() {
        let mut cell = ObservableCell::new(0);
        let mut first = cell.subscribe();
        let mut second = cell.subscribe();

        assert!(matches!(
            poll_next_once(Pin::new(&mut first)),
            Poll::Pending
        ));
        assert!(matches!(
            poll_next_once(Pin::new(&mut second)),
            Poll::Pending
        ));

        cell.set(1);

        assert!(matches!(
            poll_next_once(Pin::new(&mut first)),
            Poll::Ready(Some(()))
        ));
        assert!(matches!(
            poll_next_once(Pin::new(&mut second)),
            Poll::Ready(Some(()))
        ));
    }

    #[test]
    fn repeated_changes_notify_same_subscriber() {
        let mut cell = ObservableCell::new(0);
        let mut stream = cell.subscribe();

        cell.set(1);
        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Ready(Some(()))
        ));

        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));

        cell.set(2);
        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Ready(Some(()))
        ));
    }

    #[test]
    fn setting_same_value_does_not_notify() {
        let mut cell = ObservableCell::new(5);
        let mut stream = cell.subscribe();

        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));

        cell.set(5);

        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));
    }
}
