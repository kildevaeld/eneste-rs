use core::cell::Cell;

use alloc::{
    fmt,
    rc::{Rc, Weak},
};

use crate::{Downgrade, Upgrade, emitter::EventTarget, event::Event};

pub struct ObservableCell<T> {
    value: Rc<(Cell<T>, Event<()>)>,
}

impl<T: Copy> Clone for ObservableCell<T> {
    fn clone(&self) -> Self {
        Self {
            value: Rc::new((Cell::new(self.value.0.get()), Event::new())),
        }
    }
}

impl<T: PartialEq + Copy> ObservableCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: Rc::new((Cell::new(value), Event::new())),
        }
    }

    pub fn get(&self) -> T {
        // SAFETY: We ensure exclusive access through the event system.
        self.value.0.get()
    }

    pub fn set(&mut self, value: T) {
        // SAFETY: We ensure exclusive access through the event system.
        if self.value.0.get() != value {
            self.value.0.set(value);
            self.value.1.notify(usize::MAX);
        }
    }
}

impl<T> EventTarget<()> for ObservableCell<T> {
    type Stream = crate::event::EventStream<()>;

    fn subscribe(&self) -> Self::Stream {
        self.value.1.stream()
    }
}

impl<T: fmt::Debug + Copy> fmt::Debug for ObservableCell<T>
where
    T: Clone + PartialEq,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservableCell")
            .field("value", &self.get())
            .finish()
    }
}

impl<T: PartialEq + Copy> PartialEq for ObservableCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<T> Eq for ObservableCell<T> where T: Eq + Copy {}

impl<T> Downgrade for ObservableCell<T> {
    type Target = WeakObservableCell<T>;

    fn downgrade(&self) -> Self::Target {
        WeakObservableCell(Rc::downgrade(&self.value))
    }
}

pub struct WeakObservableCell<T>(Weak<(Cell<T>, Event<()>)>);

impl<T> Clone for WeakObservableCell<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Upgrade for WeakObservableCell<T> {
    type Target = ObservableCell<T>;

    fn upgrade(&self) -> Option<Self::Target> {
        self.0.upgrade().map(|value| ObservableCell { value })
    }
}

impl<T> Downgrade for WeakObservableCell<T> {
    type Target = WeakObservableCell<T>;

    fn downgrade(&self) -> Self::Target {
        WeakObservableCell(self.0.clone())
    }
}

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
        assert_eq!(cell.get(), 42);
    }

    #[test]
    fn set_updates_stored_value() {
        let mut cell = ObservableCell::new(10);
        cell.set(20);
        assert_eq!(cell.get(), 20);
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

#[cfg(test)]
mod behavior_tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_next_once, poll_next_with};
    use alloc::format;
    use core::{pin::Pin, task::Poll};

    #[test]
    fn clone_copies_the_value_but_is_independent() {
        let mut original = ObservableCell::new(1);
        let mut copy = original.clone();

        copy.set(2);
        assert_eq!(original.get(), 1);
        assert_eq!(copy.get(), 2);

        original.set(3);
        assert_eq!(copy.get(), 2);
    }

    #[test]
    fn clone_does_not_share_subscribers() {
        let original = ObservableCell::new(1);
        let mut copy = original.clone();
        let mut original_stream = original.subscribe();
        let mut copy_stream = copy.subscribe();
        let _ = poll_next_once(Pin::new(&mut original_stream));
        let _ = poll_next_once(Pin::new(&mut copy_stream));

        copy.set(2);

        assert_eq!(
            poll_next_once(Pin::new(&mut copy_stream)),
            Poll::Ready(Some(()))
        );
        assert!(poll_next_once(Pin::new(&mut original_stream)).is_pending());
    }

    #[test]
    fn change_wakes_waiting_subscriber() {
        let mut cell = ObservableCell::new(false);
        let mut stream = cell.subscribe();
        let (waker, wakes) = counting_waker();

        assert!(poll_next_with(Pin::new(&mut stream), &waker).is_pending());
        cell.set(true);

        assert_eq!(wakes.count(), 1);
    }

    #[test]
    fn works_with_non_integer_copy_types() {
        let mut cell = ObservableCell::new('a');
        cell.set('b');
        assert_eq!(cell.get(), 'b');

        let mut cell = ObservableCell::new((1u8, true));
        cell.set((2, false));
        assert_eq!(cell.get(), (2, false));
    }

    #[test]
    fn equality_compares_current_values() {
        let a = ObservableCell::new(1);
        let mut b = ObservableCell::new(1);
        assert!(a == b);

        b.set(2);
        assert!(a != b);
    }

    #[test]
    fn debug_shows_the_value() {
        let cell = ObservableCell::new(7);
        let output = format!("{cell:?}");
        assert!(output.contains("value: 7"), "unexpected output: {output}");
    }

    #[test]
    fn weak_upgrade_shares_state_with_the_original() {
        let mut cell = ObservableCell::new(1);
        let weak = cell.downgrade();

        let mut strong = weak.upgrade().expect("cell is alive");
        strong.set(5);
        assert_eq!(cell.get(), 5);

        cell.set(6);
        assert_eq!(strong.get(), 6);
    }

    #[test]
    fn weak_upgrade_shares_subscribers() {
        let cell = ObservableCell::new(1);
        let weak = cell.downgrade();
        let mut stream = cell.subscribe();
        let _ = poll_next_once(Pin::new(&mut stream));

        weak.upgrade().unwrap().set(2);

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn weak_fails_to_upgrade_after_the_cell_is_dropped() {
        let cell = ObservableCell::new(1);
        let weak = cell.downgrade();
        let weak_clone = weak.clone();
        drop(cell);

        assert!(weak.upgrade().is_none());
        assert!(weak_clone.upgrade().is_none());
    }
}
