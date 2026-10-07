use core::{
    cell::{Ref, RefCell, RefMut},
    fmt,
};

use alloc::rc::Rc;

use crate::{Downgrade, Upgrade, emitter::EventTarget, event::Event};

struct Inner<T> {
    value: RefCell<T>,
    event: Event<()>,
}

pub struct ObservableRefCell<T>(Rc<Inner<T>>);

impl<T: Clone> Clone for ObservableRefCell<T> {
    fn clone(&self) -> Self {
        Self(Rc::new(Inner {
            value: RefCell::new(self.0.value.borrow().clone()),
            event: Event::new(),
        }))
    }
}

impl<T> ObservableRefCell<T> {
    pub fn new(value: T) -> Self {
        Self(Rc::new(Inner {
            value: RefCell::new(value),
            event: Event::new(),
        }))
    }

    pub fn borrow<'a>(&'a self) -> CellRef<'a, T> {
        CellRef(self.0.value.borrow())
    }

    pub fn borrow_mut<'a>(&'a self) -> CellRefMut<'a, T> {
        CellRefMut {
            value: self.0.value.borrow_mut(),
            event: &self.0.event,
            mutated: false,
        }
    }

    pub fn try_borrow<'a>(&'a self) -> Option<CellRef<'a, T>> {
        self.0.value.try_borrow().ok().map(CellRef)
    }

    pub fn try_borrow_mut<'a>(&'a self) -> Option<CellRefMut<'a, T>> {
        self.0.value.try_borrow_mut().ok().map(|value| CellRefMut {
            value,
            event: &self.0.event,
            mutated: false,
        })
    }
}

impl<T: fmt::Debug> fmt::Debug for ObservableRefCell<T>
where
    T: Clone + PartialEq,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservableCell")
            .field("value", &*self.borrow())
            .finish()
    }
}

impl<T: PartialEq> PartialEq for ObservableRefCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.borrow().as_ref() == other.borrow().as_ref()
    }
}

impl<T> Eq for ObservableRefCell<T> where T: Eq {}

impl<T> EventTarget<()> for ObservableRefCell<T> {
    type Stream = crate::event::EventStream<()>;

    fn subscribe(&self) -> Self::Stream {
        self.0.event.stream()
    }
}

impl<T> Downgrade for ObservableRefCell<T> {
    type Target = WeakObservableRefCell<T>;

    fn downgrade(&self) -> Self::Target {
        WeakObservableRefCell(Rc::downgrade(&self.0))
    }
}

pub struct CellRef<'a, T>(Ref<'a, T>);

impl<'a, T> CellRef<'a, T> {
    pub fn get(&self) -> &T {
        &self.0
    }
}

impl<'a, T> core::ops::Deref for CellRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, T> AsRef<T> for CellRef<'a, T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

pub struct CellRefMut<'a, T> {
    value: RefMut<'a, T>,
    event: &'a Event<()>,
    mutated: bool,
}

impl<'a, T> core::ops::Deref for CellRefMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, T> core::ops::DerefMut for CellRefMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mutated = true;
        &mut self.value
    }
}

impl<'a, T> Drop for CellRefMut<'a, T> {
    fn drop(&mut self) {
        if self.mutated {
            self.event.notify(usize::MAX);
        }
    }
}

impl<'a, T> core::borrow::Borrow<T> for CellRefMut<'a, T> {
    fn borrow(&self) -> &T {
        &self.value
    }
}

impl<'a, T> core::borrow::BorrowMut<T> for CellRefMut<'a, T> {
    fn borrow_mut(&mut self) -> &mut T {
        self.mutated = true;
        &mut self.value
    }
}

pub struct WeakObservableRefCell<T>(alloc::rc::Weak<Inner<T>>);

impl<T> Clone for WeakObservableRefCell<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Upgrade for WeakObservableRefCell<T> {
    type Target = ObservableRefCell<T>;

    fn upgrade(&self) -> Option<Self::Target> {
        self.0.upgrade().map(|inner| ObservableRefCell(inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_next_once, poll_next_with};
    use alloc::{format, string::String, vec, vec::Vec};
    use core::{borrow::BorrowMut, pin::Pin, task::Poll};

    #[test]
    fn new_cell_exposes_initial_value() {
        let cell = ObservableRefCell::new(10);
        assert_eq!(*cell.borrow(), 10);
        assert_eq!(*cell.borrow().get(), 10);
        assert_eq!(*cell.borrow().as_ref(), 10);
    }

    #[test]
    fn borrow_mut_updates_the_value() {
        let cell = ObservableRefCell::new(String::from("a"));
        cell.borrow_mut().push('b');
        assert_eq!(&*cell.borrow(), "ab");
    }

    #[test]
    fn multiple_shared_borrows_coexist() {
        let cell = ObservableRefCell::new(1);
        let a = cell.borrow();
        let b = cell.borrow();
        assert_eq!(*a + *b, 2);
        assert!(cell.try_borrow().is_some());
    }

    #[test]
    fn try_borrow_fails_while_mutably_borrowed() {
        let cell = ObservableRefCell::new(1);
        let guard = cell.borrow_mut();

        assert!(cell.try_borrow().is_none());
        assert!(cell.try_borrow_mut().is_none());

        drop(guard);
        assert!(cell.try_borrow().is_some());
        assert!(cell.try_borrow_mut().is_some());
    }

    #[test]
    fn try_borrow_mut_fails_while_borrowed() {
        let cell = ObservableRefCell::new(1);
        let guard = cell.borrow();

        assert!(cell.try_borrow_mut().is_none());
        assert!(cell.try_borrow().is_some());

        drop(guard);
        assert!(cell.try_borrow_mut().is_some());
    }

    #[test]
    #[should_panic]
    fn borrow_mut_panics_while_borrowed() {
        let cell = ObservableRefCell::new(1);
        let _shared = cell.borrow();
        let _unique = cell.borrow_mut();
    }

    #[test]
    #[should_panic]
    fn borrow_panics_while_mutably_borrowed() {
        let cell = ObservableRefCell::new(1);
        let _unique = cell.borrow_mut();
        let _shared = cell.borrow();
    }

    #[test]
    fn try_borrow_mut_guard_mutates_and_notifies() {
        let cell = ObservableRefCell::new(1);
        let mut stream = cell.subscribe();
        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));

        *cell.try_borrow_mut().expect("not borrowed") = 2;

        assert_eq!(*cell.borrow(), 2);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn mutation_notifies_subscribers_when_guard_drops() {
        let cell = ObservableRefCell::new(0);
        let mut stream = cell.subscribe();
        let (waker, wakes) = counting_waker();

        assert!(matches!(
            poll_next_with(Pin::new(&mut stream), &waker),
            Poll::Pending
        ));

        {
            let mut guard = cell.borrow_mut();
            *guard = 5;
            // Notification is deferred until the guard is released.
            assert_eq!(wakes.count(), 0);
        }

        assert_eq!(wakes.count(), 1);
        assert_eq!(
            poll_next_with(Pin::new(&mut stream), &waker),
            Poll::Ready(Some(()))
        );
    }

    #[test]
    fn read_only_access_through_borrow_mut_does_not_notify() {
        let cell = ObservableRefCell::new(3);
        let mut stream = cell.subscribe();
        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));

        {
            let guard = cell.borrow_mut();
            assert_eq!(*guard, 3);
        }

        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));
    }

    #[test]
    fn plain_shared_borrow_does_not_notify() {
        let cell = ObservableRefCell::new(3);
        let mut stream = cell.subscribe();
        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));

        let _ = *cell.borrow();

        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));
    }

    #[test]
    fn notification_is_sent_even_when_value_is_unchanged() {
        let cell = ObservableRefCell::new(3);
        let mut stream = cell.subscribe();
        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));

        *cell.borrow_mut() = 3;

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn multiple_mutations_in_one_guard_notify_once() {
        let cell = ObservableRefCell::new(Vec::<u8>::new());
        let mut stream = cell.subscribe();
        let (waker, wakes) = counting_waker();
        let _ = poll_next_with(Pin::new(&mut stream), &waker);

        {
            let mut guard = cell.borrow_mut();
            guard.push(1);
            guard.push(2);
            guard.push(3);
        }

        assert_eq!(wakes.count(), 1);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));
    }

    #[test]
    fn every_subscriber_is_notified() {
        let cell = ObservableRefCell::new(0);
        let mut first = cell.subscribe();
        let mut second = cell.subscribe();

        *cell.borrow_mut() = 1;

        assert_eq!(poll_next_once(Pin::new(&mut first)), Poll::Ready(Some(())));
        assert_eq!(poll_next_once(Pin::new(&mut second)), Poll::Ready(Some(())));
    }

    #[test]
    fn subscriber_receives_successive_mutations() {
        let cell = ObservableRefCell::new(0);
        let mut stream = cell.subscribe();

        for i in 1..=3 {
            *cell.borrow_mut() = i;
            assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
            assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));
        }
    }

    #[test]
    fn subscribe_once_resolves_on_first_mutation() {
        let cell = ObservableRefCell::new(0);
        let fut = cell.subscribe_once();
        let mut fut = core::pin::pin!(fut);
        assert!(crate::test_util::poll_once(fut.as_mut()).is_pending());

        *cell.borrow_mut() = 1;

        assert_eq!(crate::test_util::poll_once(fut.as_mut()), Poll::Ready(()));
    }

    #[test]
    fn deref_mut_marks_guard_as_mutated() {
        let cell = ObservableRefCell::new(vec![1]);
        let mut stream = cell.subscribe();
        let _ = poll_next_once(Pin::new(&mut stream));

        {
            let mut guard = cell.borrow_mut();
            guard.push(2);
        }

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn borrow_trait_on_guard_does_not_notify_but_borrow_mut_trait_does() {
        let cell = ObservableRefCell::new(1);
        let mut stream = cell.subscribe();
        let _ = poll_next_once(Pin::new(&mut stream));

        {
            let guard = cell.borrow_mut();
            let value: &i32 = core::borrow::Borrow::borrow(&guard);
            assert_eq!(*value, 1);
        }
        assert!(matches!(poll_next_once(Pin::new(&mut stream)), Poll::Pending));

        {
            let mut guard = cell.borrow_mut();
            *BorrowMut::<i32>::borrow_mut(&mut guard) = 2;
        }
        assert_eq!(*cell.borrow(), 2);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn clone_creates_an_independent_cell() {
        let original = ObservableRefCell::new(vec![1, 2]);
        let copy = original.clone();

        copy.borrow_mut().push(3);

        assert_eq!(*original.borrow(), [1, 2]);
        assert_eq!(*copy.borrow(), [1, 2, 3]);
    }

    #[test]
    fn clone_does_not_share_subscribers() {
        let original = ObservableRefCell::new(0);
        let copy = original.clone();
        let mut original_stream = original.subscribe();
        let mut copy_stream = copy.subscribe();
        let _ = poll_next_once(Pin::new(&mut original_stream));
        let _ = poll_next_once(Pin::new(&mut copy_stream));

        *copy.borrow_mut() = 1;

        assert_eq!(poll_next_once(Pin::new(&mut copy_stream)), Poll::Ready(Some(())));
        assert!(matches!(
            poll_next_once(Pin::new(&mut original_stream)),
            Poll::Pending
        ));
    }

    #[test]
    fn equality_compares_contents() {
        let a = ObservableRefCell::new(1);
        let b = ObservableRefCell::new(1);
        let c = ObservableRefCell::new(2);

        assert!(a == b);
        assert!(a != c);
    }

    #[test]
    fn debug_shows_the_value() {
        let cell = ObservableRefCell::new(5);
        let output = format!("{cell:?}");
        assert!(output.contains("value: 5"), "unexpected output: {output}");
    }

    #[test]
    fn weak_upgrade_returns_the_same_cell() {
        let cell = ObservableRefCell::new(1);
        let weak = cell.downgrade();

        let strong = weak.upgrade().expect("cell is alive");
        *strong.borrow_mut() = 9;

        assert_eq!(*cell.borrow(), 9);
    }

    #[test]
    fn weak_upgrade_shares_subscribers() {
        let cell = ObservableRefCell::new(1);
        let weak = cell.downgrade();
        let mut stream = cell.subscribe();
        let _ = poll_next_once(Pin::new(&mut stream));

        *weak.upgrade().unwrap().borrow_mut() = 2;

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn weak_fails_to_upgrade_after_all_strong_handles_are_gone() {
        let cell = ObservableRefCell::new(1);
        let weak = cell.downgrade();
        let weak_clone = weak.clone();
        drop(cell);

        assert!(weak.upgrade().is_none());
        assert!(weak_clone.upgrade().is_none());
    }

    #[test]
    fn upgraded_cell_keeps_the_value_alive() {
        let cell = ObservableRefCell::new(1);
        let weak = cell.downgrade();
        let strong = weak.upgrade().unwrap();
        drop(cell);

        assert!(weak.upgrade().is_some());
        drop(strong);
        assert!(weak.upgrade().is_none());
    }
}
