use core::{
    cell::RefCell,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloc::{
    collections::btree_map::BTreeMap,
    rc::{Rc, Weak},
};
use futures_core::{FusedFuture, Stream};
use pin_project_lite::pin_project;

#[derive(Debug)]
pub struct Event<T = ()> {
    inner: Rc<RefCell<Inner<T>>>,
}

impl Default for Event<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl Event<()> {
    pub fn new() -> Self {
        Self::new_with()
    }
}

impl<T> Event<T> {
    pub fn new_with() -> Self {
        Event {
            inner: Rc::new(RefCell::new(Inner {
                listeners: BTreeMap::new(),
                next_id: 0,
                notified: 0,
                closed: false,
                value: PhantomData,
            })),
        }
    }

    pub fn notify<V>(&self, value: V)
    where
        V: Notification<Tag = T>,
    {
        if value.is_additional() {
            self.notify_additional(value);
        } else {
            self.notify_inner(value);
        }
    }

    pub fn listen(&self) -> EventListener<T> {
        let id = self.inner.borrow_mut().listen();
        EventListener {
            id,
            event: Rc::clone(&self.inner),
        }
    }

    /// Notifies a number of active listeners.
    ///
    /// The number of notified listeners is determined by `n`:
    /// - If `n` is `usize::MAX`, all active listeners are notified.
    /// - Otherwise, `n` active listeners are notified.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use eneste::event::Event;
    ///
    /// let event = Event::new();
    ///
    /// // Notify all listeners.
    /// event.notify(usize::MAX);
    ///
    /// // Notify exactly 5 listeners.
    /// event.notify(5);
    /// ```
    fn notify_inner<N: Notification<Tag = T>>(&self, notification: N) {
        let mut inner = self.inner.borrow_mut();

        let count = if notification.count() == usize::MAX {
            inner.listeners.len()
        } else {
            notification.count().saturating_sub(inner.notified)
        };

        let mut notified = 0;
        for entry in inner.listeners.values_mut() {
            if notified >= count {
                break;
            }
            if entry.is_notified() {
                continue;
            }

            entry.value = Some(notification.tag());
            if let Some(waker) = entry.waker.take() {
                waker.wake();
            }
            notified += 1;
        }

        inner.notified += notified;
    }

    /// Notifies a number of active and still waiting listeners.
    ///
    /// Unlike `notify()`, this method only notifies listeners that haven't been
    /// notified yet and are still registered.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use eneste::event::{Event, NotificationExt};
    ///
    /// let event = Event::new();
    /// event.notify(2usize.additional());
    /// ```
    fn notify_additional<N: Notification<Tag = T>>(&self, notification: N) {
        let mut inner = self.inner.borrow_mut();

        let count = if notification.count() == usize::MAX {
            inner.listeners.len()
        } else {
            notification.count().min(inner.listeners.len())
        };

        let mut notified = 0;
        for entry in inner.listeners.values_mut() {
            if notified >= count {
                break;
            }
            if entry.is_notified() {
                continue;
            }
            entry.value = Some(notification.tag());
            if let Some(waker) = entry.waker.take() {
                waker.wake();
            }
            notified += 1;
        }

        inner.notified += notified;
    }

    pub fn stream(&self) -> EventStream<T> {
        let id = self.inner.borrow_mut().listen();
        EventStream {
            listener: Rc::downgrade(&self.inner),
            state: EventStreamState::Listening {
                listener: EventListener {
                    id,
                    event: Rc::clone(&self.inner),
                },
            },
        }
    }
}

impl<T> Drop for Event<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        inner.closed = true;
        for entry in inner.listeners.values_mut() {
            if let Some(waker) = entry.waker.take() {
                waker.wake();
            }
        }
    }
}

#[derive(Debug)]
struct Inner<T> {
    /// List of listeners waiting for notification.
    listeners: BTreeMap<usize, ListenerEntry<T>>,

    /// Counter for generating unique listener IDs.
    next_id: usize,

    /// Number of notified listeners that haven't been woken yet.
    notified: usize,

    /// Set once the owning `Event` has been dropped.
    closed: bool,
    value: PhantomData<fn(T)>,
}

impl<T> Inner<T> {
    fn listen(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;

        self.listeners.insert(
            id,
            ListenerEntry {
                waker: None,
                value: None,
            },
        );

        id
    }
}

#[derive(Debug)]
struct ListenerEntry<T> {
    waker: Option<Waker>,
    value: Option<T>,
}

impl<T> ListenerEntry<T> {
    fn is_notified(&self) -> bool {
        self.value.is_some()
    }
}

pub struct EventListener<T = ()> {
    id: usize,
    event: Rc<RefCell<Inner<T>>>,
}

impl<T> EventListener<T> {
    /// Whether the `Event` this listener belongs to has been dropped.
    pub(crate) fn is_closed(&self) -> bool {
        self.event.borrow().closed
    }

    pub fn is_notified(&self) -> bool {
        self.event
            .borrow()
            .listeners
            .get(&self.id)
            .map(|e| e.is_notified())
            .unwrap_or(false)
    }
}

impl<T> Drop for EventListener<T> {
    fn drop(&mut self) {
        let mut inner = self.event.borrow_mut();

        // Find and remove this listener
        let Some(entry) = inner.listeners.remove(&self.id) else {
            return;
        };

        if !entry.is_notified() || inner.notified == 0 {
            return;
        }

        inner.notified -= 1;

        let Some(next) = inner.listeners.values_mut().find(|e| !e.is_notified()) else {
            return;
        };

        next.value = entry.value;

        if let Some(waker) = next.waker.take() {
            waker.wake();
        }

        inner.notified += 1;
    }
}

impl<T> core::future::Future for EventListener<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.event.borrow_mut();
        let inner = &mut *inner;

        let Some(entry) = inner.listeners.get_mut(&self.id) else {
            unreachable!("Entry shouldn't be removed")
        };

        if let Some(value) = entry.value.take() {
            // The notification has been consumed, so it must no longer count
            // towards `notified`. Otherwise later `notify(n)` calls would
            // believe that `n` listeners were already notified and get lost.
            inner.notified = inner.notified.saturating_sub(1);
            return Poll::Ready(value);
        }

        // Store the waker for later notification
        entry.waker = Some(cx.waker().clone());

        Poll::Pending
    }
}

impl<T> FusedFuture for EventListener<T> {
    fn is_terminated(&self) -> bool {
        self.event
            .borrow()
            .listeners
            .get(&self.id)
            .map(|e| e.is_notified())
            .unwrap_or(true)
    }
}

pin_project! {
    #[project = EventStreamProj]
    enum EventStreamState<T> {
        Listening {
            #[pin]
            listener: EventListener<T>,
        },
        Next,
        Done,
    }
}

pin_project! {
    pub struct EventStream<T> {
        #[pin]
        listener: Weak<RefCell<Inner<T>>>,
        #[pin]
        state: EventStreamState<T>
    }
}

impl<T> Stream for EventStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();

            match this.state.as_mut().project() {
                EventStreamProj::Listening { mut listener } => match listener.as_mut().poll(cx) {
                    Poll::Ready(data) => {
                        *this.state = EventStreamState::Next;
                        return Poll::Ready(Some(data));
                    }
                    Poll::Pending => {
                        if listener.is_closed() {
                            *this.state = EventStreamState::Done;
                            continue;
                        }
                        return Poll::Pending;
                    }
                },
                EventStreamProj::Next => {
                    let Some(event) = this.listener.upgrade() else {
                        *this.state = EventStreamState::Done;
                        continue;
                    };

                    let id = event.borrow_mut().listen();

                    *this.state = EventStreamState::Listening {
                        listener: EventListener { id, event },
                    };
                }
                EventStreamProj::Done => return Poll::Ready(None),
            }
        }
    }
}

// Notification trait to represent the notification type and its associated data.

mod internal {
    use crate::event::{AddtionalNotification, WithNotification};

    pub trait Sealed {}

    impl Sealed for usize {}
    impl<T> Sealed for AddtionalNotification<T> {}
    impl<T, V> Sealed for WithNotification<T, V> {}
}

pub trait Notification: internal::Sealed {
    type Tag;
    fn count(&self) -> usize;
    fn is_additional(&self) -> bool;
    fn tag(&self) -> Self::Tag;
}

impl Notification for usize {
    type Tag = ();

    fn count(&self) -> usize {
        *self
    }

    fn is_additional(&self) -> bool {
        false
    }

    fn tag(&self) -> () {}
}

pub trait NotificationExt: Notification {
    fn additional(self) -> AddtionalNotification<Self>
    where
        Self: Sized,
    {
        AddtionalNotification { inner: self }
    }

    fn with<V>(self, value: V) -> WithNotification<V, Self>
    where
        Self: Sized,
    {
        WithNotification { inner: self, value }
    }
}

impl<V> NotificationExt for V where V: Notification {}

pub struct AddtionalNotification<T> {
    inner: T,
}

impl<V> Notification for AddtionalNotification<V>
where
    V: Notification,
{
    type Tag = V::Tag;
    fn count(&self) -> usize {
        self.inner.count()
    }

    fn is_additional(&self) -> bool {
        true
    }

    fn tag(&self) -> V::Tag {
        self.inner.tag()
    }
}

pub struct WithNotification<T, N> {
    inner: N,
    value: T,
}

impl<T, N> Notification for WithNotification<T, N>
where
    N: Notification,
    T: Clone,
{
    type Tag = T;

    fn count(&self) -> usize {
        self.inner.count()
    }

    fn is_additional(&self) -> bool {
        self.inner.is_additional()
    }

    fn tag(&self) -> T {
        self.value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };
    use futures_core::FusedFuture;

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

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        future.poll(&mut cx)
    }

    #[test]
    fn listener_starts_unnotified_and_pending() {
        let event = Event::new();
        let mut listener = event.listen();

        assert!(!listener.is_notified());
        assert!(!listener.is_terminated());
        assert!(matches!(poll_once(Pin::new(&mut listener)), Poll::Pending));
    }

    #[test]
    fn notify_one_notifies_earliest_listener() {
        let event = Event::new();
        let mut first = event.listen();
        let mut second = event.listen();

        event.notify(1);

        assert!(first.is_notified());
        assert!(!second.is_notified());
        assert_eq!(pollster::block_on(&mut first), ());
        assert!(matches!(poll_once(Pin::new(&mut second)), Poll::Pending));
    }

    #[test]
    fn notify_all_notifies_all_active_listeners() {
        let event = Event::new();
        let mut first = event.listen();
        let mut second = event.listen();
        let mut third = event.listen();

        event.notify(usize::MAX);

        assert_eq!(pollster::block_on(&mut first), ());
        assert_eq!(pollster::block_on(&mut second), ());
        assert_eq!(pollster::block_on(&mut third), ());
    }

    #[test]
    fn additional_only_notifies_still_waiting_listeners() {
        let event = Event::new();
        let mut first = event.listen();
        let mut second = event.listen();
        let mut third = event.listen();

        event.notify(1);
        event.notify(1.additional());

        assert_eq!(pollster::block_on(&mut first), ());
        assert_eq!(pollster::block_on(&mut second), ());
        assert!(matches!(poll_once(Pin::new(&mut third)), Poll::Pending));
    }

    #[test]
    fn listener_transitions_to_terminated_once_notified() {
        let event = Event::new();
        let mut listener = event.listen();

        assert!(!listener.is_terminated());
        event.notify(1);
        assert!(listener.is_notified());
        assert!(listener.is_terminated());

        let output = pollster::block_on(async { (&mut listener).await });
        assert_eq!(output, ());
    }

    #[test]
    fn with_notification_delivers_typed_value() {
        let event = Event::<usize>::new_with();
        let mut listener = event.listen();

        event.notify(1.with(42));

        assert_eq!(pollster::block_on(&mut listener), 42);
    }

    #[test]
    fn dropping_notified_listener_transfers_notification_to_next_waiting() {
        let event = Event::new();
        let first = event.listen();
        let mut second = event.listen();
        let mut third = event.listen();

        event.notify(1);
        assert!(first.is_notified());
        assert!(!second.is_notified());
        assert!(!third.is_notified());

        drop(first);

        assert!(second.is_notified());
        assert!(!third.is_notified());
        assert_eq!(pollster::block_on(&mut second), ());
        assert!(matches!(poll_once(Pin::new(&mut third)), Poll::Pending));
    }

    // ---- additional coverage ----

    use crate::test_util::{counting_waker, poll_next_once, poll_with};
    use alloc::vec::Vec;

    #[test]
    fn default_event_is_usable() {
        let event = Event::default();
        let listener = event.listen();
        event.notify(1);
        assert!(listener.is_notified());
    }

    #[test]
    fn notify_without_listeners_is_a_noop() {
        let event = Event::new();
        event.notify(1);
        event.notify(usize::MAX);
        event.notify(3.additional());

        // Listeners created afterwards are not retroactively notified.
        let listener = event.listen();
        assert!(!listener.is_notified());
    }

    #[test]
    fn notify_zero_notifies_nobody() {
        let event = Event::new();
        let listener = event.listen();

        event.notify(0);

        assert!(!listener.is_notified());
    }

    #[test]
    fn notify_n_notifies_exactly_n_listeners_in_registration_order() {
        let event = Event::new();
        let listeners: Vec<_> = (0..5).map(|_| event.listen()).collect();

        event.notify(3);

        let notified: Vec<bool> = listeners.iter().map(|l| l.is_notified()).collect();
        assert_eq!(notified, [true, true, true, false, false]);
    }

    #[test]
    fn notify_more_than_listener_count_notifies_everyone() {
        let event = Event::new();
        let first = event.listen();
        let second = event.listen();

        event.notify(10);

        assert!(first.is_notified());
        assert!(second.is_notified());
    }

    #[test]
    fn notify_counts_already_notified_listeners_towards_the_total() {
        let event = Event::new();
        let first = event.listen();
        let second = event.listen();

        event.notify(1);
        // "At least 2 notified": only one more is required.
        event.notify(2);

        assert!(first.is_notified());
        assert!(second.is_notified());
    }

    #[test]
    fn additional_notifications_stack_on_top_of_existing_ones() {
        let event = Event::new();
        let listeners: Vec<_> = (0..4).map(|_| event.listen()).collect();

        event.notify(1);
        event.notify(2.additional());

        let notified: Vec<bool> = listeners.iter().map(|l| l.is_notified()).collect();
        assert_eq!(notified, [true, true, true, false]);
    }

    #[test]
    fn additional_max_notifies_every_waiting_listener() {
        let event = Event::new();
        let first = event.listen();
        let second = event.listen();
        let third = event.listen();

        event.notify(1);
        event.notify(usize::MAX.additional());

        assert!(first.is_notified());
        assert!(second.is_notified());
        assert!(third.is_notified());
    }

    #[test]
    fn listener_created_after_notify_is_not_notified() {
        let event = Event::new();
        let early = event.listen();
        event.notify(usize::MAX);
        let late = event.listen();

        assert!(early.is_notified());
        assert!(!late.is_notified());
    }

    #[test]
    fn notify_wakes_the_registered_waker() {
        let event = Event::new();
        let mut listener = event.listen();
        let (waker, wakes) = counting_waker();

        assert_eq!(poll_with(Pin::new(&mut listener), &waker), Poll::Pending);
        assert_eq!(wakes.count(), 0);

        event.notify(1);

        assert_eq!(wakes.count(), 1);
        assert_eq!(poll_with(Pin::new(&mut listener), &waker), Poll::Ready(()));
    }

    #[test]
    fn notify_only_wakes_selected_listeners() {
        let event = Event::new();
        let mut first = event.listen();
        let mut second = event.listen();
        let (first_waker, first_wakes) = counting_waker();
        let (second_waker, second_wakes) = counting_waker();

        let _ = poll_with(Pin::new(&mut first), &first_waker);
        let _ = poll_with(Pin::new(&mut second), &second_waker);

        event.notify(1);

        assert_eq!(first_wakes.count(), 1);
        assert_eq!(second_wakes.count(), 0);
    }

    #[test]
    fn notify_all_wakes_every_waiting_listener_once() {
        let event = Event::new();
        let mut first = event.listen();
        let mut second = event.listen();
        let (first_waker, first_wakes) = counting_waker();
        let (second_waker, second_wakes) = counting_waker();

        let _ = poll_with(Pin::new(&mut first), &first_waker);
        let _ = poll_with(Pin::new(&mut second), &second_waker);

        event.notify(usize::MAX);
        event.notify(usize::MAX);

        assert_eq!(first_wakes.count(), 1);
        assert_eq!(second_wakes.count(), 1);
    }

    #[test]
    fn repolling_updates_the_stored_waker() {
        let event = Event::new();
        let mut listener = event.listen();
        let (old_waker, old_wakes) = counting_waker();
        let (new_waker, new_wakes) = counting_waker();

        let _ = poll_with(Pin::new(&mut listener), &old_waker);
        let _ = poll_with(Pin::new(&mut listener), &new_waker);

        event.notify(1);

        assert_eq!(old_wakes.count(), 0);
        assert_eq!(new_wakes.count(), 1);
    }

    #[test]
    fn polling_a_consumed_listener_is_pending_again() {
        let event = Event::new();
        let mut listener = event.listen();
        event.notify(1);

        assert_eq!(pollster::block_on(&mut listener), ());
        assert!(matches!(poll_once(Pin::new(&mut listener)), Poll::Pending));
    }

    #[test]
    fn dropping_an_unnotified_listener_does_not_affect_others() {
        let event = Event::new();
        let first = event.listen();
        let second = event.listen();

        drop(first);
        event.notify(1);

        assert!(second.is_notified());
    }

    #[test]
    fn dropping_a_notified_listener_with_no_one_waiting_discards_the_notification() {
        let event = Event::new();
        let first = event.listen();
        event.notify(1);
        drop(first);

        let second = event.listen();
        assert!(!second.is_notified());

        event.notify(1);
        assert!(second.is_notified());
    }

    #[test]
    fn dropping_a_notified_listener_wakes_the_listener_receiving_the_handoff() {
        let event = Event::new();
        let first = event.listen();
        let mut second = event.listen();
        let (waker, wakes) = counting_waker();

        let _ = poll_with(Pin::new(&mut second), &waker);
        event.notify(1);
        assert_eq!(wakes.count(), 0);

        drop(first);

        assert_eq!(wakes.count(), 1);
        assert_eq!(poll_with(Pin::new(&mut second), &waker), Poll::Ready(()));
    }

    #[test]
    fn handed_off_notification_keeps_its_typed_value() {
        let event = Event::<u32>::new_with();
        let first = event.listen();
        let mut second = event.listen();

        event.notify(1.with(99));
        drop(first);

        assert_eq!(pollster::block_on(&mut second), 99);
    }

    #[test]
    fn with_notification_clones_value_for_each_listener() {
        let event = Event::<u32>::new_with();
        let mut first = event.listen();
        let mut second = event.listen();

        event.notify(usize::MAX.with(7));

        assert_eq!(pollster::block_on(&mut first), 7);
        assert_eq!(pollster::block_on(&mut second), 7);
    }

    #[test]
    fn additional_notification_can_carry_a_value() {
        let event = Event::<&'static str>::new_with();
        let mut first = event.listen();
        let mut second = event.listen();

        event.notify(1.with("one"));
        event.notify(1.with("two").additional());

        assert_eq!(pollster::block_on(&mut first), "one");
        assert_eq!(pollster::block_on(&mut second), "two");
    }

    #[test]
    fn notification_trait_accessors() {
        assert_eq!(3usize.count(), 3);
        assert!(!3usize.is_additional());
        assert!(3usize.additional().is_additional());
        assert_eq!(3usize.additional().count(), 3);

        let with = 2usize.with("tag");
        assert_eq!(with.count(), 2);
        assert_eq!(with.tag(), "tag");
        assert!(!with.is_additional());
        assert!(with.additional().is_additional());
    }

    #[test]
    fn dropping_the_event_wakes_pending_listeners() {
        let event = Event::new();
        let mut listener = event.listen();
        let (waker, wakes) = counting_waker();

        let _ = poll_with(Pin::new(&mut listener), &waker);
        drop(event);

        assert_eq!(wakes.count(), 1);
    }

    #[test]
    fn listener_outlives_its_event_without_panicking() {
        let event = Event::new();
        let listener = event.listen();
        drop(event);

        assert!(!listener.is_notified());
        drop(listener);
    }

    #[test]
    fn stream_is_pending_until_notified_and_then_yields() {
        let event = Event::new();
        let mut stream = event.stream();

        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));
        event.notify(usize::MAX);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
    }

    #[test]
    fn stream_resubscribes_after_each_item() {
        let event = Event::new();
        let mut stream = event.stream();

        for _ in 0..3 {
            event.notify(usize::MAX);
            assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));
            assert!(matches!(
                poll_next_once(Pin::new(&mut stream)),
                Poll::Pending
            ));
        }
    }

    #[test]
    fn stream_yields_typed_values() {
        let event = Event::<u32>::new_with();
        let mut stream = event.stream();

        event.notify(usize::MAX.with(1));
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(1)));
        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));
        event.notify(usize::MAX.with(2));
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(2)));
    }

    #[test]
    fn multiple_streams_each_receive_broadcasts() {
        let event = Event::new();
        let mut a = event.stream();
        let mut b = event.stream();

        event.notify(usize::MAX);

        assert_eq!(poll_next_once(Pin::new(&mut a)), Poll::Ready(Some(())));
        assert_eq!(poll_next_once(Pin::new(&mut b)), Poll::Ready(Some(())));
    }

    #[test]
    fn stream_ends_once_the_event_is_dropped() {
        let event = Event::new();
        let mut stream = event.stream();

        event.notify(usize::MAX);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(Some(())));

        drop(event);
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(None));
        // The stream stays finished.
        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(None));
    }

    /// Dropping the event wakes parked listeners; a stream parked on it
    /// must then terminate instead of staying pending forever.
    #[test]
    fn waiting_stream_ends_when_the_event_is_dropped() {
        let event = Event::new();
        let mut stream = event.stream();
        assert!(matches!(
            poll_next_once(Pin::new(&mut stream)),
            Poll::Pending
        ));

        drop(event);

        assert_eq!(poll_next_once(Pin::new(&mut stream)), Poll::Ready(None));
    }

    /// Regression test: consuming a notification must be subtracted from the
    /// internal `notified` counter, otherwise later `notify(n)` calls silently
    /// lose wake-ups (this used to hang `WaitGroup::wait`, `AsyncMutex`, mpsc
    /// channels and friends under contention).
    #[test]
    fn notify_one_still_works_after_a_notification_was_consumed() {
        let event = Event::new();

        let mut first = event.listen();
        event.notify(1);
        assert_eq!(pollster::block_on(&mut first), ());
        drop(first);

        let second = event.listen();
        event.notify(1);

        assert!(second.is_notified());
    }
}
