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
    /// use lamp_runtime::event::Event;
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
    /// use lamp_runtime::event::{Event, NotificationExt};
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

        let Some(entry) = inner.listeners.get_mut(&self.id) else {
            unreachable!("Entry shouldn't be removed")
        };

        if entry.is_notified() {
            return Poll::Ready(entry.value.take().unwrap());
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
                EventStreamProj::Listening { listener } => match listener.poll(cx) {
                    Poll::Ready(data) => {
                        *this.state = EventStreamState::Next;
                        return Poll::Ready(Some(data));
                    }
                    Poll::Pending => return Poll::Pending,
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
}
