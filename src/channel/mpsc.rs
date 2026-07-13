use core::{
    cell::RefCell,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    channel::ChannelError,
    event::{Event, EventListener},
};
use alloc::{
    collections::vec_deque::VecDeque,
    rc::{Rc, Weak},
};
use futures_core::Stream;
use pin_project_lite::pin_project;

pub fn channel<T>(max_queue_size: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(State {
        // Receivers wait for data; senders wait for capacity. Keeping separate events avoids
        // waking the wrong side when the queue changes state.
        receiver_event: Rc::new(Event::new()),
        sender_event: Rc::new(Event::new()),
        queue: VecDeque::new(),
        max_queue_size,
        senders: 1,
    }));
    let sender = Sender {
        shared: Rc::downgrade(&shared),
    };
    let receiver = Receiver {
        shared,
        state: ReceiverState::Idle,
    };
    (sender, receiver)
}

pub struct Sender<T> {
    // Weak handle allows sends to fail cleanly once the receiver side is dropped.
    shared: Weak<RefCell<State<T>>>,
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Send<T> {
        Send {
            shared: self.shared.clone(),
            state: SendState::Idle,
            value: Some(value),
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), T> {
        let Some(shared) = self.shared.upgrade() else {
            return Err(value);
        };
        let should_notify_receiver = {
            let mut shared = shared.borrow_mut();
            if shared.queue.len() < shared.max_queue_size {
                let was_empty = shared.queue.is_empty();
                shared.queue.push_back(value);
                was_empty
            } else {
                return Err(value);
            }
        };

        if should_notify_receiver {
            shared.borrow().receiver_event.notify(1);
        }

        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        if let Some(shared) = self.shared.upgrade() {
            let shared = shared.borrow();
            shared.senders == 0
        } else {
            true
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            let should_notify_receiver = {
                let mut shared = shared.borrow_mut();
                shared.senders -= 1;
                shared.senders == 0
            };

            if should_notify_receiver {
                shared.borrow().receiver_event.notify(usize::MAX);
            }
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        if let Some(shared) = self.shared.upgrade() {
            let mut shared = shared.borrow_mut();
            shared.senders += 1;
        }
        Sender {
            shared: self.shared.clone(),
        }
    }
}

pin_project! {
    #[project = SendStateProj]
    enum SendState {
        Waiting {
            #[pin]
            send: EventListener<()>,
        },
        Idle,
    }
}

pin_project! {

    pub struct Send<T> {
        // Shared channel state, upgraded on each poll to detect closed receiver.
        #[pin]
        shared: Weak<RefCell<State<T>>>,
        // Tracks whether this future is waiting for capacity or retrying send.
        #[pin]
        state: SendState,
        // Value is moved into the queue once space is available.
        value: Option<T>,
    }
}

impl<T> Future for Send<T> {
    type Output = Result<(), ChannelError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut this = self.as_mut().project();
            match this.state.as_mut().project() {
                SendStateProj::Waiting { send } => match send.poll(cx) {
                    Poll::Ready(_) => {
                        *this.state = SendState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                SendStateProj::Idle => {
                    let Some(shared) = this.shared.upgrade() else {
                        return Poll::Ready(Err(ChannelError));
                    };
                    let should_notify_receiver = {
                        let mut shared = shared.borrow_mut();
                        if shared.queue.len() < shared.max_queue_size {
                            // Only wake a receiver when the queue transitions from empty to non-empty.
                            let was_empty = shared.queue.is_empty();
                            shared
                                .queue
                                .push_back(this.value.take().expect("value should be present"));
                            was_empty
                        } else {
                            *this.state = SendState::Waiting {
                                send: shared.sender_event.listen(),
                            };
                            continue;
                        }
                    };

                    if should_notify_receiver {
                        shared.borrow().receiver_event.notify(1);
                    }

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

struct State<T> {
    // Notifies receivers when data arrives or when all senders have gone away.
    receiver_event: Rc<Event>,
    // Notifies blocked senders when capacity is freed or receiver drops.
    sender_event: Rc<Event>,
    // FIFO storage for queued values.
    queue: VecDeque<T>,
    // Upper bound for queued items before senders must wait.
    max_queue_size: usize,
    // Number of live sender handles, including clones.
    senders: usize,
    // sender_ready_stream: Pin<Box<EventStream<()>>>,
}

impl<T> State<T> {
    // fn is_full(&self) -> bool {
    //     self.queue.len() >= self.max_queue_size
    // }

    // fn is_empty(&self) -> bool {
    //     self.queue.is_empty()
    // }

    // fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ChannelError>> {
    //     if self.queue.len() < self.max_queue_size {
    //         Poll::Ready(Ok(()))
    //     } else {
    //         self.sender_ready_stream.as_mut().poll_next(cx).map(|_| {
    //             if self.senders == 0 {
    //                 Err(ChannelError)
    //             } else {
    //                 Ok(())
    //             }
    //         });

    //         todo!()
    //     }
    // }
}

pin_project! {
    #[project = ReceiverStateProj]
    enum ReceiverState {
        Waiting {
            #[pin]
            recv: EventListener<()>,
        },
        Idle,
    }
}

pin_project! {
    pub struct Receiver<T> {
        // Strong owner of shared state; dropping receiver closes the channel.
        shared: Rc<RefCell<State<T>>>,
        // Tracks whether this stream is waiting for data or ready to read.
        #[pin]
        state: ReceiverState,
    }

    impl<T> PinnedDrop for Receiver<T> {
        fn drop(this: Pin<&mut Self>) {
            let sender_event = {
                let this = this.project();
                let shared = this.shared.borrow();
                Rc::clone(&shared.sender_event)
            };

            sender_event.notify(usize::MAX);
        }
    }
}

impl<T> Receiver<T> {
    pub fn is_empty(&self) -> bool {
        let shared = self.shared.borrow();
        shared.queue.is_empty()
    }

    pub fn is_full(&self) -> bool {
        let shared = self.shared.borrow();
        shared.queue.len() >= shared.max_queue_size
    }

    pub fn is_closed(&self) -> bool {
        let shared = self.shared.borrow();
        shared.senders == 0
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            match this.state.as_mut().project() {
                ReceiverStateProj::Waiting { recv } => match recv.poll(cx) {
                    Poll::Ready(_) => {
                        *this.state = ReceiverState::Idle;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ReceiverStateProj::Idle => {
                    let mut shared = this.shared.borrow_mut();
                    // Only wake one blocked sender when a full queue gains a free slot.
                    let was_full = shared.queue.len() == shared.max_queue_size;
                    if let Some(value) = shared.queue.pop_front() {
                        drop(shared);

                        if was_full {
                            this.shared.borrow().sender_event.notify(1);
                        }

                        return Poll::Ready(Some(value));
                    }

                    if shared.senders == 0 {
                        return Poll::Ready(None);
                    }

                    *this.state = ReceiverState::Waiting {
                        recv: shared.receiver_event.listen(),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloc::rc::Rc;
    use core::{
        cell::Cell,
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

    fn counting_waker() -> (Waker, Rc<Cell<usize>>) {
        unsafe fn clone(data: *const ()) -> RawWaker {
            let counter = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
            let cloned = Rc::clone(&counter);
            let _ = Rc::into_raw(counter);
            RawWaker::new(Rc::into_raw(cloned).cast(), &VTABLE)
        }

        unsafe fn wake(data: *const ()) {
            let counter = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
            counter.set(counter.get() + 1);
        }

        unsafe fn wake_by_ref(data: *const ()) {
            let counter = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
            counter.set(counter.get() + 1);
            let _ = Rc::into_raw(counter);
        }

        unsafe fn release(data: *const ()) {
            let _ = unsafe { Rc::<Cell<usize>>::from_raw(data.cast()) };
        }

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, release);

        let counter = Rc::new(Cell::new(0));
        let raw = RawWaker::new(Rc::into_raw(Rc::clone(&counter)).cast(), &VTABLE);

        unsafe { (Waker::from_raw(raw), counter) }
    }

    fn poll_future_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        let mut cx = Context::from_waker(waker);
        future.poll(&mut cx)
    }

    fn poll_receiver_once<T>(receiver: Pin<&mut Receiver<T>>, waker: &Waker) -> Poll<Option<T>> {
        let mut cx = Context::from_waker(waker);
        receiver.poll_next(&mut cx)
    }

    #[test]
    fn receiver_wakes_when_data_becomes_available() {
        let (sender, mut receiver) = channel(1);
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert!(matches!(
            poll_receiver_once(Pin::new(&mut receiver), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        assert_eq!(sender.try_send(7), Ok(()));
        assert_eq!(wake_count.get(), 1);
        assert_eq!(
            poll_receiver_once(Pin::new(&mut receiver), &idle_waker),
            Poll::Ready(Some(7))
        );
    }

    #[test]
    fn sender_wakes_when_capacity_becomes_available() {
        let (sender, mut receiver) = channel(1);
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert_eq!(sender.try_send(1), Ok(()));

        let mut send = sender.send(2);
        assert!(matches!(
            poll_future_once(Pin::new(&mut send), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        assert_eq!(
            poll_receiver_once(Pin::new(&mut receiver), &idle_waker),
            Poll::Ready(Some(1))
        );
        assert_eq!(wake_count.get(), 1);
        assert!(matches!(
            poll_future_once(Pin::new(&mut send), &idle_waker),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(
            poll_receiver_once(Pin::new(&mut receiver), &idle_waker),
            Poll::Ready(Some(2))
        );
    }

    #[test]
    fn waiting_sender_errors_when_receiver_is_dropped() {
        let (sender, receiver) = channel(1);
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();

        assert_eq!(sender.try_send(1), Ok(()));

        let mut send = sender.send(2);
        assert!(matches!(
            poll_future_once(Pin::new(&mut send), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        drop(receiver);

        assert_eq!(wake_count.get(), 1);
        assert!(matches!(
            poll_future_once(Pin::new(&mut send), &idle_waker),
            Poll::Ready(Err(_))
        ));
    }

    #[test]
    fn receiver_finishes_only_after_queue_is_empty_and_all_senders_are_dropped() {
        let (sender, mut receiver) = channel(2);
        let (waker, wake_count) = counting_waker();
        let idle_waker = noop_waker();
        let sender_clone = sender.clone();

        assert_eq!(sender.try_send(5), Ok(()));
        drop(sender);

        assert_eq!(
            poll_receiver_once(Pin::new(&mut receiver), &idle_waker),
            Poll::Ready(Some(5))
        );
        assert!(matches!(
            poll_receiver_once(Pin::new(&mut receiver), &waker),
            Poll::Pending
        ));
        assert_eq!(wake_count.get(), 0);

        drop(sender_clone);

        assert_eq!(wake_count.get(), 1);
        assert_eq!(
            poll_receiver_once(Pin::new(&mut receiver), &idle_waker),
            Poll::Ready(None)
        );
    }

    #[test]
    fn queue_state_tracks_fill_level_and_receiver_closure() {
        let (sender, receiver) = channel(1);

        assert!(receiver.is_empty());
        assert!(!receiver.is_full());
        assert!(!receiver.is_closed());
        assert!(!sender.is_closed());

        assert_eq!(sender.try_send(3), Ok(()));
        assert!(!receiver.is_empty());
        assert!(receiver.is_full());

        drop(receiver);

        assert!(sender.is_closed());
        assert_eq!(sender.try_send(4), Err(4));
    }
}
