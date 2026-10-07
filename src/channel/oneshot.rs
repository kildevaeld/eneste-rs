use core::{
    cell::{Cell, RefCell},
    task::ready,
};

use alloc::rc::Rc;
use pin_project_lite::pin_project;

use crate::{
    channel::ChannelError,
    event::{Event, EventListener},
};

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(State {
        value: RefCell::new(None),
        state: Cell::new(ChannelState::Empty),
    });
    let event = Event::new();

    let receiver = Receiver {
        listener: event.listen(),
        state: state.clone(),
    };

    let sender = Sender { state, event };

    (sender, receiver)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelState {
    Empty,
    SenderClosed,
    ReceiverClosed,
    Full,
}

struct State<T> {
    value: RefCell<Option<T>>,
    state: Cell<ChannelState>,
}

pub struct Sender<T> {
    state: Rc<State<T>>,
    event: Event,
}

impl<T> Sender<T> {
    pub fn send(self, value: T) -> Result<(), ChannelError> {
        match self.state.state.get() {
            ChannelState::Empty => {
                *self.state.value.borrow_mut() = Some(value);
                self.state.state.set(ChannelState::Full);
                self.event.notify(1);
                Ok(())
            }
            ChannelState::ReceiverClosed => Err(ChannelError),
            _ => {
                unreachable!("Sender should not be able to send when the channel is closed");
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        match self.state.state.get() {
            ChannelState::Empty => {
                self.state.state.set(ChannelState::SenderClosed);
                self.event.notify(1);
            }
            _ => {}
        }
    }
}

pin_project! {
    pub struct Receiver<T> {
        #[pin]
        listener: EventListener<()>,
        state: Rc<State<T>>,
    }

    impl<T> PinnedDrop for Receiver<T> {
        fn drop(this: Pin<&mut Self>) {
            this.get_mut().close();
        }
    }

}

impl<T> Receiver<T> {
    pub fn close(&mut self) {
        match self.state.state.get() {
            ChannelState::Empty => {
                self.state.state.set(ChannelState::ReceiverClosed);
                self.state.value.borrow_mut().take();
            }
            _ => {}
        }
    }

    pub fn try_recv(&mut self) -> Result<Option<T>, ChannelError> {
        match self.state.state.get() {
            ChannelState::Full => {
                let value = self.state.value.borrow_mut().take();
                self.state.state.set(ChannelState::Empty);
                Ok(value)
            }
            ChannelState::SenderClosed | ChannelState::ReceiverClosed => Err(ChannelError),
            ChannelState::Empty => Ok(None),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, ChannelError>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let this = self.project();
        match this.state.state.get() {
            ChannelState::Full => {
                let value = this.state.value.borrow_mut().take();
                this.state.state.set(ChannelState::Empty);
                core::task::Poll::Ready(value.ok_or(ChannelError))
            }
            ChannelState::SenderClosed => core::task::Poll::Ready(Err(ChannelError)),
            _ => {
                ready!(this.listener.poll(cx));
                core::task::Poll::Pending
            }
        }
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
    fn receiver_starts_pending_until_sender_fires() {
        let (sender, mut receiver) = channel();

        assert!(matches!(poll_once(Pin::new(&mut receiver)), Poll::Pending));

        assert!(sender.send(7).is_ok());
        assert_eq!(pollster::block_on(&mut receiver), Ok(7));
    }

    #[test]
    fn sent_value_is_delivered_once() {
        let (sender, mut receiver) = channel();

        assert!(sender.send(42).is_ok());
        assert_eq!(pollster::block_on(&mut receiver), Ok(42));
    }

    #[test]
    fn send_fails_after_receiver_is_dropped() {
        let (sender, receiver) = channel::<usize>();

        drop(receiver);

        assert!(matches!(sender.send(5), Err(ChannelError)));
    }

    #[test]
    fn receiver_is_closed_after_sender_is_dropped() {
        let (sender, mut receiver) = channel::<usize>();

        drop(sender);

        // assert!(receiver.is_closed());
        assert_eq!(pollster::block_on(&mut receiver), Err(ChannelError));
    }
}

#[cfg(test)]
mod behavior_tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_once, poll_with};
    use alloc::rc::Rc;
    use core::{pin::pin, task::Poll};

    #[test]
    fn value_can_be_awaited_after_sending() {
        let (sender, receiver) = channel();
        sender.send(5).unwrap();

        assert_eq!(pollster::block_on(receiver), Ok(5));
    }

    #[test]
    fn awaiting_after_the_sender_was_dropped_fails() {
        let (sender, receiver) = channel::<u8>();
        drop(sender);

        assert_eq!(pollster::block_on(receiver), Err(ChannelError));
    }

    #[test]
    fn receiver_is_woken_by_send() {
        let (sender, receiver) = channel();
        let (waker, wakes) = counting_waker();
        let mut receiver = pin!(receiver);

        assert!(poll_with(receiver.as_mut(), &waker).is_pending());
        assert_eq!(wakes.count(), 0);

        sender.send("hello").unwrap();

        assert_eq!(wakes.count(), 1);
        assert_eq!(poll_with(receiver.as_mut(), &waker), Poll::Ready(Ok("hello")));
    }

    #[test]
    fn receiver_is_woken_when_sender_is_dropped() {
        let (sender, receiver) = channel::<u8>();
        let (waker, wakes) = counting_waker();
        let mut receiver = pin!(receiver);

        assert!(poll_with(receiver.as_mut(), &waker).is_pending());
        drop(sender);

        assert_eq!(wakes.count(), 1);
        assert_eq!(
            poll_with(receiver.as_mut(), &waker),
            Poll::Ready(Err(ChannelError))
        );
    }

    #[test]
    fn try_recv_is_none_before_anything_is_sent() {
        let (_sender, mut receiver) = channel::<u8>();
        assert_eq!(receiver.try_recv(), Ok(None));
        assert_eq!(receiver.try_recv(), Ok(None));
    }

    #[test]
    fn try_recv_returns_the_value_once() {
        let (sender, mut receiver) = channel();
        sender.send(3).unwrap();

        assert_eq!(receiver.try_recv(), Ok(Some(3)));
        // The channel is back to "empty" after the value has been taken.
        assert_eq!(receiver.try_recv(), Ok(None));
    }

    #[test]
    fn try_recv_errors_after_sender_drop() {
        let (sender, mut receiver) = channel::<u8>();
        drop(sender);

        assert_eq!(receiver.try_recv(), Err(ChannelError));
    }

    #[test]
    fn try_recv_still_returns_a_value_sent_before_the_sender_dropped() {
        let (sender, mut receiver) = channel();
        sender.send(1).unwrap();

        assert_eq!(receiver.try_recv(), Ok(Some(1)));
    }

    #[test]
    fn closing_the_receiver_makes_send_fail() {
        let (sender, mut receiver) = channel();
        receiver.close();

        assert_eq!(sender.send(1), Err(ChannelError));
    }

    #[test]
    fn closing_the_receiver_makes_try_recv_fail() {
        let (_sender, mut receiver) = channel::<u8>();
        receiver.close();

        assert_eq!(receiver.try_recv(), Err(ChannelError));
    }

    #[test]
    fn closing_twice_is_harmless() {
        let (sender, mut receiver) = channel::<u8>();
        receiver.close();
        receiver.close();

        assert_eq!(sender.send(1), Err(ChannelError));
    }

    #[test]
    fn closing_after_a_value_was_sent_keeps_the_value() {
        let (sender, mut receiver) = channel();
        sender.send(8).unwrap();
        receiver.close();

        assert_eq!(receiver.try_recv(), Ok(Some(8)));
    }

    #[test]
    fn value_is_dropped_when_unreceived_receiver_is_dropped() {
        let tracker = Rc::new(());
        let (sender, receiver) = channel();
        sender.send(Rc::clone(&tracker)).unwrap();
        assert_eq!(Rc::strong_count(&tracker), 2);

        drop(receiver);

        // The sent-but-unreceived value is released together with the channel.
        assert_eq!(Rc::strong_count(&tracker), 1);
    }

    #[test]
    fn dropping_an_unsent_sender_does_not_panic_and_pending_receiver_can_be_dropped() {
        let (sender, receiver) = channel::<u8>();
        let mut receiver = pin!(receiver);
        assert!(poll_once(receiver.as_mut()).is_pending());
        drop(sender);
    }

    #[test]
    fn supports_non_copy_payloads() {
        let (sender, receiver) = channel();
        sender.send(alloc::vec![1, 2, 3]).unwrap();

        assert_eq!(pollster::block_on(receiver), Ok(alloc::vec![1, 2, 3]));
    }
}
