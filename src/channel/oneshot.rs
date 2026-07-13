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
