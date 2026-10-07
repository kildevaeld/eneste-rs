use futures_core::Stream;
use pin_project_lite::pin_project;

pub fn next<T>(stream: &mut T) -> Next<'_, T> {
    Next { stream }
}

pin_project! {

    pub struct Next<'a, T> {
        #[pin]
        stream: &'a mut T
    }
}

impl<'a, T> Future for Next<'a, T>
where
    T: Stream + Unpin,
{
    type Output = Option<T::Item>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.project().stream.poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::Event,
        test_util::{IterStream, counting_waker, poll_with},
    };
    use alloc::vec::Vec;
    use core::{pin::pin, task::Poll};
    use futures_core::Stream;

    #[test]
    fn next_yields_items_then_none() {
        let mut stream = IterStream([1, 2, 3].into_iter());

        assert_eq!(pollster::block_on(next(&mut stream)), Some(1));
        assert_eq!(pollster::block_on(next(&mut stream)), Some(2));
        assert_eq!(pollster::block_on(next(&mut stream)), Some(3));
        assert_eq!(pollster::block_on(next(&mut stream)), None);
        assert_eq!(pollster::block_on(next(&mut stream)), None);
    }

    #[test]
    fn next_on_empty_stream_returns_none() {
        let mut stream = IterStream(core::iter::empty::<u8>());
        assert_eq!(pollster::block_on(next(&mut stream)), None);
    }

    #[test]
    fn next_can_drain_a_stream_in_a_loop() {
        let mut stream = IterStream(0..5);
        let collected = pollster::block_on(async {
            let mut items = Vec::new();
            while let Some(item) = next(&mut stream).await {
                items.push(item);
            }
            items
        });
        assert_eq!(collected, [0, 1, 2, 3, 4]);
    }

    #[test]
    fn next_is_pending_until_the_stream_produces_a_value() {
        let event = Event::new();
        let mut stream = event.stream();
        let (waker, wakes) = counting_waker();

        {
            let mut fut = pin!(next(&mut stream));
            assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Pending);
            assert_eq!(wakes.count(), 0);

            event.notify(1);
            assert_eq!(wakes.count(), 1);
            assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Ready(Some(())));
        }

        // The stream remains usable after a `next` future completes.
        let mut fut = pin!(next(&mut stream));
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Pending);
    }

    #[test]
    fn dropping_next_future_does_not_consume_items() {
        let mut stream = IterStream([10, 20].into_iter());
        drop(next(&mut stream));

        assert_eq!(pollster::block_on(next(&mut stream)), Some(10));
    }

    fn assert_stream<S: Stream + Unpin>(_: &S) {}

    #[test]
    fn iter_stream_helper_is_a_stream() {
        assert_stream(&IterStream(core::iter::empty::<()>()));
    }
}
