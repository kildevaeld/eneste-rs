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
