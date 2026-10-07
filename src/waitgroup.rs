use alloc::rc::Rc;
use core::{cell::Cell, fmt};

use crate::{event::Event, util::next};

pub struct WaitGroup {
    event: Rc<Event>,
    tickets: Rc<Cell<usize>>,
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WaitGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waitgroup")
            .field("tickets", &self.tickets)
            .finish()
    }
}

impl WaitGroup {
    pub fn new() -> WaitGroup {
        WaitGroup {
            event: Rc::new(Event::new()),
            tickets: Rc::new(Cell::new(0)),
        }
    }
}

impl WaitGroup {
    pub fn add(&mut self) -> Ticket {
        self.tickets.set(self.tickets.get() + 1);
        Ticket {
            event: self.event.clone(),
            tickets: self.tickets.clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.tickets.get()
    }

    pub fn is_empty(&self) -> bool {
        self.tickets.get() == 0
    }

    pub async fn wait(&mut self) {
        if self.is_empty() {
            return;
        }
        let mut events = self.event.stream();
        while let Some(_) = next(&mut events).await {
            if self.tickets.get() == 0 {
                break;
            }
        }
    }
}

pub struct Ticket {
    event: Rc<Event>,
    tickets: Rc<Cell<usize>>,
}

impl fmt::Debug for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ticket").finish()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.tickets.set(self.tickets.get() - 1);
        self.event.notify(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{counting_waker, poll_once, poll_with};
    use alloc::format;
    use core::{pin::pin, task::Poll};

    #[test]
    fn new_wait_group_is_empty() {
        let wg = WaitGroup::new();
        assert_eq!(wg.len(), 0);
        assert!(wg.is_empty());
    }

    #[test]
    fn default_matches_new() {
        let wg = WaitGroup::default();
        assert_eq!(wg.len(), 0);
        assert!(wg.is_empty());
    }

    #[test]
    fn add_increments_len() {
        let mut wg = WaitGroup::new();
        let _a = wg.add();
        assert_eq!(wg.len(), 1);
        let _b = wg.add();
        let _c = wg.add();
        assert_eq!(wg.len(), 3);
        assert!(!wg.is_empty());
    }

    #[test]
    fn dropping_ticket_decrements_len() {
        let mut wg = WaitGroup::new();
        let a = wg.add();
        let b = wg.add();

        drop(a);
        assert_eq!(wg.len(), 1);
        drop(b);
        assert_eq!(wg.len(), 0);
        assert!(wg.is_empty());
    }

    #[test]
    fn wait_returns_immediately_when_empty() {
        let mut wg = WaitGroup::new();
        pollster::block_on(wg.wait());
    }

    #[test]
    fn wait_completes_immediately_after_all_tickets_were_dropped() {
        let mut wg = WaitGroup::new();
        drop(wg.add());
        drop(wg.add());

        pollster::block_on(wg.wait());
    }

    #[test]
    fn wait_is_pending_while_a_ticket_is_outstanding() {
        let mut wg = WaitGroup::new();
        let _ticket = wg.add();

        let mut fut = pin!(wg.wait());
        assert_eq!(poll_once(fut.as_mut()), Poll::Pending);
    }

    #[test]
    fn wait_wakes_and_completes_when_ticket_is_dropped() {
        let mut wg = WaitGroup::new();
        let ticket = wg.add();
        let (waker, wakes) = counting_waker();

        let mut fut = pin!(wg.wait());
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Pending);
        assert_eq!(wakes.count(), 0);

        drop(ticket);
        assert_eq!(wakes.count(), 1);
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Ready(()));
    }

    /// Regression test for the `Event::notified` accounting bug (see
    /// `event::tests::notify_one_still_works_after_a_notification_was_consumed`):
    /// the second `Ticket` drop calls `notify(1)`, which used to be swallowed.
    #[test]
    fn wait_stays_pending_until_every_ticket_is_dropped() {
        let mut wg = WaitGroup::new();
        let a = wg.add();
        let b = wg.add();
        let (waker, _wakes) = counting_waker();

        let mut fut = pin!(wg.wait());
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Pending);

        drop(a);
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Pending);

        drop(b);
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Ready(()));
    }

    #[test]
    fn wait_completes_when_all_tickets_drop_before_next_poll() {
        let mut wg = WaitGroup::new();
        let a = wg.add();
        let b = wg.add();
        let c = wg.add();
        let (waker, _wakes) = counting_waker();

        let mut fut = pin!(wg.wait());
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Pending);

        drop(a);
        drop(b);
        drop(c);
        assert_eq!(poll_with(fut.as_mut(), &waker), Poll::Ready(()));
    }

    #[test]
    fn wait_group_can_be_reused_after_waiting() {
        let mut wg = WaitGroup::new();
        drop(wg.add());
        pollster::block_on(wg.wait());

        let ticket = wg.add();
        assert_eq!(wg.len(), 1);
        {
            let mut fut = pin!(wg.wait());
            assert_eq!(poll_once(fut.as_mut()), Poll::Pending);
        }
        drop(ticket);
        pollster::block_on(wg.wait());
        assert!(wg.is_empty());
    }

    #[test]
    fn debug_output_names_the_type() {
        let mut wg = WaitGroup::new();
        let ticket = wg.add();

        let wg_debug = format!("{wg:?}");
        assert!(wg_debug.contains("Waitgroup"));
        assert!(wg_debug.contains("tickets"));
        assert_eq!(format!("{ticket:?}"), "Ticket");
    }
}
