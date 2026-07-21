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
