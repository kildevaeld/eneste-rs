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
