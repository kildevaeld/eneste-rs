use core::{borrow::Borrow, fmt};

use alloc::{
    rc::{Rc, Weak},
    string::String,
};

use crate::{Downgrade, Upgrade};

pub struct Atom(Rc<str>);

impl Atom {
    pub fn new(value: &str) -> Self {
        Self(Rc::from(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Atom {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Atom {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl core::ops::Deref for Atom {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for Atom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<str> for Atom {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<Atom> for str {
    fn eq(&self, other: &Atom) -> bool {
        self == other.as_str()
    }
}

impl<'a> From<&'a str> for Atom {
    fn from(value: &'a str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Atom {
    fn from(value: String) -> Self {
        Self::new(&value)
    }
}

#[derive(Clone)]
pub struct WeakAtom(Weak<str>);

impl Upgrade for WeakAtom {
    type Target = Atom;

    fn upgrade(&self) -> Option<Self::Target> {
        self.0.upgrade().map(|rc| Atom(rc))
    }
}

impl Downgrade for Atom {
    type Target = WeakAtom;

    fn downgrade(&self) -> Self::Target {
        WeakAtom(Rc::downgrade(&self.0))
    }
}
