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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{format, string::ToString};

    #[test]
    fn new_stores_string_contents() {
        let atom = Atom::new("hello");
        assert_eq!(atom.as_str(), "hello");
    }

    #[test]
    fn empty_and_unicode_strings_are_supported() {
        assert_eq!(Atom::new("").as_str(), "");
        assert!(Atom::new("").is_empty());
        assert_eq!(Atom::new("æøå 🦀").as_str(), "æøå 🦀");
    }

    #[test]
    fn deref_exposes_str_methods() {
        let atom = Atom::new("Hello World");
        assert_eq!(atom.len(), 11);
        assert!(atom.starts_with("Hello"));
        assert_eq!(atom.to_uppercase(), "HELLO WORLD");
    }

    #[test]
    fn as_ref_and_borrow_return_the_same_str() {
        let atom = Atom::new("value");
        let as_ref: &str = atom.as_ref();
        let borrowed: &str = core::borrow::Borrow::borrow(&atom);
        assert_eq!(as_ref, "value");
        assert_eq!(borrowed, "value");
    }

    #[test]
    fn display_prints_contents() {
        let atom = Atom::new("shown");
        assert_eq!(format!("{atom}"), "shown");
        assert_eq!(atom.to_string(), "shown");
    }

    #[test]
    fn atoms_compare_by_content() {
        assert!(Atom::new("a") == Atom::new("a"));
        assert!(Atom::new("a") != Atom::new("b"));
    }

    #[test]
    fn atom_compares_with_str_in_both_directions() {
        let atom = Atom::new("abc");
        assert!(atom == *"abc");
        assert!(*"abc" == atom);
        assert!(atom != *"abd");
        assert!(*"abd" != atom);
    }

    #[test]
    fn from_str_and_from_string_build_atoms() {
        let from_str = Atom::from("x");
        let from_string = Atom::from(String::from("x"));
        assert!(from_str == from_string);
        assert_eq!(from_string.as_str(), "x");
    }

    #[test]
    fn weak_atom_upgrades_while_atom_is_alive() {
        let atom = Atom::new("alive");
        let weak = atom.downgrade();

        let upgraded = weak.upgrade().expect("atom is still alive");
        assert!(upgraded == atom);
    }

    #[test]
    fn weak_atom_fails_to_upgrade_after_atom_is_dropped() {
        let atom = Atom::new("gone");
        let weak = atom.downgrade();
        drop(atom);

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn upgraded_atom_keeps_value_alive() {
        let atom = Atom::new("kept");
        let weak = atom.downgrade();
        let upgraded = weak.upgrade().unwrap();
        drop(atom);

        assert_eq!(weak.upgrade().as_deref(), Some("kept"));
        drop(upgraded);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn cloned_weak_atom_behaves_like_the_original() {
        let atom = Atom::new("clone");
        let weak = atom.downgrade();
        let weak_clone = weak.clone();

        assert!(weak_clone.upgrade().is_some());
        drop(atom);
        assert!(weak.upgrade().is_none());
        assert!(weak_clone.upgrade().is_none());
    }
}
