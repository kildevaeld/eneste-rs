#![no_std]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod emitter;
pub mod event;

mod atom;
pub mod cell;
pub mod channel;

#[cfg(feature = "executor")]
pub mod executor;
pub mod lock;
pub mod poll_lock;
pub mod spawner;
mod upgrade;

pub use self::atom::{Atom, WeakAtom};
pub use self::upgrade::{Downgrade, Upgrade};
