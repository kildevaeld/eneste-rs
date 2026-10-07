#![no_std]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod emitter;
pub mod event;

mod atom;
pub mod cell;
pub mod channel;
pub mod ref_cell;
pub mod util;
pub mod waitgroup;

#[cfg(feature = "executor")]
pub mod executor;
pub mod lock;
pub mod poll_lock;
pub mod spawner;
mod upgrade;

#[cfg(test)]
mod test_util;

pub use self::atom::{Atom, WeakAtom};
pub use self::upgrade::{Downgrade, Upgrade};
