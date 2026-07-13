use core::fmt;

pub mod mpsc;
pub mod oneshot;

#[derive(Debug, PartialEq)]
pub struct ChannelError;

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "channel closed")
    }
}

impl core::error::Error for ChannelError {}
