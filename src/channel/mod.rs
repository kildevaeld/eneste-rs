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

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{format, string::ToString};

    #[test]
    fn display_says_channel_closed() {
        assert_eq!(ChannelError.to_string(), "channel closed");
    }

    #[test]
    fn debug_and_equality() {
        assert_eq!(format!("{ChannelError:?}"), "ChannelError");
        assert_eq!(ChannelError, ChannelError);
    }

    #[test]
    fn implements_error_trait() {
        fn assert_error<E: core::error::Error>(_: &E) {}
        assert_error(&ChannelError);
    }
}
