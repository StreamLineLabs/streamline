//! KIP-848 Error Codes
//!
//! Error codes specific to the new consumer protocol.

use std::fmt;

/// KIP-848 specific error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kip848Error {
    /// No error
    None = 0,

    /// Group coordinator not available
    CoordinatorNotAvailable = 15,

    /// Not the coordinator for this group
    NotCoordinator = 16,

    /// Unknown member ID
    UnknownMemberId = 25,

    /// Fenced member epoch - member's epoch is stale
    /// Client must rejoin with a new member ID
    FencedMemberEpoch = 110,

    /// Unsupported assignor - the requested assignor is not supported
    UnsupportedAssignor = 111,

    /// Unreleased instance ID - static membership instance ID is already in use
    UnreleasedInstanceId = 112,

    /// Group max size reached
    GroupMaxSizeReached = 81,

    /// Stale member epoch - member's epoch is ahead of server
    StaleMemberEpoch = 114,
}

impl Kip848Error {
    /// Convert to i16 error code for Kafka protocol
    pub fn code(&self) -> i16 {
        *self as i16
    }

    /// Create from error code
    pub fn from_code(code: i16) -> Option<Self> {
        match code {
            0 => Some(Self::None),
            15 => Some(Self::CoordinatorNotAvailable),
            16 => Some(Self::NotCoordinator),
            25 => Some(Self::UnknownMemberId),
            81 => Some(Self::GroupMaxSizeReached),
            110 => Some(Self::FencedMemberEpoch),
            111 => Some(Self::UnsupportedAssignor),
            112 => Some(Self::UnreleasedInstanceId),
            114 => Some(Self::StaleMemberEpoch),
            _ => None,
        }
    }

    /// Check if error requires member to rejoin
    pub fn requires_rejoin(&self) -> bool {
        matches!(
            self,
            Self::FencedMemberEpoch | Self::UnknownMemberId | Self::StaleMemberEpoch
        )
    }

    /// Check if error is retriable
    pub fn is_retriable(&self) -> bool {
        matches!(self, Self::CoordinatorNotAvailable | Self::NotCoordinator)
    }
}

impl fmt::Display for Kip848Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "No error"),
            Self::CoordinatorNotAvailable => write!(f, "Group coordinator not available"),
            Self::NotCoordinator => write!(f, "Not the coordinator for this group"),
            Self::UnknownMemberId => write!(f, "Unknown member ID"),
            Self::FencedMemberEpoch => write!(f, "Fenced member epoch - rejoin required"),
            Self::UnsupportedAssignor => write!(f, "Unsupported assignor"),
            Self::UnreleasedInstanceId => write!(f, "Instance ID already in use"),
            Self::GroupMaxSizeReached => write!(f, "Group maximum size reached"),
            Self::StaleMemberEpoch => write!(f, "Stale member epoch"),
        }
    }
}

impl std::error::Error for Kip848Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(Kip848Error::None.code(), 0);
        assert_eq!(Kip848Error::FencedMemberEpoch.code(), 110);
        assert_eq!(Kip848Error::UnsupportedAssignor.code(), 111);
    }

    #[test]
    fn test_from_code() {
        assert_eq!(Kip848Error::from_code(0), Some(Kip848Error::None));
        assert_eq!(
            Kip848Error::from_code(110),
            Some(Kip848Error::FencedMemberEpoch)
        );
        assert_eq!(Kip848Error::from_code(999), None);
    }

    #[test]
    fn test_requires_rejoin() {
        assert!(Kip848Error::FencedMemberEpoch.requires_rejoin());
        assert!(Kip848Error::UnknownMemberId.requires_rejoin());
        assert!(!Kip848Error::CoordinatorNotAvailable.requires_rejoin());
    }
}
