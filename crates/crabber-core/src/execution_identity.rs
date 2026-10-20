//! Public correlation and monotonic generations, never execution authority.
use crate::CoreError;
use serde::{Deserialize, Serialize};

macro_rules! generation {
    ($name:ident, $initial:expr, $description:literal) => {
        #[doc = $description]
        /// Unstable until the durable-host contract is qualified. Values fit a
        /// signed database BIGINT and cannot wrap. Possession grants no fence.
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(try_from = "u64", into = "u64")]
        pub struct $name(u64);
        impl $name {
            pub const INITIAL: Self = Self($initial);
            /// Validate a persisted generation/revision.
            /// # Errors
            /// Rejects values outside this counter's database-representable range.
            pub fn new(value: u64) -> Result<Self, CoreError> {
                value.try_into()
            }
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
            /// Advance once without wraparound or reusing the final generation.
            /// # Errors
            /// Exhaustion fails closed and requires an explicit owner decision.
            pub fn next(self) -> Result<Self, CoreError> {
                Self::new(self.0.checked_add(1).ok_or_else(|| {
                    CoreError::Validation(concat!(stringify!($name), " exhausted").into())
                })?)
            }
        }
        impl TryFrom<u64> for $name {
            type Error = CoreError;
            fn try_from(value: u64) -> Result<Self, Self::Error> {
                if ($initial..=i64::MAX as u64).contains(&value) {
                    Ok(Self(value))
                } else {
                    Err(CoreError::Validation(
                        concat!("invalid ", stringify!($name)).into(),
                    ))
                }
            }
        }
        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

generation!(
    SessionRevision,
    0,
    "Revision of all public-visible session mutations; distinct from `EventCursor`."
);
generation!(
    AttemptGeneration,
    1,
    "Generation of an owned execution attempt within one logical `RunId`."
);
generation!(
    ContinuationGeneration,
    1,
    "Generation binding a complete continuation response set to its suspension."
);
generation!(
    PhaseGeneration,
    1,
    "Generation of one explicitly claimed tool phase, independently fenced by storage."
);

generation!(
    SessionOwnerGeneration,
    1,
    "Generation of an independently fenced session resource or preparation owner."
);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persisted_generations_reject_zero_and_exhaustion_without_wrapping() {
        assert_eq!(SessionRevision::INITIAL.get(), 0);
        assert_eq!(SessionRevision::INITIAL.next().unwrap().get(), 1);
        assert_eq!(AttemptGeneration::INITIAL.get(), 1);
        assert_eq!(ContinuationGeneration::INITIAL.get(), 1);
        assert_eq!(PhaseGeneration::INITIAL.get(), 1);
        assert_eq!(SessionOwnerGeneration::INITIAL.get(), 1);
        assert!(serde_json::from_str::<AttemptGeneration>("0").is_err());
        assert!(serde_json::from_str::<ContinuationGeneration>("0").is_err());
        assert!(serde_json::from_str::<PhaseGeneration>("0").is_err());
        assert!(serde_json::from_str::<SessionOwnerGeneration>("0").is_err());
        for invalid in ["-1", "1.5", "9223372036854775808", "18446744073709551615"] {
            assert!(serde_json::from_str::<SessionRevision>(invalid).is_err());
            assert!(serde_json::from_str::<AttemptGeneration>(invalid).is_err());
            assert!(serde_json::from_str::<ContinuationGeneration>(invalid).is_err());
            assert!(serde_json::from_str::<PhaseGeneration>(invalid).is_err());
            assert!(serde_json::from_str::<SessionOwnerGeneration>(invalid).is_err());
        }
        let last = i64::MAX as u64;
        assert!(SessionRevision::new(last).unwrap().next().is_err());
        assert!(AttemptGeneration::new(last).unwrap().next().is_err());
        assert!(ContinuationGeneration::new(last).unwrap().next().is_err());
        assert!(PhaseGeneration::new(last).unwrap().next().is_err());
        assert!(SessionOwnerGeneration::new(last).unwrap().next().is_err());
        assert_eq!(
            serde_json::to_string(&AttemptGeneration::new(17).unwrap()).unwrap(),
            "17"
        );
    }
}
