use crate::StoreError;
use serde::{Deserialize, Serialize};
fn invalid() -> StoreError {
    StoreError::Validation("invalid snapshot continuation".into())
}
/// Backend-private fixed profiles; this is not an arbitrary public token signer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Profile {
    Raw,
    AggregateV2,
}
impl Profile {
    pub const fn token_cap(self) -> usize {
        match self {
            Self::Raw => 2048,
            Self::AggregateV2 => 4096,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AggregateBoundary<T> {
    version: u32,
    profile: String,
    boundary: T,
}
const AGGREGATE_PROFILE: &str = "public-session.aggregate.v2";
pub(crate) fn profile_json<T: Serialize>(
    boundary: &T,
    profile: Profile,
) -> Result<String, StoreError> {
    match profile {
        Profile::Raw => serde_json::to_string(boundary),
        Profile::AggregateV2 => serde_json::to_string(&AggregateBoundary {
            version: 2,
            profile: AGGREGATE_PROFILE.to_owned(),
            boundary,
        }),
    }
    .map_err(|_| invalid())
}
pub(crate) fn profile_boundary<T: serde::de::DeserializeOwned>(
    json: &str,
    profile: Profile,
) -> Result<T, StoreError> {
    match profile {
        Profile::Raw => serde_json::from_str(json).map_err(|_| invalid()),
        Profile::AggregateV2 => {
            let envelope: AggregateBoundary<T> =
                serde_json::from_str(json).map_err(|_| invalid())?;
            if envelope.version != 2 || envelope.profile != AGGREGATE_PROFILE {
                return Err(invalid());
            }
            Ok(envelope.boundary)
        }
    }
}
