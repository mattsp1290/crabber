//! Ownership of the required schedule; partial runs never qualify the suite.
#[cfg(feature = "postgres-check")]
use serde_json::{Value, json};
#[cfg(feature = "postgres-check")]
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    QualityContracts,
}

#[cfg(feature = "postgres-check")]
impl Part {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::QualityContracts => "quality-contracts",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    QualityContracts,
    #[cfg(feature = "postgres-check")]
    QualityArtifacts,
    #[cfg(feature = "postgres-check")]
    Native,
}

impl Owner {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::QualityContracts => "quality-contracts",
            #[cfg(feature = "postgres-check")]
            Self::QualityArtifacts => "quality-artifacts",
            #[cfg(feature = "postgres-check")]
            Self::Native => "native",
        }
    }
}

#[cfg(feature = "postgres-check")]
pub(crate) fn ownership<'a>(inventory: impl IntoIterator<Item = &'a Value>) -> Value {
    let mut union = BTreeSet::new();
    let mut groups = json!({"quality-contracts": [], "quality-artifacts": [], "native": []});
    for gate in inventory {
        let id = gate["id"].as_str().expect("gate ID");
        assert!(union.insert(id.to_owned()), "duplicate owned gate {id}");
        let owner = gate["owner"].as_str().expect("unowned required gate");
        groups
            .get_mut(owner)
            .expect("unknown required gate owner")
            .as_array_mut()
            .unwrap()
            .push(json!(id));
    }
    assert!(!union.is_empty(), "empty required gate ownership");
    for group in groups.as_object().unwrap().values() {
        assert!(
            !group.as_array().unwrap().is_empty(),
            "empty required group"
        );
    }
    let projected: BTreeSet<_> = groups
        .as_object()
        .unwrap()
        .values()
        .flat_map(|group| group.as_array().unwrap())
        .map(|id| id.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(projected, union, "required gate ownership union differs");
    groups
}
