//! Ownership of the required schedule; partial runs never qualify the suite.
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    QualityContracts,
    QualityArtifacts,
    NativeLibraries(Quarter),
    NativeRemaining(Quarter),
}

impl Part {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::QualityContracts => "quality-contracts",
            Self::QualityArtifacts => "quality-artifacts",
            Self::NativeLibraries(quarter) => quarter.library_name(),
            Self::NativeRemaining(quarter) => quarter.remaining_name(),
        }
    }
}

pub(crate) fn native_slots() -> impl Iterator<Item = Part> {
    let quarters = [Quarter::One, Quarter::Two, Quarter::Three, Quarter::Four];
    quarters
        .into_iter()
        .map(Part::NativeLibraries)
        .chain(quarters.into_iter().map(Part::NativeRemaining))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PartFamily {
    #[cfg(feature = "postgres-check")]
    QualityContracts,
    QualityArtifacts,
    NativeLibraries,
    NativeRemaining,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Quarter {
    One,
    Two,
    Three,
    Four,
}

impl Quarter {
    pub(crate) fn index(self) -> usize {
        match self {
            Self::One => 1,
            Self::Two => 2,
            Self::Three => 3,
            Self::Four => 4,
        }
    }
    fn library_name(self) -> &'static str {
        match self {
            Self::One => "native-library-1",
            Self::Two => "native-library-2",
            Self::Three => "native-library-3",
            Self::Four => "native-library-4",
        }
    }
    fn remaining_name(self) -> &'static str {
        match self {
            Self::One => "native-remaining-1",
            Self::Two => "native-remaining-2",
            Self::Three => "native-remaining-3",
            Self::Four => "native-remaining-4",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResourcePolicy {
    Direct,
    OwnedPostgres,
    OwnedPostgresWithDemos,
}

impl Part {
    pub(crate) fn native_metadata(self, value: &mut Value) {
        let (family, field, quarter) = match self {
            Self::NativeLibraries(q) => ("native-libraries", "library_quarter", q),
            Self::NativeRemaining(q) => ("native-remaining", "target_quarter", q),
            _ => return,
        };
        value["part_family"] = json!(family);
        value[field] = json!(quarter.index());
        value[format!("{field}_count")] = json!(4);
        value["full_coverage_policy_executed"] = json!(false);
        value["requires_owned_postgres"] = json!(true);
        value["postgres_demos"] = json!([]);
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn family(self) -> PartFamily {
        match self {
            Self::QualityContracts => PartFamily::QualityContracts,
            Self::QualityArtifacts => PartFamily::QualityArtifacts,
            Self::NativeLibraries(_) => PartFamily::NativeLibraries,
            Self::NativeRemaining(_) => PartFamily::NativeRemaining,
        }
    }
    pub(crate) fn resources(self) -> ResourcePolicy {
        match self {
            Self::QualityContracts => ResourcePolicy::Direct,
            Self::QualityArtifacts => ResourcePolicy::OwnedPostgresWithDemos,
            Self::NativeLibraries(_) | Self::NativeRemaining(_) => ResourcePolicy::OwnedPostgres,
        }
    }
    pub(crate) fn owns_postgres(self) -> bool {
        self.resources() != ResourcePolicy::Direct
    }
    #[cfg(feature = "postgres-check")]
    pub(crate) fn owns(self, owner: Owner) -> bool {
        matches!(
            (self, owner),
            (Self::QualityContracts, Owner::QualityContracts)
                | (Self::QualityArtifacts, Owner::QualityArtifacts)
        )
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

#[derive(Clone, Default)]
pub(crate) enum GateRole {
    #[default]
    Unpartitioned,
    Required(Owner, BTreeSet<PartFamily>),
    #[cfg(feature = "postgres-check")]
    Preparation(Part),
    #[cfg(feature = "postgres-check")]
    NativeContribution(PartFamily),
}

pub(crate) struct Scope {
    pub(crate) part: Part,
    pub(crate) ownership: Value,
    pub(crate) demos: Value,
}

impl Scope {
    pub(crate) fn metadata(&self) -> Value {
        let mut value = json!({
            "coverage_scope": self.part.name(), "qualified": false,
            "full_native_suite_executed": false,
            "requires_owned_postgres": self.part.owns_postgres(),
            "owned_postgres_run": if self.part.owns_postgres() { Value::Null } else { json!(false) },
            "ownership": self.ownership, "postgres_demos": self.demos,
        });
        self.part.native_metadata(&mut value);
        value
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
