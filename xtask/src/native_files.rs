#[cfg(feature = "postgres-check")]
use crate::parts::Quarter;
#[cfg(feature = "postgres-check")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "postgres-check")]
use std::path::{Path, PathBuf};

pub(crate) const METADATA: &str = "cargo-metadata.json";

#[cfg(feature = "postgres-check")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum Family {
    #[serde(rename = "native-libraries")]
    Libraries,
    #[serde(rename = "native-remaining")]
    Remaining,
}

#[cfg(feature = "postgres-check")]
impl Family {
    pub(crate) fn prefix(self) -> &'static str {
        match self {
            Self::Libraries => "library",
            Self::Remaining => "remaining",
        }
    }
}

#[cfg(feature = "postgres-check")]
#[derive(Clone, Copy)]
pub(crate) enum Run {
    LibraryQuarter,
    LibraryRole,
    Remaining,
}

#[cfg(feature = "postgres-check")]
impl Run {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::LibraryQuarter => "native-library-quarter",
            Self::LibraryRole => "native-library-role",
            Self::Remaining => "native-remaining",
        }
    }

    pub(crate) fn path(self, directory: &Path) -> PathBuf {
        directory.join(self.name())
    }
}

#[cfg(feature = "postgres-check")]
#[derive(Clone, Copy)]
pub(crate) enum File {
    Metadata,
    Fixtures,
    RustcVersion,
    DockerVersion,
    WasmToolsVersion,
    Compiler(Family),
    CompilerInput,
    Binaries(Family),
    Targets(Family),
    TargetPlan,
    Cases(Family),
    Selected(Family),
    Groups(Family),
    Owner(Family),
    OwnerJournal(Family),
    RoleCases,
    Execution(Run),
    Stdout(Run),
    Stderr(Run),
}

#[cfg(feature = "postgres-check")]
impl File {
    pub(crate) fn name(self) -> String {
        match self {
            Self::Metadata => METADATA.into(),
            Self::Fixtures => "fixtures.jsonl".into(),
            Self::RustcVersion => "rustc-version.txt".into(),
            Self::DockerVersion => "docker-version.txt".into(),
            Self::WasmToolsVersion => "wasm-tools-version.txt".into(),
            Self::Compiler(family) => format!("{}-artifacts.jsonl", family.prefix()),
            Self::CompilerInput => "remaining-compiler-input.json".into(),
            Self::Binaries(family) => format!("{}-binaries.json", family.prefix()),
            Self::Targets(family) => format!("{}-targets.json", family.prefix()),
            Self::TargetPlan => "remaining-target-plan.json".into(),
            Self::Cases(family) => format!("{}-tests.json", family.prefix()),
            Self::Selected(Family::Libraries) => "native-library-quarter-tests.json".into(),
            Self::Selected(Family::Remaining) => "remaining-selected-tests.json".into(),
            Self::Groups(family) => format!("{}-resource-groups.json", family.prefix()),
            Self::Owner(family) => format!("{}-migration-owner.json", family.prefix()),
            Self::OwnerJournal(family) => {
                format!("{}-migration-owner-artifacts.jsonl", family.prefix())
            }
            Self::RoleCases => "native-library-role-tests.json".into(),
            Self::Execution(run) => format!("{}-execution.json", run.name()),
            Self::Stdout(run) => format!("{}.stdout.log", run.name()),
            Self::Stderr(run) => format!("{}.stderr.log", run.name()),
        }
    }

    pub(crate) fn path(self, directory: &Path) -> PathBuf {
        directory.join(self.name())
    }

    pub(crate) fn role(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Fixtures => "fixture-ledger",
            Self::RustcVersion => "rustc-version",
            Self::DockerVersion => "docker-version",
            Self::WasmToolsVersion => "wasm-tools-version",
            Self::Compiler(_) => "compiler-journal",
            Self::CompilerInput => "compiler-input",
            Self::Binaries(_) => "binaries",
            Self::Targets(_) => "targets",
            Self::TargetPlan => "target-plan",
            Self::Cases(_) => "unpartitioned-cases",
            Self::Selected(_) => "selected-cases",
            Self::Groups(_) => "groups",
            Self::Owner(_) => "migration-owner",
            Self::OwnerJournal(_) => "migration-journal",
            Self::RoleCases => "role-cases",
            Self::Execution(Run::LibraryRole) => "role-execution",
            Self::Stdout(Run::LibraryRole) => "role-stdout",
            Self::Stderr(Run::LibraryRole) => "role-stderr",
            Self::Execution(_) => "execution",
            Self::Stdout(_) => "stdout",
            Self::Stderr(_) => "stderr",
        }
    }
}

#[cfg(feature = "postgres-check")]
pub(crate) fn files(family: Family, quarter: Quarter) -> Vec<File> {
    let run = match family {
        Family::Libraries => Run::LibraryQuarter,
        Family::Remaining => Run::Remaining,
    };
    let mut files = vec![
        File::Metadata,
        File::Fixtures,
        File::RustcVersion,
        File::DockerVersion,
        File::WasmToolsVersion,
        File::Compiler(family),
        File::Binaries(family),
        File::Targets(family),
        File::Cases(family),
        File::Selected(family),
        File::Groups(family),
        File::Owner(family),
        File::OwnerJournal(family),
        File::Execution(run),
        File::Stdout(run),
        File::Stderr(run),
    ];
    if family == Family::Remaining {
        files.extend([File::CompilerInput, File::TargetPlan]);
    } else if quarter == Quarter::One {
        files.extend([
            File::RoleCases,
            File::Execution(Run::LibraryRole),
            File::Stdout(Run::LibraryRole),
            File::Stderr(Run::LibraryRole),
        ]);
    }
    files
}
