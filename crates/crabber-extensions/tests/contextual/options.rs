use crabber_extensions::{
    background_jobs as jobs, delegate_task as delegate, python_repl as python,
};
use std::{collections::BTreeMap, path::Path, time::Duration};
pub fn jobs(root: &Path) -> jobs::Options {
    jobs::Options {
        shell_path: "/bin/sh".into(),
        shell_identity: "contextual-sh-v1".into(),
        environment: jobs::Environment {
            mode: jobs::EnvironmentMode::ExplicitOnly,
            identity: "contextual-env-v1".into(),
            overrides: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("FIXTURE_ROOT".into(), root.to_str().unwrap().into()),
            ]),
        },
        limits: jobs::Limits {
            max_running: 1,
            max_tracked: 4,
            max_command_bytes: 4096,
            max_working_directory_bytes: 1024,
            max_output_bytes_per_stream: 4096,
            max_environment_entries: 16,
            max_environment_bytes: 4096,
            default_timeout: Duration::from_secs(20),
            max_timeout: Duration::from_secs(30),
            terminate_grace: Duration::from_millis(100),
            kill_wait: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(3),
        },
    }
}
pub fn python(root: &Path) -> Option<python::Options> {
    Some(python_with_interpreter(
        root,
        super::owned_process::python()?,
    ))
}
pub fn python_with_interpreter(root: &Path, python_path: std::path::PathBuf) -> python::Options {
    python::Options {
        python_path,
        python_identity: "contextual-python-v1".into(),
        temp_root: root.join("temporary"),
        environment: python::Environment {
            mode: python::EnvironmentMode::ExplicitOnly,
            identity: "contextual-env-v1".into(),
            overrides: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("FIXTURE_ROOT".into(), root.to_str().unwrap().into()),
            ]),
        },
        limits: python::Limits {
            max_sessions: 3,
            max_queued_per_session: 2,
            max_code_bytes: 4096,
            max_output_bytes_per_stream: 4096,
            max_result_bytes: 4096,
            max_exception_bytes: 4096,
            max_environment_entries: 16,
            max_environment_bytes: 4096,
            default_timeout: Duration::from_secs(10),
            max_timeout: Duration::from_secs(30),
            runner_start_timeout: Duration::from_secs(10),
            terminate_grace: Duration::from_millis(100),
            kill_wait: Duration::from_secs(2),
            shutdown_grace: Duration::from_secs(3),
        },
    }
}
pub fn delegate(runner: delegate::Runner) -> delegate::Options {
    delegate::Options {
        runner_identity: "contextual-runner-v1".into(),
        runner,
        limits: delegate::Limits {
            max_task_bytes: 4096,
            max_profile_bytes: 64,
            max_result_bytes: 4096,
            max_in_flight: 1,
            max_wait: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(3),
            max_tracked: 4,
            max_background_duration: Duration::from_secs(30),
            max_wait_ids: 4,
            max_wait_reply_bytes: 4096,
        },
    }
}
