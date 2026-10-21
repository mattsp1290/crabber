//! Terminal controller histories whose sticky owners intentionally survive
//! runtime shutdown need an actual process boundary before database cleanup.

use super::{DatabaseMode, FixtureError, Result, TestDatabase};
use std::{
    env,
    io::Write,
    process::{Command, Stdio},
};

const CHILD: &str = "CRABBER_TEST_HISTORY_CHILD";

impl TestDatabase {
    /// Run the original history in a fresh controller process. The parent owns
    /// its database; the marked child attaches and never provisions or drops it.
    /// A post-history acknowledgement prevents a zero-match libtest invocation
    /// from counting as success. Process exit closes terminal sticky owners
    /// without reporting a native Join or changing their persisted uncertainty.
    /// Call from a synchronous test and retain its original runtime configuration.
    /// # Errors
    /// Fails on configuration, provisioning, child failure, missing acknowledgement
    /// or normal connection drain. The outer runner supervises descendants.
    pub fn run_in_process(
        mode: DatabaseMode,
        test_name: &str,
        mut history: tokio::runtime::Builder,
        journey: impl AsyncFnOnce(String),
    ) -> Result<()> {
        if env::var(CHILD).as_deref() == Ok(test_name) {
            let runtime = history
                .enable_all()
                .build()
                .map_err(|_| FixtureError("cannot create controller history runtime"))?;
            let endpoint = runtime.block_on(Self::attached_url())?;
            runtime.block_on(journey(endpoint));
            drop(runtime);
            let identity = env::var("CRABBER_TEST_POSTGRES_CHILD_DATABASE")
                .map_err(|_| FixtureError("missing controller database identity"))?;
            // libtest may have printed a progress prefix without a newline.
            println!("\ncrabber-controller-history-complete={identity}");
            return Ok(());
        }
        if env::var_os(CHILD).is_some() {
            return Err(FixtureError(
                "controller history marker does not match requested test",
            ));
        }
        if !Self::available()? {
            return Ok(());
        }
        let admin = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| FixtureError("cannot create fixture administrative runtime"))?;
        let database = admin.block_on(Self::create(mode))?;
        let output = Command::new(
            env::current_exe()
                .map_err(|_| FixtureError("cannot locate controller test executable"))?,
        )
        .args(["--exact", test_name, "--nocapture", "--test-threads", "1"])
        .envs(Self::child_environment(&database.url())?)
        .env(CHILD, test_name)
        .stderr(Stdio::inherit())
        .output()
        .map_err(|_| FixtureError("cannot run controller history process"))?;
        std::io::stdout()
            .write_all(&output.stdout)
            .map_err(|_| FixtureError("cannot forward controller history output"))?;
        if !output.status.success() {
            return Err(FixtureError("controller history process failed"));
        }
        if !acknowledged(&output.stdout, database.identity()) {
            return Err(FixtureError(
                "controller history did not acknowledge its parent database",
            ));
        }
        admin.block_on(database.finish())
    }
}

fn acknowledged(stdout: &[u8], identity: &str) -> bool {
    let acknowledgement = format!("crabber-controller-history-complete={identity}");
    stdout
        .split(|byte| *byte == b'\n')
        .any(|line| line == acknowledgement.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::acknowledged;

    #[test]
    fn successful_zero_match_and_foreign_controller_output_are_not_history_evidence() {
        let zero_match = b"running 0 tests\n\ntest result: ok. 0 passed; 0 failed\n";
        assert!(!acknowledged(zero_match, "ct_parent"));
        assert!(!acknowledged(
            b"crabber-controller-history-complete=ct_foreign\n",
            "ct_parent"
        ));
        assert!(!acknowledged(
            b"prefix-crabber-controller-history-complete=ct_parent\n",
            "ct_parent"
        ));
        assert!(acknowledged(
            b"running 1 test\ncrabber-controller-history-complete=ct_parent\n",
            "ct_parent"
        ));
    }
}
