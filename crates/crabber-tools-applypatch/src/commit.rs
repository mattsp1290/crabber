use crate::{
    parser::Kind,
    preflight::{self, Change, Plan},
    result,
};
use crabber_tools_core::{
    ToolError, WorkspaceRoot,
    atomic::{self, Install},
};
use serde_json::{Value, json};
use std::path::Path;
use tokio_util::sync::CancellationToken;
fn apply_one(root: &WorkspaceRoot, change: &Change) -> Result<(), ToolError> {
    #[cfg(test)]
    COMMIT_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    if change.op.kind == Kind::Delete {
        return root
            .dir()
            .remove_file(&change.src)
            .map_err(preflight::error);
    }
    let parent = change
        .dst
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    root.dir()
        .create_dir_all(parent)
        .map_err(preflight::error)?;
    let parent = root.dir().open_dir(parent).map_err(preflight::error)?;
    let parent_owner = parent.dir_metadata().map_err(preflight::error)?;
    let owner = change.owner.as_ref().unwrap_or(&parent_owner);
    let install = if change.op.kind == Kind::Add || change.op.new_path.is_some() {
        Install::CreateNew
    } else {
        Install::Replace
    };
    atomic::write_sibling(
        &parent,
        change.dst.file_name().expect("preflighted basename"),
        &change.bytes,
        owner,
        change.mode,
        install,
    )
    .map_err(preflight::error)?;
    if change.op.new_path.is_some() {
        root.dir()
            .remove_file(&change.src)
            .map_err(preflight::error)?;
    }
    Ok(())
}
pub(crate) fn apply(root: &WorkspaceRoot, mut plan: Plan, cancel: &CancellationToken) -> Value {
    for (i, change) in plan.changes.iter().enumerate() {
        // Stop between operations; never interrupt an atomic installation.
        let result = preflight::check(cancel).and_then(|_| apply_one(root, change));
        if let Err(error) = result {
            plan.files[i].status = "failed";
            return result::failure(error, plan.files, true);
        }
        plan.files[i].status = "applied";
    }
    json!({"outcome":"succeeded","files":plan.files,"partial":false})
}

#[cfg(test)]
thread_local! {
    static COMMIT_HOOK:std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const {std::cell::RefCell::new(None)};
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser;
    use crabber_tools_core::*;
    use std::{sync::Arc, time::Duration};
    fn planned(root: &WorkspaceRoot, patch: &str) -> Plan {
        preflight::plan(
            root,
            parser::parse(patch).unwrap(),
            &CancellationToken::new(),
        )
        .unwrap_or_else(|_| panic!("valid plan"))
    }
    #[test]
    fn commit_failure_reports_applied_failed_and_partial() {
        let temp = tempfile::tempdir().unwrap();
        let root = WorkspaceRoot::open(temp.path()).unwrap();
        let plan = planned(
            &root,
            "*** Begin Patch\n*** Add File: first\n+one\n*** Add File: sub/second\n+two\n*** End Patch",
        );
        std::fs::write(temp.path().join("sub"), "external file").unwrap();
        let result = apply(&root, plan, &CancellationToken::new());
        assert_eq!(result["partial"], true);
        assert_eq!(result["outcome"], "failed");
        assert_eq!(result["files"][0]["status"], "applied");
        assert_eq!(result["files"][1]["status"], "failed");
        assert_eq!(
            std::fs::read_to_string(temp.path().join("first")).unwrap(),
            "one\n"
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("sub")).unwrap(),
            "external file"
        );
    }
    #[test]
    fn move_cleanup_failure_and_destination_race_are_partial() {
        let temp = tempfile::tempdir().unwrap();
        let root = WorkspaceRoot::open(temp.path()).unwrap();
        std::fs::write(temp.path().join("src"), "source\n").unwrap();
        let plan = planned(
            &root,
            "*** Begin Patch\n*** Update File: src\n*** Move to: dst\n*** End Patch",
        );
        std::fs::remove_file(temp.path().join("src")).unwrap();
        std::fs::create_dir(temp.path().join("src")).unwrap();
        let result = apply(&root, plan, &CancellationToken::new());
        assert_eq!(result["partial"], true);
        assert_eq!(result["files"][0]["status"], "failed");
        assert_eq!(
            std::fs::read_to_string(temp.path().join("dst")).unwrap(),
            "source\n"
        );
        let plan = planned(
            &root,
            "*** Begin Patch\n*** Add File: race\n+patch\n*** End Patch",
        );
        std::fs::write(temp.path().join("race"), "external").unwrap();
        let result = apply(&root, plan, &CancellationToken::new());
        assert_eq!(result["partial"], true);
        assert_eq!(result["outcome"], "failed");
        assert_eq!(
            std::fs::read_to_string(temp.path().join("race")).unwrap(),
            "external"
        );
        assert!(std::fs::read_dir(temp.path()).unwrap().all(|e| {
            !e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".crabber-tools-tmp-")
        }));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_committing_worker_retains_guards_until_atomic_write_finishes() {
        let temp = tempfile::tempdir().unwrap();
        let root = WorkspaceRoot::open(temp.path()).unwrap();
        let plan = planned(
            &root,
            "*** Begin Patch\n*** Add File: first\n+one\n*** Add File: second\n+two\n*** End Patch",
        );
        let limits = Limits {
            max_in_flight: 1,
            max_blocking_wait: Duration::from_secs(2),
        };
        let capacity = Capacity::new(&limits).unwrap();
        let lock = RootLock::for_root(&root);
        let cancel = CancellationToken::new();
        let guards = acquire_mutating(&lock, &capacity, &limits, &cancel)
            .await
            .unwrap();
        let (started, start) = tokio::sync::oneshot::channel();
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let release = gate.clone();
        let child_cancel = cancel.clone();
        let child_root = root.clone();
        let worker = tokio::spawn(async move {
            let scan_cancel = child_cancel.clone();
            run_blocking(&child_cancel, move || {
                let _guards = guards;
                COMMIT_HOOK.with(|h| {
                    *h.borrow_mut() = Some(Box::new(move || {
                        started.send(()).unwrap();
                        let (lock, cv) = &*gate;
                        let mut go = lock.lock().unwrap();
                        while !*go {
                            go = cv.wait(go).unwrap();
                        }
                    }))
                });
                apply(&child_root, plan, &scan_cancel)
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), start)
            .await
            .unwrap()
            .unwrap();
        cancel.cancel();
        assert!(worker.await.unwrap().is_err());
        let short = Limits {
            max_blocking_wait: Duration::from_millis(10),
            ..limits.clone()
        };
        assert!(matches!(
            acquire_mutating(&lock, &capacity, &short, &CancellationToken::new()).await,
            Err(AcquireError::Unavailable)
        ));
        {
            let (lock, cv) = &*release;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        let _recovered = acquire_mutating(&lock, &capacity, &limits, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(temp.path().join("first")).unwrap(),
            "one\n"
        );
        assert!(!temp.path().join("second").exists());
    }
}
