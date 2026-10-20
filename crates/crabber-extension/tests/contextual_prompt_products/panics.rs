use super::*;
async fn case(typed: bool, mode: usize) {
    let seen = Seen::new();
    seen.mode.store(mode, Ordering::SeqCst);
    let (provider, host, _) = provider(&seen, typed, FactoryScope::Session, "original");
    let plan = provider.acquire_plan_in(&session()).await.unwrap();
    let (outcome, joined) = call(&plan.prompt_contributors).await;
    drop(plan);
    let closed = provider.close_all(BOUND).await;
    let node = host.0.lock().unwrap()[0].prompt_product(0).unwrap();
    let disposed = node.disposed(BOUND).await;
    let failed = mode >= 3;
    assert_eq!(joined.pending, 0);
    assert_eq!(
        !joined.failures.is_empty(),
        failed,
        "mode={mode} {joined:?}"
    );
    assert_eq!(closed.is_err(), failed);
    assert_eq!(disposed.is_err(), failed);
    assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(seen.futures.load(Ordering::SeqCst), usize::from(mode != 6));
    if mode == 0 {
        completed(outcome);
    } else {
        assert!(matches!(outcome, PromptContributionOutcome::Failed { .. }));
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_prompt_product_panic_child() {
    if std::env::var_os("CRABBER_PROMPT_PRODUCT_CHILD").is_none() {
        return;
    }
    for typed in [false, true] {
        for mode in [0, 1, 3, 4, 5, 6] {
            case(typed, mode).await;
        }
    }
}
#[test]
fn actual_product_literal_poll_and_drop_panics_do_not_double_unwind() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "panics::literal_prompt_product_panic_child",
            "--nocapture",
        ])
        .env("CRABBER_PROMPT_PRODUCT_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "status={:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
