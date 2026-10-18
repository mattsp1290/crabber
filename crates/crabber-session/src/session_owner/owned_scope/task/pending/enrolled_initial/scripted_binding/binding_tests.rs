use super::*;
use crate::MemoryStore;
use serde_json::json;

fn source(responses: &[&str]) -> LibraryScriptedSource {
    LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "p".into(),
            model_id: "m".into(),
        },
        responses.iter().map(|value| (*value).to_owned()).collect(),
    )
    .unwrap()
}

fn plan(bound: &mut crate::BoundKeyedAdmitRequest) {
    let components: Vec<ComponentIdentity> =
        serde_json::from_value(bound.binding.runtime_semantics[12].clone()).unwrap();
    bound.binding.plan_fingerprint =
        InputFingerprint::new(compute_fingerprint(&components).to_string()).unwrap();
}

#[tokio::test]
async fn genuine_previous_scripted_profile_cannot_gain_retirement_tail() {
    const OLD_BEHAVIOR: &[u8] = b"crabber.pending-initial.scripted-behavior.v1:one-turn:no-tools:no-prompts:no-guards:no-provider-factories:no-host-callbacks:no-compaction:no-heartbeat:canonical-attempt-prelude:permission-before-run-started:five-lifecycle-events:one-assistant-message:drain-before-conclusion:paired-close";
    for keyed in [false, true] {
        let store = MemoryStore::new();
        let source = source(&["response"]);
        let (_, original) = fixture::prepared_with(&store, keyed, &source, |bound| {
            let old = InputFingerprint::new(format!("{:x}", Sha256::digest(OLD_BEHAVIOR))).unwrap();
            bound.binding.behavior_fingerprint = old.clone();
            bound.binding.runtime_semantics[12][1]["version"] = json!(old.as_str());
            bound.binding.runtime_semantics[12][2] = json!({
                "id": "contract:crabber/pending-initial-scripted.v1",
                "version": "1",
            });
            plan(bound);
        })
        .await;
        // The old profile is frozen before the genuine root/child registration,
        // activation and actual task join. No original is rewritten afterwards.
        original.verify_original_intent().unwrap();
        assert_eq!(
            verify(&source, &original, &mut Proof::bounded(32 << 20)),
            Err(conflict())
        );
        assert_eq!(source.recipe().1, ["response"]);
    }
}

#[tokio::test]
async fn actual_registered_preparations_match_only_their_actual_full_recipe() {
    for keyed in [false, true] {
        let store = MemoryStore::new();
        let source = source(&["first", "second"]);
        let (_, original) = fixture::prepared(&store, keyed, &source).await;
        original.verify_original_intent().unwrap();
        verify(&source, &original, &mut Proof::bounded(32 << 20)).unwrap();
        let changed = LibraryScriptedSource::new(
            source.recipe().0.clone(),
            vec!["first".into(), "different tail".into()],
        )
        .unwrap();
        assert_eq!(
            verify(&changed, &original, &mut Proof::bounded(32 << 20)),
            Err(conflict())
        );
        // Refusal only borrows the genuine pristine carrier; it remains intact.
        assert_eq!(changed.recipe().1, ["first", "different tail"]);
        verify(&source, &original, &mut Proof::bounded(32 << 20)).unwrap();
    }
}

#[tokio::test]
async fn coherent_other_backend_at_the_same_route_is_refused_before_open() {
    for keyed in [false, true] {
        let store = MemoryStore::new();
        let source = source(&["response"]);
        let (_, original) = fixture::prepared_with(&store, keyed, &source, |bound| {
            let old = &bound.binding.provider;
            bound.binding.provider = ProviderBindingDescriptor::new(
                old.provider_id(),
                old.model_id(),
                "http.generic",
                old.behavior_version(),
                old.config_fingerprint().clone(),
                old.limits(),
            )
            .unwrap();
            bound.binding.runtime_semantics[12][0]["version"] =
                json!(serde_json::to_string(&bound.binding.provider).unwrap());
            plan(bound);
        })
        .await;
        original.verify_original_intent().unwrap();
        assert_eq!(
            verify(&source, &original, &mut Proof::bounded(32 << 20)),
            Err(conflict())
        );
        assert_eq!(source.recipe().1, ["response"]);
    }
}

#[tokio::test]
async fn coherent_unsupported_control_components_and_model_arguments_are_refused() {
    for case in 0..6 {
        let store = MemoryStore::new();
        let source = source(&["response"]);
        let (_, original) = fixture::prepared_with(&store, true, &source, |bound| match case {
            0 => {
                bound.binding.runtime_semantics[12]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"id":"custom-host-callback","version":"1"}));
                plan(bound);
            }
            1 => bound.binding.runtime_semantics[8] = json!(2),
            2 => {
                bound.binding.request.system_prompt = Some("unsupported system".into());
                bound.binding.runtime_semantics[3] = json!("unsupported system");
            }
            3 => {
                bound.binding.request.max_output_tokens = Some(10);
                bound.binding.runtime_semantics[4] = json!(10);
            }
            4 => {
                bound.binding.behavior_fingerprint = InputFingerprint::new("e".repeat(64)).unwrap();
                bound.binding.runtime_semantics[12][1]["version"] = json!("e".repeat(64));
                plan(bound);
            }
            5 => bound.binding.runtime_semantics[9] = json!([{"name":"unexecuted-tool"}]),
            _ => unreachable!(),
        })
        .await;
        original.verify_original_intent().unwrap();
        assert_eq!(
            verify(&source, &original, &mut Proof::bounded(32 << 20)),
            Err(conflict())
        );
    }
}

#[tokio::test]
async fn original_default_policy_is_preserved_and_cannot_be_refreshed_after_join() {
    let store = MemoryStore::new();
    let source = source(&["response"]);
    let (_, mut original) = fixture::prepared_with(&store, true, &source, |bound| {
        let policy = crate::AdmissionSelectionPolicy::session_defaults("p", "m");
        bound.binding.request.selection_policy = policy.clone();
        bound.binding.runtime_semantics[12]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id": SELECTION_POLICY_COMPONENT,
                "version": serde_json::to_string(&policy).unwrap(),
            }));
        plan(bound);
    })
    .await;
    verify(&source, &original, &mut Proof::bounded(32 << 20)).unwrap();
    let PreparedOperation::AdmitKeyed(bound) = &mut original.operation else {
        panic!("keyed original");
    };
    let changed = crate::AdmissionSelectionPolicy::session_defaults("changed-fallback", "m");
    bound.binding.request.selection_policy = changed.clone();
    bound.binding.runtime_semantics[12][3]["version"] =
        json!(serde_json::to_string(&changed).unwrap());
    plan(bound);
    fixture::synchronize(bound);
    assert!(original.verify_original_intent().is_err());
    assert!(verify(&source, &original, &mut Proof::bounded(32 << 20)).is_err());
    assert_eq!(source.recipe().1, ["response"]);
}
