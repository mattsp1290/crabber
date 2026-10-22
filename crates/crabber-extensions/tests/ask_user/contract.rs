use super::*;

#[tokio::test]
async fn durable_schema_compiles_under_store_validator_and_enforces_exactly_one_choice() {
    for n in [2, 5] {
        for described in [false, true] {
            let tool = definition(extension()).await;
            let options = (0..n)
                .map(|i| {
                    if described {
                        json!({"label":format!("option{i}"),"description":"details"})
                    } else {
                        json!({"label":format!("option{i}")})
                    }
                })
                .collect::<Vec<_>>();
            let ToolPhaseOutcome::Suspended(s) = tool
                .executor
                .resumable()
                .unwrap()
                .start(context(), json!({"question":"choose", "options":options}))
                .await
                .unwrap()
            else {
                panic!("suspended")
            };
            let schema = s.interactions()[0].response_schema();
            let validator = jsonschema::validator_for(schema).unwrap();
            for v in [
                json!({"selected":2}),
                json!({"selected":2.0}),
                json!({"custom":"x"}),
            ] {
                assert!(validator.is_valid(&v));
            }
            for v in [
                json!({"selected":9}),
                json!({"selected":1,"custom":"x"}),
                json!({}),
                json!("x"),
                json!({"custom":""}),
                json!({"selected":1,"extra":0}),
            ] {
                assert!(!validator.is_valid(&v), "{v}");
            }
            assert_eq!(s.checkpoint().schema_id(), "crabber-extensions/ask-user");
            assert!(!tool.info.retry_safe);
            assert_eq!(tool.info.required_permissions, ["interaction.ask"]);
        }
    }
}
#[tokio::test]
async fn maximal_escaped_schema_fits_and_fixed_scaffold_is_bounded() {
    let mut o = options();
    o.limits.max_option_label_bytes = 512;
    o.limits.max_option_description_bytes = 1024;
    let tool = definition(AskUser::new(o).unwrap()).await;
    let choices = (0..5).map(|i| json!({"label":format!("{}{}\"", "\"\\".repeat(255), i),"description":"\"\\".repeat(512)})).collect::<Vec<_>>();
    let ToolPhaseOutcome::Suspended(s) = tool
        .executor
        .resumable()
        .unwrap()
        .start(context(), json!({"question":"choose", "options":choices}))
        .await
        .unwrap()
    else {
        panic!("suspended")
    };
    let schema = s.interactions()[0].response_schema();
    let bytes = serde_json::to_vec(schema).unwrap().len();
    let dynamic = (512 * 2 - 1 + 1024 * 2) * 5;
    assert!(bytes <= 16384, "{bytes}");
    assert!(bytes - dynamic <= 1024, "scaffold {}", bytes - dynamic);
    assert!(
        jsonschema::validator_for(schema)
            .unwrap()
            .is_valid(&json!({"selected":5}))
    );
}
#[test]
fn finite_limits_and_authority_change_fingerprint() {
    let a = extension();
    assert_eq!(a.config_hash(), extension().config_hash());
    let mut o = options();
    o.authority = AuthorityRequirement::new("different", "1").unwrap();
    assert_ne!(a.config_hash(), AskUser::new(o).unwrap().config_hash());
    let mut o = options();
    o.limits.max_custom_answer_bytes -= 1;
    assert_ne!(a.config_hash(), AskUser::new(o).unwrap().config_hash());
    for field in 0..10 {
        let mut o = options();
        match field {
            0 => o.limits.max_question_bytes = 8193,
            1 => o.limits.max_option_label_bytes = 513,
            2 => o.limits.max_option_description_bytes = 1025,
            3 => o.limits.max_custom_answer_bytes = 10241,
            4 => o.limits.max_wait = Duration::from_millis(999),
            5 => o.limits.max_wait = Duration::from_secs(86401),
            6 => o.limits.max_question_bytes = 0,
            7 => o.limits.max_option_label_bytes = 0,
            8 => o.limits.max_option_description_bytes = 0,
            _ => o.limits.max_custom_answer_bytes = 0,
        }
        assert!(AskUser::new(o).is_err());
    }
}
