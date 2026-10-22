use super::*;
fn caps() -> Limits {
    Limits {
        max_query_bytes: 16384,
        max_results: 12,
        max_title_bytes: 1024,
        max_url_bytes: 8192,
        max_snippet_bytes: 16384,
        max_in_flight: 256,
        max_wait: Duration::from_secs(600),
    }
}
fn set(l: &mut Limits, i: usize, n: usize) {
    match i {
        0 => l.max_query_bytes = n,
        1 => l.max_results = n,
        2 => l.max_title_bytes = n,
        3 => l.max_url_bytes = n,
        4 => l.max_snippet_bytes = n,
        5 => l.max_in_flight = n,
        _ => l.max_wait = Duration::from_secs(n as u64),
    }
}
fn options(limits: Limits, identity: &str) -> Options {
    Options {
        limits,
        searcher: searcher(vec![]),
        searcher_identity: identity.into(),
    }
}
#[test]
fn validates_every_limit_and_identity_and_hashes_every_behavior() {
    for (i, cap) in [16384, 12, 1024, 8192, 16384, 256, 600]
        .into_iter()
        .enumerate()
    {
        for bad in [if i == 3 { 7 } else { 0 }, cap + 1] {
            let mut l = limits();
            set(&mut l, i, bad);
            assert!(
                WebSearch::new(options(l, "host-v1"))
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("web-search-limits")
            );
        }
        let mut l = limits();
        set(&mut l, i, cap);
        assert!(WebSearch::new(options(l, "host-v1")).is_ok());
        let mut l = limits();
        set(
            &mut l,
            i,
            if i == 6 {
                6
            } else if i == 1 {
                10
            } else {
                20
            },
        );
        assert_ne!(
            extension(vec![]).config_hash(),
            configured(searcher(vec![]), l).config_hash()
        );
    }
    assert!(
        WebSearch::new(options(
            Limits {
                max_url_bytes: 8,
                ..limits()
            },
            "host-v1"
        ))
        .is_ok()
    );
    for identity in ["".into(), " ".into(), "a".repeat(257), "host\n".into()] {
        assert!(
            WebSearch::new(options(limits(), &identity))
                .err()
                .unwrap()
                .to_string()
                .contains("web-search-policy")
        );
    }
    assert_eq!(
        extension(vec![]).config_hash(),
        extension(vec![record()]).config_hash()
    );
    assert_ne!(
        extension(vec![]).config_hash(),
        WebSearch::new(options(limits(), "host-v2"))
            .unwrap()
            .config_hash()
    );
    assert_eq!(
        limits().worst_case_result_bytes(),
        14 + 2 * (34 + 6 * 176) + 1
    );
    assert_eq!(caps().worst_case_result_bytes(), 1_843_633);
}
#[tokio::test]
async fn registered_contract_is_exact() {
    let registry = Registry::new();
    let handle = registry
        .mount(Arc::new(extension(vec![])), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let info = &plan.tools[0].info;
    assert_eq!(info.name, TOOL_NAME);
    assert_eq!(
        info.description,
        "Search one query and return bounded title, URL, and snippet source records."
    );
    assert!(!info.retry_safe);
    assert_eq!(info.required_permissions, vec![PERMISSION_SEARCH]);
    assert_eq!(
        info.parameters,
        json!({"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string"}}})
    );
    drop(plan);
    handle.close().await.unwrap();
}
