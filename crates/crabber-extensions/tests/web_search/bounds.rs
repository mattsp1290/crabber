use super::*;
// Reference verdicts executed with Go net/url at Eino commit 5389549.
#[tokio::test]
async fn url_corpus_agrees_with_reference_or_is_stricter_and_preserves_verbatim_urls() {
    let cases = [
        ("", false),                                       // Go: reject; agrees
        ("/relative", false),                              // Go: reject; agrees
        ("//example.test/path", false),                    // Go: reject; agrees
        ("ftp://example.test/file", false),                // Go: reject; agrees
        ("http:///missing-host", false),                   // Go: reject; agrees
        ("http://:80/path", false),                        // Go: reject; agrees
        ("https://:443/", false),                          // Go: reject; agrees
        ("HTTP://example.test/path", false),               // Go: reject; agrees
        ("Http://example.test/path", false),               // Go: reject; agrees
        ("https://user:pass@example.test/private", false), // Go: reject; agrees
        ("https://example.test/\ncontrol", false),         // Go: reject; agrees
        ("https://a.b%41/", false),                        // Go: reject; agrees
        ("https://ab%2e/", false),                         // Go: reject; agrees
        ("https://exa mple.test/", false),                 // Go: reject; agrees
        ("http://example.test:80x/", false),               // Go: reject; agrees
        ("https://example.test\\path", false),             // Go: reject; agrees
        ("https://@example.test/", false),                 // Go: reject; agrees
        ("https://example.test/%zz", false),               // Go: reject; agrees
        ("https://example.test/", false),                 // Go: reject; agrees
        ("http://example.test:65536/", false),             // Go: accept; stricter
        ("http://1.2.3.4.5/", false),                      // Go: accept; stricter
        ("https://[fe80::1%25eth0]/", false),              // Go: accept; stricter
        ("https://a.b%25/", false),                        // Go: accept; stricter
        ("https://exa<mple.test/", false),                 // Go: accept; stricter
        ("https://exa>mple.test/", false),                 // Go: accept; stricter
        ("https://example.test/?q=%zz", false),            // Go: accept; stricter
        ("http://a", true),                                // Go: accept; agrees
        ("https://[::1]:8080/x", true),                    // Go: accept; agrees
        ("https://example.test:/", true),                  // Go: accept; agrees
        ("https://example.test/?x=<&y=aaa", true),         // Go: accept; agrees
        ("https://bücher.example/x", true),                // Go: accept; agrees
        ("https://example.test/a%20b", true),              // Go: accept; agrees
        ("https://example.test/a b", true),                // Go: accept; agrees
        ("https://example.test/a?x=1#fragment", true),     // Go: accept; agrees
        ("https:/", false),                                // Go: reject; agrees
        ("https:example.test", false),                     // Go: reject; agrees
        ("HTTPS://example.test/", false),                  // Go: reject; agrees
        ("http://\\example.test", false),                  // Go: reject; agrees
        ("http://?example.test", false),                   // Go: reject; agrees
        ("http://#example.test", false),                   // Go: reject; agrees
        ("https://exa-mple.test/", true),                  // Go: accept; agrees
        ("https://exa.mple.test/", true),                  // Go: accept; agrees
        ("https://exa_mple.test/", true),                  // Go: accept; agrees
        ("https://exa~mple.test/", true),                  // Go: accept; agrees
        ("https://exa!mple.test/", true),                  // Go: accept; agrees
        ("https://exa$mple.test/", true),                  // Go: accept; agrees
        ("https://exa&mple.test/", true),                  // Go: accept; agrees
        ("https://exa'mple.test/", true),                  // Go: accept; agrees
        ("https://exa(mple.test/", true),                  // Go: accept; agrees
        ("https://exa)mple.test/", true),                  // Go: accept; agrees
        ("https://exa*mple.test/", true),                  // Go: accept; agrees
        ("https://exa+mple.test/", true),                  // Go: accept; agrees
        ("https://exa,mple.test/", true),                  // Go: accept; agrees
        ("https://exa;mple.test/", true),                  // Go: accept; agrees
        ("https://exa=mple.test/", true),                  // Go: accept; agrees
        ("https://exa:mple.test/", false),                 // Go: reject; agrees
        ("https://exa[mple.test/", false),                 // Go: reject; agrees
        ("https://exa]mple.test/", false),                 // Go: accept; stricter
        ("https://exa<mple.test/", false),                 // Go: accept; stricter
        ("https://exa>mple.test/", false),                 // Go: accept; stricter
        ("https://exa\"mple.test/", true),                 // Go: accept; agrees
        ("https://exa^mple.test/", false),                 // Go: reject; agrees
        ("https://exa`mple.test/", false),                 // Go: reject; agrees
        ("https://exa{mple.test/", false),                 // Go: reject; agrees
        ("https://exa|mple.test/", false),                 // Go: reject; agrees
        ("https://exa}mple.test/", false),                 // Go: reject; agrees
        ("https://exa mple.test/", false),                 // Go: accept; stricter
        ("https://example.test:/", true),                  // Go: accept; agrees
        ("https://example.test:0/", true),                 // Go: accept; agrees
        ("https://example.test:65535/", true),             // Go: accept; agrees
        ("https://example.test:65536/", false),            // Go: accept; stricter
        ("https://example.test:08/", true),                // Go: accept; agrees
        ("https://example.test/a%20b", true),              // Go: accept; agrees
        ("https://example.test/a%2", false),               // Go: reject; agrees
        ("https://example.test/?q=%G0", false),            // Go: accept; stricter
        ("https://example.test/#%25", true),               // Go: accept; agrees
        (
            "https://example.test/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            true,
        ), // Go: accept; agrees
        (
            "https://example.test/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            false,
        ), // Go: reject; agrees
    ];
    for (url, accepted) in cases {
        let source = Source {
            url: url.into(),
            ..record()
        };
        let tool = executor(extension(vec![source.clone()])).await;
        let result = tool
            .execute_json_with_context(context(CancellationToken::new()), arguments())
            .await
            .unwrap();
        assert_eq!(
            result,
            if accepted {
                json!({"results":[source]})
            } else {
                json!({"results":[]})
            },
            "{url:?}"
        );
    }
}
#[tokio::test]
async fn bounded_records_preserve_order_and_duplicates_without_refill() {
    let mut bad = record();
    bad.url = "relative".into();
    let mut second = record();
    second.title = "second".into();
    let mut huge = record();
    huge.title = "x".repeat(1 << 20);
    huge.snippet = "x".repeat(1 << 20);
    let mut huge_url = record();
    huge_url.url.push_str(&"a".repeat(1 << 20));
    let mut nul_title = record();
    nul_title.title = "nul\0".into();
    let mut nul_snippet = record();
    nul_snippet.snippet = "nul\0".into();
    for (records, l, expected) in [
        (
            vec![bad, second.clone(), record()],
            limits(),
            json!({"results":[second]}),
        ),
        (
            vec![record(), record()],
            limits(),
            json!({"results":[record(),record()]}),
        ),
        (vec![], limits(), json!({"results":[]})),
        (
            vec![huge, huge_url],
            limits(),
            json!({"results":[Source {title:"x".repeat(16),snippet:"x".repeat(32),..record()}]}),
        ),
        (
            vec![Source {
                title: "aéétail".into(),
                snippet: "aérest".into(),
                ..record()
            }],
            Limits {
                max_title_bytes: 4,
                max_snippet_bytes: 2,
                ..limits()
            },
            json!({"results":[Source {title:"aé".into(),snippet:"a".into(),..record()}]}),
        ),
        (
            vec![nul_title, nul_snippet, record()],
            Limits {
                max_results: 3,
                ..limits()
            },
            json!({"results":[record()]}),
        ),
    ] {
        let tool = executor(configured(searcher(records), l)).await;
        assert_eq!(
            tool.execute_json_with_context(context(CancellationToken::new()), arguments())
                .await
                .unwrap(),
            expected
        );
    }
}
#[tokio::test]
async fn escaping_fits_worst_case_bound() {
    let l = Limits {
        max_title_bytes: 4,
        max_url_bytes: 64,
        max_snippet_bytes: 5,
        ..limits()
    };
    let records = vec![
        Source {
            title: "\u{1}".repeat(4),
            snippet: "\u{2}".repeat(5),
            url: format!("https://example.test/?x=<&y={}", "a".repeat(24)),
        },
        Source {
            title: "\u{3}".repeat(4),
            snippet: "\u{4}".repeat(5),
            url: format!("https://example.test/?x=<&y={}", "a".repeat(24)),
        },
    ];
    let tool = executor(configured(searcher(records.clone()), l.clone())).await;
    let result = tool
        .execute_json_with_context(context(CancellationToken::new()), arguments())
        .await
        .unwrap();
    assert_eq!(result, json!({"results":records}));
    assert!(result.to_string().len() <= l.worst_case_result_bytes());
}

#[tokio::test]
async fn hard_cap_result_survives_typed_output() {
    let l = Limits {
        max_query_bytes: 16384,
        max_results: 12,
        max_title_bytes: 1024,
        max_url_bytes: 8192,
        max_snippet_bytes: 16384,
        max_in_flight: 1,
        max_wait: Duration::from_secs(5),
    };
    let record = Source {
        title: "\u{1}".repeat(l.max_title_bytes),
        url: format!("https://example.test/{}", "a".repeat(l.max_url_bytes - 21)),
        snippet: "\u{2}".repeat(l.max_snippet_bytes),
    };
    let records = vec![record; l.max_results];
    let tool = executor(configured(searcher(records.clone()), l)).await;
    let result = tool
        .execute_json_with_context(context(CancellationToken::new()), arguments())
        .await
        .unwrap();
    assert_eq!(result, json!({"results":records}));
}
