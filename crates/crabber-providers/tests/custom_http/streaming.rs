use super::*;

#[tokio::test]
async fn w3_review_completed_is_emitted_once_only_after_clean_finalization() {
    let completed = b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
    let mut malformed = [completed.as_slice(), completed.as_slice()].concat();
    malformed.push(0xff);
    let server = RawServer::start(raw_response("200 OK", malformed.len(), &malformed)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, StreamDelta::Completed))
    );
    assert_eq!(stream_errors(&items)[0].message, "provider transport body");
    let _ = server.body().await;

    let server = RawServer::start(raw_response("200 OK", completed.len() + 10, completed)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, StreamDelta::Completed))
    );
    assert_eq!(stream_errors(&items)[0].message, "provider transport body");
    let _ = server.body().await;

    let text = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n";
    let duplicated = [text.as_slice(), completed.as_slice(), completed.as_slice()].concat();
    let server = RawServer::start(raw_response("200 OK", duplicated.len(), &duplicated)).await;
    let mut stream = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap();
    let mut consumer_items = Vec::new();
    while let Some(item) = stream.next().await {
        let terminal = matches!(item, StreamDelta::Completed);
        consumer_items.push(item);
        if terminal {
            break;
        }
    }
    assert!(matches!(
        consumer_items.first(),
        Some(StreamDelta::TextDelta(text)) if text == "before"
    ));
    assert!(matches!(
        consumer_items.last(),
        Some(StreamDelta::Completed)
    ));
    assert_eq!(
        consumer_items
            .iter()
            .filter(|item| matches!(item, StreamDelta::Completed))
            .count(),
        1
    );
    assert!(stream.next().await.is_none());
    assert_eq!(stream_errors(&consumer_items), Vec::<&ProviderError>::new());
    let _ = server.body().await;

    let incomplete = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n";
    let server = RawServer::start(raw_response("200 OK", incomplete.len(), incomplete)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(stream_errors(&items)[0].message, "provider transport body");
    let _ = server.body().await;

    let invalid_json = b"data: {\n";
    let server = RawServer::start(raw_response("200 OK", invalid_json.len(), invalid_json)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(stream_errors(&items)[0].kind, ProviderErrorKind::Invalid);
    let _ = server.body().await;
}

#[tokio::test]
async fn ordinary_deltas_after_protocol_completion_are_suppressed() {
    let responses = concat!(
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"after\"}\n\n"
    );
    let chat = concat!(
        "data: [DONE]\n\n",
        "data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":11},\"choices\":[{\"delta\":{\"content\":\"after\"}}]}\n\n"
    );

    for (protocol, body) in [
        (Protocol::Responses, responses),
        (Protocol::ChatCompletions, chat),
    ] {
        let server = RawServer::start(raw_response("200 OK", body.len(), body.as_bytes())).await;
        let items: Vec<_> = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, StreamDelta::Completed))
                .count(),
            1,
            "unexpected terminal output: {items:?}"
        );
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, StreamDelta::TextDelta(text) if text == "after")),
            "post-completion text leaked: {items:?}"
        );
        assert_eq!(stream_errors(&items), Vec::<&ProviderError>::new());
        match protocol {
            Protocol::Responses => assert!(matches!(
                items.as_slice(),
                [StreamDelta::Usage(_), StreamDelta::Completed]
            )),
            Protocol::ChatCompletions => {
                assert!(matches!(items.as_slice(), [StreamDelta::Completed]));
            }
            Protocol::Messages => unreachable!(),
        }
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn valid_protocol_errors_are_the_only_terminal_delta() {
    let cases = [
        (
            Protocol::Responses,
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
                "event: response.failed\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n"
            ),
            "Responses response.failed: server_error",
        ),
        (
            Protocol::Responses,
            concat!(
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
                "event: response.incomplete\n",
                "data: {\"type\":\"response.incomplete\",\"response\":{\"error\":{\"code\":\"context_limit\"}}}\n\n"
            ),
            "Responses response.incomplete: context_limit",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n"
            ),
            "Messages stream error",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n"
            ),
            "Messages stream error",
        ),
    ];

    for (protocol, body, expected_message) in cases {
        let server = RawServer::start(raw_response("200 OK", body.len(), body.as_bytes())).await;
        let mut stream = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap();
        let mut consumer_items = Vec::new();
        while let Some(item) = stream.next().await {
            let terminal = matches!(item, StreamDelta::Completed | StreamDelta::Error(_));
            consumer_items.push(item);
            if terminal {
                break;
            }
        }

        assert_single_protocol_error(&consumer_items, expected_message);
        assert!(stream.next().await.is_none());
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn decode_stops_at_first_protocol_error_in_same_chunk() {
    let cases = [
        (
            Protocol::Responses,
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
                "event: response.failed\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n",
                "data: {\n\n"
            ),
            "Responses response.failed: server_error",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n",
                "data: {\n\n"
            ),
            "Messages stream error",
        ),
    ];

    for (protocol, body, expected_message) in cases {
        let server = RawServer::start(raw_response("200 OK", body.len(), body.as_bytes())).await;
        let items: Vec<_> = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap()
            .collect()
            .await;

        assert!(matches!(
            items.first(),
            Some(StreamDelta::TextDelta(text)) if text == "before"
        ));
        assert_single_protocol_error(&items, expected_message);
        assert_eq!(items.len(), 2, "unexpected decoded items: {items:?}");
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn parser_batches_preserve_order_before_terminal_or_parser_error() {
    let protocol_cases = [
        (
            Protocol::Responses,
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
                "event: response.failed\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n"
            ),
            "Responses response.failed: server_error",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n"
            ),
            "Messages stream error",
        ),
    ];
    for (protocol, prefix, expected_message) in protocol_cases {
        let mut body = prefix.as_bytes().to_vec();
        body.extend_from_slice(b"data: \xff\n\n");
        let server = RawServer::start(raw_response("200 OK", body.len(), &body)).await;
        let items: Vec<_> = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap()
            .collect()
            .await;

        assert!(matches!(items.first(), Some(StreamDelta::TextDelta(text)) if text == "before"));
        assert_single_protocol_error(&items, expected_message);
        assert_eq!(
            items.len(),
            2,
            "later parser failure replaced terminal: {items:?}"
        );
        let _ = server.body().await;
    }

    let prefix = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n"
    );
    let mut oversized = prefix.as_bytes().to_vec();
    oversized.extend_from_slice(b"data: ");
    oversized.resize(
        oversized.len() + crabber_providers::sse::MAX_LINE_BYTES + 1,
        b'x',
    );
    oversized.push(b'\n');
    let server = RawServer::start(raw_response("200 OK", oversized.len(), &oversized)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(matches!(items.first(), Some(StreamDelta::TextDelta(text)) if text == "before"));
    assert_single_protocol_error(&items, "Responses response.failed: server_error");
    assert_eq!(
        items.len(),
        2,
        "oversized tail replaced terminal: {items:?}"
    );
    let _ = server.body().await;

    let mut malformed =
        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n".to_vec();
    malformed.extend_from_slice(b"data: \xff\n\n");
    let server = RawServer::start(raw_response("200 OK", malformed.len(), &malformed)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(matches!(items.first(), Some(StreamDelta::TextDelta(text)) if text == "before"));
    assert!(matches!(
        items.get(1),
        Some(StreamDelta::Error(error))
            if error.kind == ProviderErrorKind::Invalid && error.message == "invalid SSE UTF-8"
    ));
    assert_eq!(items.len(), 2, "unexpected parser-error output: {items:?}");
    let _ = server.body().await;
}
