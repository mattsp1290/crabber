//! Actual protocol decoders classify before Runtime ever accumulates text.
use super::{event, fixture, message, messages, request, responses};
use crate::{ProviderErrorKind, StreamDelta};
use crabber_core::{ContentBlock, PartKind, ReasoningProvenance, Role};
use serde_json::json;

#[test]
fn responses_private_summary_and_encrypted_items_never_share_a_category() {
    let mut codec = responses::Codec::new(true);
    let mut identities = Vec::new();
    for (kind, index, expected) in [
        (
            "response.reasoning_text.delta",
            "content_index",
            ReasoningProvenance::Private,
        ),
        (
            "response.reasoning_summary_text.delta",
            "summary_index",
            ReasoningProvenance::PublicSummary,
        ),
    ] {
        let mut value = json!({"item_id":"rs_1","delta":"distinct"});
        value[index] = json!(0);
        let delta = codec.event(&event(kind, &value)).unwrap();
        if let StreamDelta::Reasoning { item_id, .. } = &delta[0] {
            identities.push(item_id.clone());
        }
        assert!(
            matches!(&delta[0], StreamDelta::Reasoning { item_id, provenance, text }
            if *provenance == expected && text == "distinct" && item_id.contains(if index=="summary_index" {":summary:"} else {":text:"}))
        );
    }
    assert_ne!(
        identities[0], identities[1],
        "same provider item has distinct channel identities"
    );
    let opaque =
        json!({"type":"reasoning","id":"rs_1","encrypted_content":"PRIVATE_CIPHER","summary":[]});
    let decoded = codec
        .event(&event("response.output_item.done", &json!({"item":opaque})))
        .unwrap();
    assert!(
        matches!(&decoded[0], StreamDelta::ProviderState { codec_id, payload }
        if codec_id == responses::CODEX_STATE && payload == &opaque)
    );
    let error = codec
        .event(&event(
            "response.failed",
            &json!({
                "response":{"error":{"code":"PRIVATE_ERROR_BODY"}}
            }),
        ))
        .unwrap();
    assert!(matches!(&error[0], StreamDelta::Error(error)
        if error.kind == ProviderErrorKind::Server && !error.message.contains("PRIVATE")));
}

#[test]
fn messages_signatures_are_scoped_to_the_actual_thinking_index() {
    let mut codec = messages::Codec::default();
    for index in [0, 2] {
        let start = codec
            .event(&event(
                "content_block_start",
                &json!({
                    "index":index,"content_block":{"type":"thinking","thinking":""}
                }),
            ))
            .unwrap();
        let StreamDelta::Reasoning {
            item_id,
            provenance,
            ..
        } = &start[0]
        else {
            panic!("thinking start")
        };
        assert_eq!(*provenance, ReasoningProvenance::Private);
        for (kind, key, value) in [
            ("thinking_delta", "thinking", "PRIVATE_THOUGHT"),
            ("signature_delta", "signature", "PRIVATE_SIGNATURE"),
        ] {
            let mut delta = json!({"type":kind});
            delta[key] = json!(value);
            let decoded = codec
                .event(&event(
                    "content_block_delta",
                    &json!({"index":index,"delta":delta}),
                ))
                .unwrap();
            match &decoded[0] {
                StreamDelta::Reasoning {
                    item_id: actual, ..
                }
                | StreamDelta::ReasoningSignature {
                    item_id: actual, ..
                } => assert_eq!(actual, item_id),
                _ => panic!("associated private item"),
            }
        }
        codec
            .event(&event("content_block_stop", &json!({"index":index})))
            .unwrap();
    }
    assert!(
        codec
            .event(&event(
                "content_block_delta",
                &json!({
                    "index":7,"delta":{"type":"signature_delta","signature":"PRIVATE"}
                })
            ))
            .is_err()
    );
    assert!(
        codec
            .event(&event(
                "content_block_start",
                &json!({
                    "index":0,"content_block":{"type":"thinking"}
                })
            ))
            .is_err()
    );
}

#[test]
fn messages_replays_each_thought_with_only_its_own_signature_and_redacted_item() {
    let mut blocks = Vec::new();
    for index in [0, 2] {
        blocks.push(ContentBlock::Reasoning {
            text:format!("thought{index}"),provenance:ReasoningProvenance::Private,item_id:Some(format!("anthropic:{index}")),
            provider_state:Some(json!({"codec_id":"crabber.anthropic.thinking/v1","signature":format!("sig{index}")})),
        });
    }
    blocks.push(ContentBlock::ProviderState {
        codec_id: "crabber.anthropic.thinking/v1".into(),
        payload: json!({"signature":"FOREIGN_SIGNATURE"}),
    });
    blocks.insert(
        1,
        ContentBlock::ProviderState {
            codec_id: "crabber.anthropic.redacted-thinking/v1".into(),
            payload: json!({"type":"redacted_thinking","data":"OPAQUE"}),
        },
    );
    let body = messages::body(&request(
        vec![message(Role::Assistant, &PartKind::Reasoning, blocks)],
        vec![],
        None,
    ));
    assert_eq!(
        body["messages"][0]["content"],
        json!([
            {"type":"thinking","thinking":"thought0","signature":"sig0"},
            {"type":"redacted_thinking","data":"OPAQUE"},
            {"type":"thinking","thinking":"thought2","signature":"sig2"}
        ])
    );
    assert!(!body.to_string().contains("FOREIGN"));
}

#[test]
fn chat_vendor_reasoning_remains_unspecified_private() {
    let mut codec = super::super::chat::Codec::default();
    let deltas = fixture(
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"PRIVATE_VENDOR\"}}]}\n\n",
        |event| codec.event(event).unwrap(),
    );
    assert_eq!(
        deltas,
        vec![StreamDelta::ReasoningDelta("PRIVATE_VENDOR".into())]
    );
}
