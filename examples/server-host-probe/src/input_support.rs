//! Pure actual HTTP codec declarations: no credentials or network request.
use crabber::{
    core::{MediaKind, MediaSource, MessageContent, Role, ToolOutput},
    providers::{HttpAdapter, MediaInputSource},
};
use serde_json::json;
pub(crate) fn run() -> Result<usize, super::Error> {
    let responses = HttpAdapter::openai().input_support();
    let messages = HttpAdapter::anthropic().input_support();
    assert!(responses.context_data && messages.context_data);
    assert_eq!(responses.roles, &[Role::User, Role::Assistant, Role::Tool]);
    assert_eq!(messages.roles, &[Role::User, Role::Assistant, Role::Tool]);
    assert_eq!(responses.media.len(), 3);
    for source in [
        MediaInputSource::Data,
        MediaInputSource::Url,
        MediaInputSource::OpaqueFile,
    ] {
        let media = responses
            .media
            .iter()
            .find(|media| media.source == source)
            .unwrap();
        assert_eq!(media.kind, MediaKind::Image);
        assert!(media.tool_results);
        assert_eq!(media.message_roles, &[Role::User]);
        assert!(media.mime_types.contains(&"image/png"));
    }
    assert_eq!(
        messages.media,
        Vec::<crabber::providers::HttpMediaSupport>::new().as_slice()
    );
    let value = json!({"literal":"JSON result","nested":[null,true,7]});
    assert_eq!(ToolOutput::from_json(&value)?.json_value()?, value);
    let mixed = ToolOutput::new(
        vec![
            MessageContent::Text { text: "{}".into() },
            MessageContent::Media {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::File {
                    file_id: "public-fixture".into(),
                },
            },
        ],
        false,
    )?;
    assert!(mixed.json_value().is_err());
    Ok(2)
}
