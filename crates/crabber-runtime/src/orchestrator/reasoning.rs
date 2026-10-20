//! Separate item/category buffers; signatures and opaque state never become deltas.
mod budget;
use super::{RuntimeError, invalid_provider};
use crabber_core::{ContentBlock, ReasoningProvenance};

const MAX_ITEMS: usize = 128;
const MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
pub(super) struct ReasoningBuffer {
    blocks: Vec<ContentBlock>,
    bytes: usize,
}

impl ReasoningBuffer {
    pub(super) fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
    pub(super) fn opaque(
        &mut self,
        codec_id: String,
        payload: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        if self.blocks.len() >= MAX_ITEMS {
            return Err(invalid_provider("reasoning item limit").into());
        }
        let bytes = budget::opaque_bytes(&codec_id, &payload, MAX_BYTES - self.bytes)?;
        self.charge(bytes)?;
        self.blocks
            .push(ContentBlock::ProviderState { codec_id, payload });
        Ok(())
    }
    pub(super) fn part_kind(block: &ContentBlock) -> crabber_core::PartKind {
        match block {
            ContentBlock::ProviderState { .. } => crabber_core::PartKind::ProviderState,
            _ => crabber_core::PartKind::Reasoning,
        }
    }
    pub(super) fn push(
        &mut self,
        id: Option<String>,
        provenance: ReasoningProvenance,
        fragment: String,
    ) -> Result<(), RuntimeError> {
        if id
            .as_ref()
            .is_some_and(|id| id.is_empty() || id.len() > 1024 || id.chars().any(char::is_control))
        {
            return Err(invalid_provider("invalid reasoning identity").into());
        }
        let existing = self.blocks.iter().position(
            |block| matches!(block, ContentBlock::Reasoning { item_id, .. } if *item_id == id),
        );
        if let Some(index) = existing {
            let ContentBlock::Reasoning {
                provenance: original,
                ..
            } = &self.blocks[index]
            else {
                unreachable!();
            };
            if *original != provenance {
                return Err(invalid_provider("reasoning provenance changed").into());
            }
            self.charge(fragment.len())?;
            if let ContentBlock::Reasoning { text, .. } = &mut self.blocks[index] {
                text.push_str(&fragment);
            }
            return Ok(());
        }
        if self.blocks.len() >= MAX_ITEMS {
            return Err(invalid_provider("reasoning limit").into());
        }
        // Retained identities count once; repeated fragments borrow that same
        // item. Opaque entries count their complete escaped codec/payload tuple.
        self.charge(fragment.len() + id.as_ref().map_or(0, String::len))?;
        self.blocks.push(ContentBlock::Reasoning {
            text: fragment,
            provider_state: None,
            provenance,
            item_id: id,
        });
        Ok(())
    }
    pub(super) fn signature(&mut self, id: &str, signature: &str) -> Result<(), RuntimeError> {
        self.charge(signature.len())?;
        for block in &mut self.blocks {
            if let ContentBlock::Reasoning {
                item_id: Some(item),
                provenance: ReasoningProvenance::Private,
                provider_state,
                ..
            } = block
                && item == id
            {
                let state = provider_state.get_or_insert_with(|| {
                    serde_json::json!({
                        "codec_id":"crabber.anthropic.thinking/v1", "signature":""
                    })
                });
                let Some(serde_json::Value::String(original)) = state.get_mut("signature") else {
                    return Err(invalid_provider("invalid private reasoning state").into());
                };
                original.push_str(signature);
                return Ok(());
            }
        }
        Err(invalid_provider("signature without private reasoning item").into())
    }
    fn charge(&mut self, bytes: usize) -> Result<(), RuntimeError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= MAX_BYTES)
            .ok_or_else(|| invalid_provider("reasoning limit"))?;
        Ok(())
    }
    pub(super) fn interrupted_blocks(&self) -> impl Iterator<Item = ContentBlock> + '_ {
        self.blocks.iter().cloned()
    }
    pub(super) fn into_blocks(self) -> impl Iterator<Item = ContentBlock> {
        self.blocks.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_BYTES, ReasoningBuffer, ReasoningProvenance};
    #[test]
    fn exact_category_identity_and_signature_association_are_enforced_before_publication() {
        let mut buffer = ReasoningBuffer::default();
        buffer
            .push(
                Some("first".into()),
                ReasoningProvenance::Private,
                "PRIVATE".into(),
            )
            .unwrap();
        buffer.signature("first", "a").unwrap();
        buffer.signature("first", "b").unwrap();
        buffer
            .push(
                Some("second".into()),
                ReasoningProvenance::Private,
                "OTHER".into(),
            )
            .unwrap();
        buffer.signature("second", "c").unwrap();
        assert!(
            buffer
                .push(
                    Some("first".into()),
                    ReasoningProvenance::PublicSummary,
                    "PRIVATE".into()
                )
                .is_err()
        );
        assert!(buffer.signature("foreign", "PRIVATE").is_err());
        let blocks: Vec<_> = buffer.into_blocks().collect();
        let json = serde_json::to_value(blocks).unwrap();
        assert_eq!(json[0]["provider_state"]["signature"], "ab");
        assert_eq!(json[1]["provider_state"]["signature"], "c");
    }
    #[test]
    fn reasoning_limit_is_shared_across_items_and_signatures() {
        let mut buffer = ReasoningBuffer::default();
        buffer
            .push(
                Some("first".into()),
                ReasoningProvenance::Private,
                "x".repeat(MAX_BYTES - "first".len() - 1),
            )
            .unwrap();
        buffer.signature("first", "a").unwrap();
        assert!(buffer.signature("first", "b").is_err());
        assert!(
            buffer
                .push(
                    Some("second".into()),
                    ReasoningProvenance::PublicSummary,
                    "x".into()
                )
                .is_err()
        );
    }

    #[test]
    fn opaque_identity_text_and_signature_share_one_exact_byte_edge() {
        let mut buffer = ReasoningBuffer::default();
        buffer
            .push(
                Some("first".into()),
                ReasoningProvenance::Private,
                "p".into(),
            )
            .unwrap();
        buffer.signature("first", "s").unwrap();
        let codec = "codec\"";
        let payload = serde_json::json!({"cipher":"\0\"\\🙂"});
        let opaque_bytes = serde_json::to_vec(&(codec, &payload)).unwrap().len();
        buffer.opaque(codec.into(), payload.clone()).unwrap();
        assert_eq!(buffer.bytes, "first".len() + 2 + opaque_bytes);
        buffer
            .push(
                Some("second".into()),
                ReasoningProvenance::Private,
                String::new(),
            )
            .unwrap();
        let remaining = MAX_BYTES - buffer.bytes;
        buffer.signature("second", &"x".repeat(remaining)).unwrap();
        assert_eq!(buffer.bytes, MAX_BYTES);
        let before = buffer.interrupted_blocks().collect::<Vec<_>>();
        assert!(
            buffer
                .opaque("foreign".into(), serde_json::Value::Null)
                .is_err()
        );
        assert!(buffer.signature("second", "x").is_err());
        assert!(
            buffer
                .push(
                    Some("third".into()),
                    ReasoningProvenance::Private,
                    String::new()
                )
                .is_err()
        );
        assert_eq!(buffer.bytes, MAX_BYTES);
        assert_eq!(buffer.blocks, before);
        assert!(
            matches!(&buffer.blocks[1], crabber_core::ContentBlock::ProviderState { codec_id, payload: actual }
            if codec_id == codec && actual == &payload)
        );
        assert!(
            matches!(&buffer.blocks[2], crabber_core::ContentBlock::Reasoning { item_id: Some(id), .. }
            if id == "second")
        );
    }

    #[test]
    fn oversized_or_deep_opaque_is_refused_before_retention() {
        let mut buffer = ReasoningBuffer::default();
        buffer
            .opaque("codec".into(), serde_json::json!({"cipher":"kept"}))
            .unwrap();
        let before = buffer.interrupted_blocks().collect::<Vec<_>>();
        let bytes = buffer.bytes;
        assert!(
            buffer
                .opaque(
                    "codec".into(),
                    serde_json::Value::String("x".repeat(MAX_BYTES))
                )
                .is_err()
        );
        let mut deep = serde_json::Value::Null;
        for _ in 0..65 {
            deep = serde_json::json!([deep]);
        }
        assert!(buffer.opaque("codec".into(), deep).is_err());
        assert_eq!(buffer.bytes, bytes);
        assert_eq!(buffer.blocks, before);
    }
}
