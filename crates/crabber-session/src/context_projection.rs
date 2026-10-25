//! Pure epoch selection over independently loaded, validated context records.
use crate::StoreError;
use crabber_core::{ContextEpoch, Message, PartKind};

pub(crate) fn project(
    all: Vec<Message>,
    selected: Option<ContextEpoch>,
) -> Result<Vec<Message>, StoreError> {
    let selected = if let Some(epoch) = selected.filter(|epoch| epoch.summary_message_id.is_some())
    {
        let summary_id = epoch.summary_message_id.as_ref().expect("checked");
        let summary_index = all
            .iter()
            .position(|message| &message.id == summary_id)
            .ok_or(StoreError::NotFound)?;
        let tail_index = if let Some(tail) = epoch.tail_start_message_id {
            all.iter()
                .position(|message| message.id == tail)
                .ok_or(StoreError::NotFound)?
        } else {
            summary_index + 1
        };
        let mut selected = vec![all[summary_index].clone()];
        selected.extend(
            all[tail_index..]
                .iter()
                .filter(|message| &message.id != summary_id)
                .cloned(),
        );
        selected
    } else {
        all
    };
    Ok(selected
        .into_iter()
        .filter_map(|mut message| {
            message
                .parts
                .retain(|part| !matches!(part.kind, PartKind::Custom { .. }));
            (!message.parts.is_empty()).then_some(message)
        })
        .collect())
}
