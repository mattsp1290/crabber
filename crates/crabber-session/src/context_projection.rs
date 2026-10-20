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
        let summary = all
            .iter()
            .find(|message| &message.id == summary_id)
            .ok_or(StoreError::NotFound)?
            .clone();
        let mut selected = vec![summary];
        if let Some(tail) = epoch.tail_start_message_id {
            let index = all
                .iter()
                .position(|message| message.id == tail)
                .ok_or(StoreError::NotFound)?;
            selected.extend(
                all[index..]
                    .iter()
                    .filter(|message| &message.id != summary_id)
                    .cloned(),
            );
        }
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
