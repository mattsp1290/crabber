use super::errors::{TransportCause, transport, transport_from_reqwest};
use super::{HttpAdapter, Protocol};
use crate::{DeltaStream, ProviderError, StreamDelta, sse};
use futures::{StreamExt, stream};
use reqwest::Response;
use std::collections::VecDeque;

impl HttpAdapter {
    pub(super) fn response_stream(&self, response: Response) -> DeltaStream {
        let codec = match self.protocol {
            Protocol::Responses => Codec::Responses(crate::responses::Codec::new(
                self.uses_codex_responses_mode(),
            )),
            Protocol::Messages => Codec::Messages(crate::messages::Codec::default()),
            Protocol::ChatCompletions => Codec::Chat(crate::chat::Codec::default()),
        };
        struct State {
            bytes: futures::stream::BoxStream<'static, Result<bytes::Bytes, reqwest::Error>>,
            parser: sse::Parser,
            codec: Codec,
            queue: VecDeque<StreamDelta>,
            protocol_complete: bool,
            /// Observers have seen output, so any retry would repeat it.
            observed: bool,
            ended: bool,
        }
        let state = State {
            bytes: Box::pin(response.bytes_stream()),
            parser: sse::Parser::default(),
            codec,
            queue: VecDeque::new(),
            protocol_complete: false,
            observed: false,
            ended: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if let Some(item) = state.queue.pop_front() {
                    return Some((item, state));
                }
                if state.ended {
                    return None;
                }
                match state.bytes.next().await {
                    Some(Ok(chunk)) => {
                        for segment in chunk.split_inclusive(|byte| *byte == b'\n') {
                            let result = state
                                .parser
                                .push(segment)
                                .and_then(|events| decode_events(&mut state.codec, events));
                            match result {
                                Ok(items) => {
                                    if queue_items(
                                        &mut state.queue,
                                        &mut state.protocol_complete,
                                        &mut state.observed,
                                        items,
                                    ) {
                                        state.ended = true;
                                    }
                                }
                                Err(error) => {
                                    state.protocol_complete = false;
                                    state.queue.push_back(StreamDelta::Error(error));
                                    state.ended = true;
                                }
                            }
                            if state.ended {
                                break;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        state.queue.push_back(StreamDelta::Error(after_output(
                            transport_from_reqwest(&error),
                            state.observed,
                        )));
                        state.ended = true;
                    }
                    None => {
                        match state.parser.finish() {
                            Ok(events) => match decode_events(&mut state.codec, events) {
                                Ok(items) => {
                                    let protocol_error = queue_items(
                                        &mut state.queue,
                                        &mut state.protocol_complete,
                                        &mut state.observed,
                                        items,
                                    );
                                    if !protocol_error {
                                        if state.protocol_complete {
                                            state.queue.push_back(StreamDelta::Completed);
                                        } else {
                                            state.queue.push_back(StreamDelta::Error(
                                                after_output(
                                                    transport(TransportCause::Body),
                                                    state.observed,
                                                ),
                                            ));
                                        }
                                    }
                                }
                                Err(error) => state.queue.push_back(StreamDelta::Error(error)),
                            },
                            Err(_) => state.queue.push_back(StreamDelta::Error(after_output(
                                transport(TransportCause::Body),
                                state.observed,
                            ))),
                        }
                        state.ended = true;
                    }
                }
            }
        }))
    }
}

fn decode_events(
    codec: &mut Codec,
    events: Vec<sse::Event>,
) -> Result<Vec<StreamDelta>, ProviderError> {
    let mut out = Vec::new();
    for event in events {
        for item in codec.event(&event)? {
            let terminal = matches!(item, StreamDelta::Error(_));
            out.push(item);
            if terminal {
                return Ok(out);
            }
        }
    }
    Ok(out)
}
/// An error after published output must not be retried.
fn after_output(mut error: ProviderError, observed: bool) -> ProviderError {
    if observed {
        error.retryable = false;
    }
    error
}
/// Deltas the runtime publishes to observers as they arrive.
fn is_observable(item: &StreamDelta) -> bool {
    match item {
        StreamDelta::TextDelta(_)
        | StreamDelta::ToolCallStart { .. }
        | StreamDelta::ToolCallArgsDelta { .. }
        | StreamDelta::ToolCallDone { .. } => true,
        StreamDelta::Reasoning { provenance, .. } => {
            *provenance == crabber_core::ReasoningProvenance::PublicSummary
        }
        _ => false,
    }
}
fn queue_items(
    queue: &mut VecDeque<StreamDelta>,
    protocol_complete: &mut bool,
    observed: &mut bool,
    items: Vec<StreamDelta>,
) -> bool {
    for item in items {
        *observed |= !*protocol_complete && is_observable(&item);
        if matches!(item, StreamDelta::Completed) {
            *protocol_complete = true;
            continue;
        }
        if let StreamDelta::Error(error) = item {
            // A retry after published output or a completed message would
            // repeat what observers already saw.
            let error = after_output(error, *observed || *protocol_complete);
            *protocol_complete = false;
            queue.push_back(StreamDelta::Error(error));
            return true;
        }
        if !*protocol_complete {
            queue.push_back(item);
        }
    }
    false
}
enum Codec {
    Responses(crate::responses::Codec),
    Messages(crate::messages::Codec),
    Chat(crate::chat::Codec),
}
impl Codec {
    fn event(&mut self, event: &sse::Event) -> Result<Vec<StreamDelta>, ProviderError> {
        match self {
            Self::Responses(c) => c.event(event),
            Self::Messages(c) => c.event(event),
            Self::Chat(c) => c.event(event),
        }
    }
}
