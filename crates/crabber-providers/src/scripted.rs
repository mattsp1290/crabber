//! Concrete value-only scripted provider; no generic resolver or ownership claim.
use crate::{Selection, StreamDelta};
use futures::{Stream, stream::FusedStream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

const MAX_RESPONSES: usize = 64;
const MAX_TEXT_BYTES: usize = 64 << 10;
const MAX_TOTAL_BYTES: usize = 1 << 20;
const MAX_ROUTE_BYTES: usize = 128;

/// Finite recipe, routing and exhaustion failures. No provider effects occur on refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScriptedError {
    #[error("invalid scripted route")]
    InvalidRoute,
    #[error("scripted recipe capacity exceeded")]
    Capacity,
    #[error("scripted route does not match")]
    RouteMismatch,
    #[error("scripted responses exhausted")]
    Exhausted,
}

/// An original owned text recipe and its cursor. This is not cleanup or native evidence.
///
/// The concrete producer has no cloned handle, callback, external resource, or
/// conversion to a generic `Resolver` or `Streamer`.
pub struct ScriptedProvider {
    selection: Selection,
    responses: Vec<String>,
    next: usize,
}
impl ScriptedProvider {
    /// Consume bounded owned values, preserving the original recipe throughout use.
    /// Routes are nonempty, control-free UTF-8 strings of at most 128 bytes each.
    /// At most 64 responses, 64 KiB per response and 1 MiB total are accepted.
    /// These input limits supply no ownership or native-policy evidence.
    /// # Errors
    /// Rejects empty/control-bearing routes and excessive response counts or bytes.
    pub fn new(selection: Selection, responses: Vec<String>) -> Result<Self, ScriptedError> {
        for route in [&selection.provider_id, &selection.model_id] {
            if route.is_empty()
                || route.len() > MAX_ROUTE_BYTES
                || route.chars().any(char::is_control)
            {
                return Err(ScriptedError::InvalidRoute);
            }
        }
        if responses.len() > MAX_RESPONSES {
            return Err(ScriptedError::Capacity);
        }
        let mut total = 0_usize;
        for response in &responses {
            if response.len() > MAX_TEXT_BYTES {
                return Err(ScriptedError::Capacity);
            }
            total = total
                .checked_add(response.len())
                .ok_or(ScriptedError::Capacity)?;
            if total > MAX_TOTAL_BYTES {
                return Err(ScriptedError::Capacity);
            }
        }
        Ok(Self {
            selection,
            responses,
            next: 0,
        })
    }

    /// Borrow the immutable original values, for metadata or pre-copy accounting.
    /// This is not source authentication or cleanup evidence.
    #[must_use]
    pub fn recipe(&self) -> (&Selection, &[String]) {
        (&self.selection, &self.responses)
    }

    /// Borrow the next original text without consuming it or creating a stream.
    #[must_use]
    pub fn next_response(&self) -> Option<&str> {
        self.responses.get(self.next).map(String::as_str)
    }

    /// Select exactly one original response. Wrong routes do not advance the cursor.
    /// # Errors
    /// Refuses a different route or an exhausted recipe.
    pub fn stream(&mut self, selection: &Selection) -> Result<ScriptedStream, ScriptedError> {
        if selection != &self.selection {
            return Err(ScriptedError::RouteMismatch);
        }
        let text = self
            .responses
            .get(self.next)
            .ok_or(ScriptedError::Exhausted)?
            .clone();
        self.next += 1;
        Ok(ScriptedStream {
            text: Some(text),
            completed: false,
        })
    }
}

/// Owned text followed by exactly one completion delta. End-of-stream is not join evidence.
/// No trait-object or underlying provider reference is exposed.
pub struct ScriptedStream {
    text: Option<String>,
    completed: bool,
}
impl Stream for ScriptedStream {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(text) = self.text.take() {
            return Poll::Ready(Some(StreamDelta::TextDelta(text)));
        }
        if !self.completed {
            self.completed = true;
            return Poll::Ready(Some(StreamDelta::Completed));
        }
        Poll::Ready(None)
    }
}
impl FusedStream for ScriptedStream {
    fn is_terminated(&self) -> bool {
        self.text.is_none() && self.completed
    }
}

#[cfg(test)]
mod tests;
