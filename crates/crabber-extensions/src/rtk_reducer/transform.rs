use super::{
    Filter,
    config::{CanonicalBinding, Selection},
    engine::{Engine, Lease, Reduced},
    marker,
};
use crabber::extension::{ToolInput, ToolOutcomeClass, ToolResultContext, TransformPhase};
use serde_json::Value;
use std::sync::{Arc, atomic::Ordering};

pub(super) struct Transformer {
    engine: Arc<Engine>,
}
impl Transformer {
    pub(super) fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
    fn binding<'a>(&'a self, context: &ToolResultContext) -> Option<&'a CanonicalBinding> {
        if context.class() != ToolOutcomeClass::Succeeded
            || context.is_error()
            || !context.resolved()
            || context.phase() != TransformPhase::Ordinary
        {
            return None;
        }
        let ToolInput::Normalized(input) = context.input() else {
            return None;
        };
        let binding = self
            .engine
            .configuration
            .bindings
            .get(context.tool_name())?;
        if let Some((field, allowed)) = &binding.match_input {
            let text = input.as_object()?.get(field)?.as_str()?;
            if text.len() > self.engine.configuration.limits.max_input_bytes
                || !allowed.contains(text)
            {
                return None;
            }
        }
        Some(binding)
    }
    fn valid_text(&self, value: &Value) -> bool {
        value.as_str().is_some_and(|s| {
            s.len() <= self.engine.configuration.limits.max_field_bytes && !s.contains('\0')
        })
    }
    pub(super) async fn transform(&self, context: ToolResultContext, mut value: Value) -> Value {
        let binding = self.binding(&context).filter(|b| match &b.selection {
            Selection::Text(_) => self.valid_text(&value),
            Selection::JsonFields(fields) => value.as_object().is_some_and(|o| {
                fields
                    .iter()
                    .all(|f| o.get(&f.name).is_some_and(|s| self.valid_text(s)))
            }),
        });
        let Some(binding) = binding else {
            self.engine
                .counters
                .passed_through
                .fetch_add(1, Ordering::Relaxed);
            return value;
        };
        let Ok(lease) = self.engine.acquire(&context) else {
            return value;
        };
        match &binding.selection {
            Selection::Text(filter) => {
                if let Ok(Some(candidate)) = self
                    .candidate(&lease, *filter, value.as_str().unwrap())
                    .await
                {
                    self.engine.counters.reduced.fetch_add(1, Ordering::Relaxed);
                    return Value::String(candidate);
                }
            }
            Selection::JsonFields(fields) => {
                // Stage replacements only. A later process failure rolls back
                // earlier candidates without cloning or serializing unselected data.
                let mut replacements = Vec::new();
                for field in fields {
                    match self
                        .candidate(&lease, field.filter, value[&field.name].as_str().unwrap())
                        .await
                    {
                        Ok(Some(candidate)) => replacements.push((&field.name, candidate)),
                        Ok(None) => {}
                        Err(()) => return value,
                    }
                }
                if !replacements.is_empty() {
                    self.engine.counters.reduced.fetch_add(1, Ordering::Relaxed);
                    let object = value.as_object_mut().unwrap();
                    for (name, candidate) in replacements {
                        object.insert(name.clone(), Value::String(candidate));
                    }
                }
            }
        }
        value
    }
    async fn candidate(
        &self,
        lease: &Lease,
        filter: Filter,
        original: &str,
    ) -> Result<Option<String>, ()> {
        if original.is_empty() {
            return Ok(None);
        }
        let Reduced::Output(output) = lease.reduce(filter, original).await else {
            return Err(());
        };
        if output != original {
            let candidate = marker(filter, original.len()) + &output;
            if encoded(&candidate) < encoded(original) {
                return Ok(Some(candidate));
            }
        }
        self.engine.counters.no_gain.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
}
pub(super) fn encoded(s: &str) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, s).expect("JSON string serializes");
    count.0
}
