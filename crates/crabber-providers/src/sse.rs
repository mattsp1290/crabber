#![allow(clippy::missing_errors_doc)]
//! Bounded, incremental Server-Sent Events framing.
use crate::{ProviderError, ProviderErrorKind};

pub const MAX_LINE_BYTES: usize = 2 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub name: String,
    pub data: String,
}
#[derive(Default)]
pub struct Parser {
    line: Vec<u8>,
    event: String,
    data: String,
}
impl Parser {
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Event>, ProviderError> {
        let mut out = Vec::new();
        for &byte in chunk {
            if byte == b'\n' {
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                let line =
                    std::str::from_utf8(&self.line).map_err(|_| invalid("invalid SSE UTF-8"))?;
                if line.is_empty() {
                    if !self.data.is_empty() {
                        self.data.pop();
                        out.push(Event {
                            name: std::mem::take(&mut self.event),
                            data: std::mem::take(&mut self.data),
                        });
                    }
                } else if !line.starts_with(':') {
                    let (field, value) = line.split_once(':').unwrap_or((line, ""));
                    let value = value.strip_prefix(' ').unwrap_or(value);
                    match field {
                        "event" => self.event = value.to_owned(),
                        "data" => {
                            if self
                                .data
                                .len()
                                .saturating_add(value.len())
                                .saturating_add(1)
                                > MAX_LINE_BYTES
                            {
                                return Err(invalid("SSE event limit exceeded"));
                            }
                            self.data.push_str(value);
                            self.data.push('\n');
                        }
                        _ => {}
                    }
                }
                self.line.clear();
            } else {
                self.line.push(byte);
                if self.line.len() > MAX_LINE_BYTES {
                    return Err(invalid("SSE line limit exceeded"));
                }
            }
        }
        Ok(out)
    }
    pub fn finish(&mut self) -> Result<Vec<Event>, ProviderError> {
        self.push(b"\n\n")
    }
}
fn invalid(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn framing_across_chunks() {
        let mut parser = Parser::default();
        assert_eq!(
            parser.push(b"event: foo\r\ndata: a").unwrap(),
            [] as [super::Event; 0]
        );
        assert_eq!(
            parser.push(b"\r\ndata: b\r\n\r\n").unwrap(),
            vec![Event {
                name: "foo".into(),
                data: "a\nb".into()
            }]
        );
    }
    #[test]
    fn bounds() {
        assert!(
            Parser::default()
                .push(&vec![b'a'; MAX_LINE_BYTES + 1])
                .is_err()
        );
        let mut parser = Parser::default();
        let half = "x".repeat(MAX_LINE_BYTES / 2);
        parser.push(format!("data: {half}\n").as_bytes()).unwrap();
        assert!(parser.push(format!("data: {half}\n").as_bytes()).is_err());
    }
}
