//! Normalize original rg byte ranges into the bounded, possibly lossy returned line.
use crate::{MAX_LINE_BYTES, MAX_RESULT_BYTES};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

pub(crate) fn retained(data: &Value, line: &str, include: bool) -> Option<Vec<Value>> {
    let mut result = vec![];
    if !include {
        return Some(result);
    }
    let raw = if let Some(text) = data["lines"]["text"].as_str() {
        text.as_bytes().to_vec()
    } else {
        data["lines"]["bytes"]
            .as_str()
            .and_then(|s| STANDARD.decode(s).ok())
            .unwrap_or_default()
    };
    let mut bytes = 2usize;
    if let Some(subs) = data["submatches"].as_array() {
        for sub in subs {
            let Some((start, end)) = sub["start"].as_u64().zip(sub["end"].as_u64()) else {
                continue;
            };
            // Lossy UTF-8 never shrinks the admitted prefix, so original offsets
            // beyond this cap cannot reference retained text. Prefix decoding is
            // therefore bounded to 4 KiB even for an 8 MiB input record.
            if start > end || end > MAX_LINE_BYTES as u64 || end > raw.len() as u64 {
                continue;
            }
            let start = String::from_utf8_lossy(&raw[..start as usize]);
            let end = String::from_utf8_lossy(&raw[..end as usize]);
            if !line.starts_with(start.as_ref()) || !line.starts_with(end.as_ref()) {
                continue;
            }
            let (start, end) = (start.len(), end.len());
            if start > end || !line.is_char_boundary(start) || !line.is_char_boundary(end) {
                continue;
            }
            let item = json!({"text":&line[start..end],"start":start,"end":end});
            bytes += serde_json::to_vec(&item).unwrap().len() + 1;
            if bytes > MAX_RESULT_BYTES {
                return None;
            }
            result.push(item);
        }
    }
    Some(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filters_after_crossing_and_multibyte_ranges() {
        for (raw, start, end) in [
            (format!("{}needle", "x".repeat(4500)), 4500, 4506),
            (format!("{}needle", "x".repeat(4094)), 4094, 4100),
            (format!("{}é", "x".repeat(4095)), 4095, 4097),
        ] {
            let data = json!({"lines":{"text":raw},"submatches":[{"start":start,"end":end}]});
            let mut boundary = raw.len().min(MAX_LINE_BYTES);
            while !raw.is_char_boundary(boundary) {
                boundary -= 1;
            }
            assert!(retained(&data, &raw[..boundary], true).unwrap().is_empty());
        }
    }
    #[test]
    fn translates_lossy_offsets_and_suppresses_literal_spans() {
        let data = json!({"lines":{"bytes":STANDARD.encode(b"\xffneedle")},"submatches":[{"start":1,"end":7}]});
        let line = "\u{fffd}needle";
        assert_eq!(
            retained(&data, line, true).unwrap(),
            vec![json!({"start":3,"end":9,"text":"needle"})]
        );
        assert!(retained(&data, line, false).unwrap().is_empty());
    }
}
