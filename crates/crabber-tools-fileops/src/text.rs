//! Bounded text scanning; no line can allocate in proportion to file size.
use crate::MAX_READ_LINE_BYTES;
use std::io::{self, BufRead};

pub(crate) fn utf8_prefix(bytes: &[u8], truncated: bool) -> Result<&str, ()> {
    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(e) if truncated && e.error_len().is_none() => {
            std::str::from_utf8(&bytes[..e.valid_up_to()]).map_err(|_| ())
        }
        Err(_) => Err(()),
    }
}
// Read and drain one line, retaining only its bounded prefix. Validate UTF-8
// incrementally, including bytes discarded beyond the retained prefix.
pub(crate) fn line(reader: &mut impl BufRead) -> io::Result<Option<(String, bool)>> {
    let mut kept = Vec::new();
    let mut total = 0usize;
    let mut tail = Vec::new();
    let mut any = false;
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            if !tail.is_empty() {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            break;
        }
        let end = buf.iter().position(|b| *b == b'\n').map(|i| i + 1);
        let n = end.unwrap_or(buf.len());
        let chunk = &buf[..n];
        any = true;
        total = total.saturating_add(n);
        if chunk.contains(&0) {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        let mut check = std::mem::take(&mut tail);
        check.extend_from_slice(chunk);
        match std::str::from_utf8(&check) {
            Ok(_) => {}
            Err(e) if e.error_len().is_none() => tail.extend_from_slice(&check[e.valid_up_to()..]),
            Err(_) => return Err(io::Error::from(io::ErrorKind::InvalidData)),
        }
        let retain = n.min(MAX_READ_LINE_BYTES.saturating_sub(kept.len()));
        kept.extend_from_slice(&chunk[..retain]);
        reader.consume(n);
        if end.is_some() {
            break;
        }
    }
    if !any {
        return Ok(None);
    }
    let truncated = total > MAX_READ_LINE_BYTES;
    let text =
        utf8_prefix(&kept, truncated).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    let mut text = text.to_owned();
    if truncated {
        text.push_str("… [line truncated]\n");
    }
    Ok(Some((text, truncated)))
}
