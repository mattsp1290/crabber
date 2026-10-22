use super::{Limits, Source};

pub(super) fn bound_sources(records: Vec<Source>, limits: &Limits) -> Vec<Source> {
    records
        .into_iter()
        .take(limits.max_results)
        .filter_map(|record| {
            if !valid_source_url(&record.url, limits.max_url_bytes)
                || record.title.contains('\0')
                || record.snippet.contains('\0')
            {
                return None;
            }
            Some(Source {
                title: utf8_prefix(record.title, limits.max_title_bytes),
                url: record.url,
                snippet: utf8_prefix(record.snippet, limits.max_snippet_bytes),
            })
        })
        .collect()
}
fn utf8_prefix(mut value: String, max: usize) -> String {
    if value.len() > max {
        let mut end = max;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
    }
    value
}
fn valid_source_url(value: &str, max: usize) -> bool {
    if value.len() > max || value.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return false;
    }
    let Some(rest) = value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"))
    else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty()
        || authority.bytes().any(|b| {
            b.is_ascii() && !b.is_ascii_alphanumeric() && !b"-._~!$&'()*+,;=:[]<>\"".contains(&b)
        })
    {
        return false;
    }
    let bytes = value.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'%'
            && (bytes.get(i + 1).is_none_or(|b| !b.is_ascii_hexdigit())
                || bytes.get(i + 2).is_none_or(|b| !b.is_ascii_hexdigit()))
        {
            return false;
        }
    }
    url::Url::parse(value).is_ok_and(|u| {
        u.host_str().is_some_and(|h| !h.is_empty())
            && u.username().is_empty()
            && u.password().is_none()
    })
}
