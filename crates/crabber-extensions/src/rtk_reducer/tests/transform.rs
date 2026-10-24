use super::super::{Filter, marker, transform::encoded};
#[test]
fn marker_and_encoded_acceptance_arithmetic_are_pinned() {
    let marker = marker(Filter::Log, 100);
    assert_eq!(marker, "[Reduced by RTK log; original_bytes=100]\n");
    assert_eq!(marker.len(), 41);
    assert_eq!(encoded(&marker), 44);
    for (original, output, raw, encoded_size, accepted) in [
        ("a".repeat(100), "a".repeat(30), 71, 74, true),
        ("a".repeat(100), "\"".repeat(55), 96, 154, false),
        ("\n".repeat(100), "a".repeat(110), 151, 154, true),
    ] {
        let candidate = marker.clone() + &output;
        assert_eq!(candidate.len(), raw);
        assert_eq!(encoded(&candidate), encoded_size);
        assert_eq!(encoded(&candidate) < encoded(&original), accepted);
    }
}
