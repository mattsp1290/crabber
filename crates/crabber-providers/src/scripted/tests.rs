use super::*;
use futures::StreamExt;

fn route() -> Selection {
    Selection {
        provider_id: "library-scripted".into(),
        model_id: "text".into(),
    }
}

#[test]
fn wrong_route_never_consumes_original_response() {
    let mut provider = ScriptedProvider::new(route(), vec!["first".into()]).unwrap();
    assert_eq!(
        provider.recipe(),
        (&route(), ["first".to_owned()].as_slice())
    );
    assert_eq!(provider.next_response(), Some("first"));
    let mut other = route();
    other.model_id = "other".into();
    assert!(matches!(
        provider.stream(&other),
        Err(ScriptedError::RouteMismatch)
    ));
    let text =
        futures::executor::block_on(async { provider.stream(&route()).unwrap().next().await });
    assert!(matches!(text, Some(StreamDelta::TextDelta(value)) if value == "first"));
    assert!(matches!(
        provider.stream(&route()),
        Err(ScriptedError::Exhausted)
    ));
    assert_eq!(provider.next_response(), None);
    assert_eq!(provider.recipe().1, &["first"]);
}

#[test]
fn concrete_stream_owns_response_independently_of_producer() {
    let mut provider =
        ScriptedProvider::new(route(), vec!["original".into(), String::new()]).unwrap();
    let first = provider.stream(&route()).unwrap();
    let second = provider.stream(&route()).unwrap();
    drop(provider);
    futures::executor::block_on(async {
        for (mut stream, expected) in [(first, "original"), (second, "")] {
            assert!(!stream.is_terminated());
            assert!(
                matches!(stream.next().await, Some(StreamDelta::TextDelta(text)) if text == expected)
            );
            assert!(!stream.is_terminated());
            assert!(matches!(stream.next().await, Some(StreamDelta::Completed)));
            assert!(stream.is_terminated());
            assert!(stream.next().await.is_none());
            assert!(stream.next().await.is_none());
        }
    });
}

#[test]
fn constructor_checks_each_limit_at_its_boundary() {
    assert!(ScriptedProvider::new(route(), vec![]).is_ok());
    assert!(ScriptedProvider::new(route(), vec![String::new(); MAX_RESPONSES]).is_ok());
    assert!(matches!(
        ScriptedProvider::new(route(), vec![String::new(); MAX_RESPONSES + 1]),
        Err(ScriptedError::Capacity)
    ));
    assert!(
        ScriptedProvider::new(
            route(),
            vec!["a".repeat(MAX_TEXT_BYTES); MAX_TOTAL_BYTES / MAX_TEXT_BYTES]
        )
        .is_ok()
    );
    assert!(matches!(
        ScriptedProvider::new(route(), vec!["a".repeat(MAX_TEXT_BYTES + 1)]),
        Err(ScriptedError::Capacity)
    ));
    let mut too_large = vec!["a".repeat(MAX_TEXT_BYTES); MAX_TOTAL_BYTES / MAX_TEXT_BYTES];
    too_large.push("a".into());
    assert!(matches!(
        ScriptedProvider::new(route(), too_large),
        Err(ScriptedError::Capacity)
    ));
    let mut invalid = route();
    invalid.provider_id = "x".repeat(MAX_ROUTE_BYTES);
    assert!(ScriptedProvider::new(invalid, vec![]).is_ok());
    for provider_id in [
        String::new(),
        "bad\nroute".into(),
        "x".repeat(MAX_ROUTE_BYTES + 1),
        "é".repeat(MAX_ROUTE_BYTES),
    ] {
        let mut invalid = route();
        invalid.provider_id = provider_id;
        assert!(matches!(
            ScriptedProvider::new(invalid, vec![]),
            Err(ScriptedError::InvalidRoute)
        ));
    }
}
