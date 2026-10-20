//! Exercise the real request deadline only after partial arguments reach HTTP.
use agui_sse::check::{CheckResult, Decoder, Server, input};
use crabber_agui::ag_ui_core::event::Event;
use futures::StreamExt;
use std::{
    cell::Cell,
    future::Future,
    time::{Duration, Instant},
};

struct ResumeTime(bool);
impl ResumeTime {
    fn resume(&mut self) {
        if self.0 {
            tokio::time::resume();
            self.0 = false;
        }
    }
}
impl Drop for ResumeTime {
    fn drop(&mut self) {
        self.resume();
    }
}

// Keep the current-thread runtime runnable while waiting for actual loopback I/O.
// Otherwise a paused Tokio clock may auto-advance the deadline before the OS
// delivers headers. The real wall bound still fails a stalled fixture finitely.
async fn frozen_io<T>(
    stage: &Cell<&str>,
    future: impl Future<Output = CheckResult<T>>,
) -> CheckResult<T> {
    let expires = Instant::now() + Duration::from_secs(2);
    tokio::pin!(future);
    loop {
        if Instant::now() >= expires {
            return Err(format!("deadline fixture stalled at {}", stage.get()).into());
        }
        tokio::select! {
            biased;
            result = &mut future => return result,
            () = tokio::task::yield_now() => (),
        }
    }
}

/// # Errors
/// Returns real HTTP/framing failures or a finite fixture progress failure.
pub(super) async fn request(server: &Server, deadline: Duration) -> CheckResult<Vec<Event>> {
    tokio::time::pause();
    let mut resume = ResumeTime(true);
    let stage = Cell::new("HTTP headers");
    let (mut body, mut raw) =
        frozen_io(&stage, async {
            let response = reqwest::Client::new()
                .post(&server.url)
                .json(&input("deadline"))
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert_eq!(
                response.headers()["content-type"],
                crabber_agui::ag_ui_encoder::CONTENT_TYPE
            );
            stage.set("partial HTTP ToolCallArgs");
            let mut body = response.bytes_stream();
            let mut observed = Decoder::default();
            let mut raw = Vec::new();
            while !observed.events.iter().any(
                |event| matches!(event, Event::ToolCallArgs(arguments) if arguments.delta == "{"),
            ) {
                let bytes = body
                    .next()
                    .await
                    .ok_or("SSE ended before partial arguments")??;
                observed.push(&bytes)?;
                raw.extend_from_slice(&bytes);
            }
            Ok((body, raw))
        })
        .await?;

    // The production host's original request deadline remains unchanged. Only
    // this test clock advances, after real argument presentation is established.
    tokio::time::advance(deadline).await;
    // Tokio rounds fractional deadlines up to a timer-wheel millisecond. Let
    // that final tick and the real cleanup joins progress on the same clock.
    resume.resume();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(bytes) = body.next().await {
            raw.extend_from_slice(&bytes?);
        }
        Ok::<(), reqwest::Error>(())
    })
    .await
    .map_err(|_| "deadline fixture stalled after original request deadline")??;
    assert!(!raw.windows(6).any(|bytes| bytes == b"[DONE]"));
    assert!(!raw.windows(4).any(|bytes| bytes == b"id: "));
    assert!(!String::from_utf8_lossy(&raw).contains("SECRET_SENTINEL"));
    let mut decoder = Decoder::default();
    for byte in raw {
        decoder.push(&[byte])?;
    }
    decoder.finish()
}
