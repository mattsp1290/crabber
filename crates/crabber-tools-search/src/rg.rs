//! Argv-only ripgrep invocation and bounded streaming collection.
use crate::{MAX_STDERR_BYTES, Options, args::Args, parse::Parser};
use crabber_tools_core::*;
use serde_json::{Value, json};
use std::{io, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    time::Instant,
};
const RECORD_CAP: usize = 8 * 1024 * 1024;
// Buffer lives outside the select branch: losing a select race never loses a
// partial JSON record or permits unbounded read_until allocation.
async fn next_record(
    reader: &mut BufReader<tokio::process::ChildStdout>,
    buffer: &mut Vec<u8>,
) -> io::Result<bool> {
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(!buffer.is_empty());
        }
        let end = available.iter().position(|b| *b == b'\n').map(|n| n + 1);
        let n = end.unwrap_or(available.len());
        if buffer.len() + n > RECORD_CAP {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        buffer.extend_from_slice(&available[..n]);
        reader.consume(n);
        if end.is_some() {
            return Ok(true);
        }
    }
}
pub(crate) async fn run(
    options: &Options,
    args: Args,
    ctx: &crabber::extension::ToolContext,
    permit: impl Send + 'static,
) -> Value {
    let start = Instant::now();
    let mut command = Command::new(&options.policy.rg_binary);
    command.args(["--json", "--no-messages", "--no-config"]);
    if args.literal {
        command.arg("-F");
    }
    if args.ignore_case {
        command.arg("-i");
    }
    if args.context > 0 {
        command.arg("-C").arg(args.context.to_string());
    }
    command.arg("-e").arg(&args.pattern);
    if let Some(globs) = &args.glob {
        for glob in globs.values() {
            command.arg("-g").arg(glob);
        }
    }
    command
        .arg("--")
        .arg(&args.path)
        .current_dir(options.root.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    apply_policy(&mut command, &options.policy.env, options.policy.run_as);
    command.env_remove("RIPGREP_CONFIG_PATH");
    let context = args.context;
    let limit = args.limit;
    let regex = !args.literal;
    match run_process_owned_with(ctx, command, permit, move |guard| Box::pin(async move {
    let mut stdout = BufReader::new(guard.take_stdout().expect("piped stdout"));
    let mut stderr = guard.take_stderr().expect("piped stderr");
    let mut diagnostic = CappedOutput::default();
    let mut error_chunk = [0; 4096];
    let mut record = vec![];
    let mut parser = Parser::new(args.context, args.limit, !args.literal);
    let (mut out_eof, mut err_eof) = (false, false);
    let mut status = None;
    let mut timed_out = false;
    let mut failed = false;
    let deadline = start + Duration::from_secs(args.timeout_seconds);
    let mut stop = deadline;
    loop {
        if out_eof && err_eof && status.is_some() {
            guard.finish();
            break;
        }
        tokio::select! {biased;
            _=tokio::time::sleep_until(stop)=>{
                timed_out=status.is_none();guard.kill_group();
                let _=guard.wait_with_deadline(Instant::now()+WAIT_AFTER_KILL).await;break;
            },
            exit=guard.wait(),if status.is_none()=>{
                match exit{Ok(s)=>status=Some(s),Err(_)=>{failed=true;break;}}
                stop=deadline.min(Instant::now()+WAIT_AFTER_KILL);
            },
            next=next_record(&mut stdout,&mut record),if !out_eof=>{
                match next {
                    Ok(true)=>{parser.feed(&record);record.clear();if parser.reason.is_some(){guard.kill_group();let _=guard.wait_with_deadline(Instant::now()+WAIT_AFTER_KILL).await;break;}},
                    Ok(false)=>out_eof=true,
                    Err(_)=>{failed=true;guard.kill_group();let _=guard.wait_with_deadline(Instant::now()+WAIT_AFTER_KILL).await;break;},
                }
            },
            n=stderr.read(&mut error_chunk),if !err_eof=>{
                match n{Ok(n)=>{err_eof=n==0;diagnostic.push(&error_chunk[..n],MAX_STDERR_BYTES);},Err(_)=>{failed=true;break;}}
            },
        }
    }
    Ok(result(parser, start, timed_out, failed, status.and_then(|s| s.code()), &diagnostic.text()))
    })).await {
        Ok(value) => value,
        Err(ProcessRunError::Unavailable) => failed_value(category::UNAVAILABLE, "call resources unavailable"),
        Err(_) => result(Parser::new(context, limit, regex), start, false, true, None, ""),
    }
}

fn result(
    parser: Parser,
    start: Instant,
    timed_out: bool,
    failed: bool,
    code: Option<i32>,
    stderr: &str,
) -> Value {
    let error = failed || parser.malformed || !matches!(code, Some(0 | 1));
    let partial = error && !parser.matches.is_empty() && parser.reason.is_none() && !timed_out;
    let invalid = stderr
        .lines()
        .map(str::trim)
        .any(|s| s == "regex parse error:" || s.starts_with("rg: regex parse error:"));
    let mut value = if timed_out {
        failed_value(category::TIMEOUT, "search exceeded timeout")
    } else if parser.reason.is_some() || !error || partial {
        json!({"outcome":"succeeded"})
    } else {
        failed_value(
            if invalid {
                category::INVALID_PATTERN
            } else {
                category::EXEC_FAILED
            },
            if invalid {
                "invalid regular expression"
            } else {
                "ripgrep execution failed"
            },
        )
    };
    if partial {
        value["partial"] = json!(true);
        value["error"] =
            json!({"category":"exec_failed","message":"ripgrep returned incomplete results"});
    }
    value["match_count"] = json!(parser.matches.len());
    value["matches"] = json!(parser.matches);
    value["duration_ms"] = json!(start.elapsed().as_millis() as u64);
    if timed_out {
        value["timed_out"] = json!(true);
    }
    if let Some(reason) = parser.reason {
        value["truncated"] = json!(true);
        value["truncation_reason"] = json!(reason);
    }
    value
}
fn failed_value(category: &'static str, message: &str) -> Value {
    crabber_tools_core::failed(category, message)
}
