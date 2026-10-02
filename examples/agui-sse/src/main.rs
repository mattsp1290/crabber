use agui_sse::{
    check::{CheckResult, factory, journey},
    host::Host,
};
use crabber::session::MemoryStore;
use std::{net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> CheckResult {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--check"] {
        return agui_sse::check::run().await;
    }
    let address: SocketAddr = match args.as_slice() {
        [] => "127.0.0.1:3000".parse()?,
        [flag, address] if flag == "--listen" => address.parse()?,
        _ => return Err("usage: agui-sse [--check | --listen 127.0.0.1:3000]".into()),
    };
    if !address.ip().is_loopback() {
        return Err("example requires a loopback address".into());
    }
    let host = Host::new(factory(Arc::new(MemoryStore::new()), journey(false)));
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("AG-UI fake host: http://{}/run", listener.local_addr()?);
    let stop = tokio_util::sync::CancellationToken::new();
    let signal = stop.clone();
    let router = host.router();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(signal.cancelled_owned())
            .await
    });
    tokio::select! {
        result = &mut server => { result??; return Ok(()); }
        result = tokio::signal::ctrl_c() => result?,
    }
    let cleanup = host.shutdown().await;
    stop.cancel();
    // A noncooperative worker must not make graceful HTTP shutdown unbounded.
    let listener_cleanup = tokio::time::timeout(host.cleanup, &mut server).await;
    cleanup?;
    listener_cleanup???;
    Ok(())
}
