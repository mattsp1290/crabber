use sqlx::postgres::{PgConnectOptions, PgSslMode};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

#[derive(Default)]
struct Gate {
    query: Mutex<Option<(&'static [u8], u8)>>,
    waiting: AtomicU8,
    held: Notify,
    release: Notify,
}

pub(super) struct Proxy {
    port: u16,
    gate: Arc<Gate>,
    task: JoinHandle<io::Result<()>>,
    stop: oneshot::Sender<()>,
}

impl Proxy {
    pub(super) async fn start(options: &PgConnectOptions) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let address = (options.get_host().to_owned(), options.get_port());
        let gate = Arc::new(Gate::default());
        let task_gate = gate.clone();
        let (stop, mut stopping) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let mut accepted = 0;
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopping => break,
                    joined = connections.join_next(), if !connections.is_empty() => {
                        joined.unwrap().unwrap()?;
                    }
                    next = listener.accept() => {
                        let (client, _) = next?;
                        accepted += 1;
                        if accepted > 4 {
                            return Err(io::Error::other("test connection bound exceeded"));
                        }
                        let address = address.clone();
                        let gate = task_gate.clone();
                        connections.spawn(async move { relay(client, address, gate).await });
                    }
                }
            }
            drop(listener);
            while let Some(joined) = connections.join_next().await {
                joined.unwrap()?;
            }
            Ok(())
        });
        Self {
            port,
            gate,
            task,
            stop,
        }
    }

    pub(super) fn options(&self, options: PgConnectOptions) -> PgConnectOptions {
        options
            .host("127.0.0.1")
            .port(self.port)
            .ssl_mode(PgSslMode::Disable)
    }

    pub(super) fn arm(&self) {
        self.arm_query(b"BEGIN\0", b'T');
    }
    pub(super) fn arm_query(&self, query: &'static [u8], ready: u8) {
        *self.gate.query.lock().unwrap() = Some((query, ready));
    }
    pub(super) async fn held(&self) {
        self.gate.held.notified().await;
    }
    pub(super) fn release(&self) {
        self.gate.release.notify_one();
    }

    pub(super) async fn join(mut self) {
        let _ = self.stop.send(());
        if let Ok(result) = timeout(Duration::from_secs(5), &mut self.task).await {
            result.unwrap().unwrap();
        } else {
            self.task.abort();
            let _ = self.task.await;
            panic!("retained protocol proxy failed to stop after pool closure");
        }
    }
}

async fn relay(mut client: TcpStream, address: (String, u16), gate: Arc<Gate>) -> io::Result<()> {
    let mut backend = TcpStream::connect(address).await?;
    let length = client.read_u32().await?;
    let mut startup = vec![0; bounded(length, 4)?];
    client.read_exact(&mut startup).await?;
    backend.write_u32(length).await?;
    backend.write_all(&startup).await?;
    let (client_read, client_write) = client.into_split();
    let (backend_read, backend_write) = backend.into_split();
    match tokio::try_join!(
        forward(client_read, backend_write, gate.clone(), true),
        forward(backend_read, client_write, gate, false),
    ) {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::NotConnected
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn bounded(length: u32, header: u32) -> io::Result<usize> {
    length
        .checked_sub(header)
        .filter(|length| *length <= 8 << 20)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| io::Error::other("invalid test protocol frame length"))
}

async fn forward<R, W>(mut read: R, mut write: W, gate: Arc<Gate>, frontend: bool) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let tag = match read.read_u8().await {
            Ok(tag) => tag,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                write.shutdown().await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let length = read.read_u32().await?;
        let mut body = vec![0; bounded(length, 4)?];
        read.read_exact(&mut body).await?;
        if frontend && tag == b'Q' {
            let mut query = gate.query.lock().unwrap();
            if let Some((expected, ready)) = *query
                && body == expected
            {
                gate.waiting.store(ready, Ordering::SeqCst);
                *query = None;
            }
        }
        if !frontend && tag == b'Z' {
            let expected = gate.waiting.swap(0, Ordering::SeqCst);
            if expected != 0 {
                assert_eq!(body, [expected], "actual transaction command status");
                gate.held.notify_one();
                gate.release.notified().await;
            }
        }
        write.write_u8(tag).await?;
        write.write_u32(length).await?;
        write.write_all(&body).await?;
    }
}
