use sqlx::postgres::{PgConnectOptions, PgSslMode};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
    time::timeout,
};

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    begin: AtomicBool,
    held: Notify,
    release: Notify,
}

pub(super) struct Proxy {
    port: u16,
    gate: Arc<Gate>,
    task: JoinHandle<io::Result<()>>,
}

impl Proxy {
    pub(super) async fn start(options: &PgConnectOptions) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let address = (options.get_host().to_owned(), options.get_port());
        let gate = Arc::new(Gate::default());
        let task_gate = gate.clone();
        let task = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await?;
            let mut backend = TcpStream::connect(address).await?;
            let length = client.read_u32().await?;
            let mut startup = vec![0; bounded(length, 4)?];
            client.read_exact(&mut startup).await?;
            backend.write_u32(length).await?;
            backend.write_all(&startup).await?;
            let (client_read, client_write) = client.into_split();
            let (backend_read, backend_write) = backend.into_split();
            tokio::try_join!(
                forward(client_read, backend_write, task_gate.clone(), true),
                forward(backend_read, client_write, task_gate, false),
            )?;
            Ok(())
        });
        Self { port, gate, task }
    }

    pub(super) fn options(&self, options: PgConnectOptions) -> PgConnectOptions {
        options
            .host("127.0.0.1")
            .port(self.port)
            .ssl_mode(PgSslMode::Disable)
    }

    pub(super) fn arm(&self) {
        self.gate.armed.store(true, Ordering::SeqCst);
    }
    pub(super) async fn held(&self) {
        self.gate.held.notified().await;
    }
    pub(super) fn release(&self) {
        self.gate.release.notify_one();
    }

    pub(super) async fn join(mut self) {
        if let Ok(result) = timeout(Duration::from_secs(5), &mut self.task).await {
            result.unwrap().unwrap();
        } else {
            self.task.abort();
            let _ = self.task.await;
            panic!("retained protocol proxy failed to stop after pool closure");
        }
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
        if frontend && tag == b'Q' && body == b"BEGIN\0" && gate.armed.load(Ordering::SeqCst) {
            gate.begin.store(true, Ordering::SeqCst);
        }
        if !frontend && tag == b'Z' && gate.begin.swap(false, Ordering::SeqCst) {
            assert_eq!(
                body, b"T",
                "actual BEGIN must be in transaction before cancellation"
            );
            gate.armed.store(false, Ordering::SeqCst);
            gate.held.notify_one();
            gate.release.notified().await;
        }
        write.write_u8(tag).await?;
        write.write_u32(length).await?;
        write.write_all(&body).await?;
    }
}
