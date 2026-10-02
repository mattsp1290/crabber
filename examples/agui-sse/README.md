# AG-UI SSE host

Run the credential-free fake-provider/native-echo journey:

```sh
cargo run -p agui-sse -- --check
cargo run -p agui-sse -- --listen 127.0.0.1:3000
```

POST the single-text-user request in [docs/ag-ui.md](../../docs/ag-ui.md) to
`/run`. The server accepts loopback addresses only. It maintains process-local
memory history under threadId and uses runId only as a wire alias. Native echo
results remove a private sentinel before durable settlement/projection. Every
request constructs a fresh provider script with unique call IDs; the host store
and thread admission remain shared.

The check uses a real pinned Rust HttpAgent for ASCII text/tools and a separate
byte-first decoder for fragmented Unicode, comparing public content with stored
messages. The pinned client's arbitrary split-UTF-8 path remains unsupported.
The [full contract](../../docs/ag-ui.md) documents request/status profiles,
projection bounds, reasoning opt-in, exactly-one terminal, disconnect/shutdown
cleanup and required production-host policies. Tests use server-owned fixtures;
request bodies cannot select faults or arbitrary tools.
