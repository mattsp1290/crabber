# Session resource ownership

`Store::supports_session_owners()` is available on Memory, PostgreSQL schema 13,
and SQLite schema 9. The SQL adapters persist ownership across controller
restarts. Hosts explicitly reserve and begin an owner before starting the
corresponding resource, and report its actual join after cleanup finishes.
This ledger does not automatically register every runtime or factory resource.

`ReserveSessionOwner` binds one resource ID to an exact session, workspace,
directory, component, behavior fingerprint and optional native execution parent.
A preparation can be reserved before a Session or Run exists. The reservation
stores an immutable session context without creating either object. Later
managed creation and ordinary admission must preserve that context. Opaque
workspace and directory values, including newlines, are preserved; NUL cannot
be stored. Session IDs accept bounded opaque Unicode.

Operation IDs have a separate typed ownership namespace. An exact retry returns
its original receipt before checking current state, archive status, leases,
parent authority or context. Changed semantics conflict. Reserve lease duration
is an acquisition preference, excluded from its stable semantic identity.

Only a freshly applied Reserve or ReclaimUnstarted returns private authority.
Receipt lookup, owner snapshots and retries return metadata. After an unknown
acknowledgement, reconcile the original key; an original receipt cannot recover
a lost token. An expired reservation that never began can be explicitly
reclaimed into a new generation. Started work requires explicit uncertainty or
join resolution. Expiry alone never establishes cleanup.

`Begin` requires the exact unexpired authority. `Renew` preserves the public
snapshot and session revision. Other public changes advance the session's public
revision when that Session exists. An unchanged expired authority can still
report its actual `Join`; a rotated generation cannot. Host `ResolveJoined`
records the supplied resolver identity, authority claim and termination reason.
It requires an expired lease and represents the host's explicit cleanup report.

Detached invocation reservations additionally authenticate the actual native
Started attempt, or Started phase and its Running logical call. Store-clock
leases, run and attempt tokens, generation, call binding and original native
Begin receipts/events must agree. Independent Begin witnesses and a Run fence
journal prevent restored mutable rows from reviving an old parent after
revocation or takeover. Ordinary private lease renewal keeps the same issuance.
Legacy executions without an original witness cannot grant a new parent handoff.

SQL operations use the session mutation gate, including when no Session exists.
Each current owner is checked against its original reservation and the
independently latest immutable audit. Context checks corroborate the first
reservation. Identity anti-joins detect missing or substituted current owners.
Capacity is 1,024 non-Joined owners per session, derived from independently latest
audit state. Historical private audit bodies are not scanned to reserve another
owner. Selected current/audit bodies are bounded to 32 KiB before transfer;
context bodies to 32 KiB. Parent Run proof reads transfer at most 16 KiB of actual
metadata, excluding checkpoint and error content, and corroborate indexed
identity, status, token and lease. Corrupt or oversized selected evidence refuses
the operation with a sanitized unknown-storage outcome.

PostgreSQL reads use a read-only Repeatable Read transaction; SQLite reads use a
reader transaction. These metadata reads never return parent credentials, owner
tokens or execution output, and never grant authority or infer resource joins.
