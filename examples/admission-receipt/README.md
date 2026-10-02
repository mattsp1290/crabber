# Admission completion example

Run `cargo run -p admission-receipt -- --memory`, or set
`CRABBER_TEST_POSTGRES_URL` to a disposable PostgreSQL 14+ database and run
`cargo run -p admission-receipt --features postgres -- --postgres`.

The demo-only wrapper commits the production keyed admission transaction and
loses its response before execution spawn. A fresh Agent restores the original
semantic configuration, looks up and exactly retries the original key, waits for
the persisted positive lease to expire, then calls `recover_admission`. Its handle
completes the original run with one provider request, one user message and the same
receipt. Later prompt and recovery retries return metadata only. Memory mode uses
shared store state; PostgreSQL survives real process restarts, as independently
proven by the facade process suite.

Persist the original stable session/key/text/config/opaque behavior fingerprint
before sending. LiveLease means wait/observe; semantic conflict means restore the
original version. Unknown admission/claim/begin outcomes mean reconcile the same
key. Started/legacy evidence never authorizes a fresh initial provider call. A
handle must complete before declaring success. Pre-admission extension/plan setup
must not perform turn effects. See [the full host protocol](../../docs/admission-receipts.md).

Schema 5 requires stopping writers, backing up, explicitly migrating and deploying
matching binaries. No mixed v4/v5 writers or down migration; rollback uses backup
restore or forward repair. Capsules/receipts remain for the retained session
lifetime. The example uses fake provider output and requires no credentials.
