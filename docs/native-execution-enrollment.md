# Native execution enrollment birth records

New native Runs retain an independent enrollment origin and the first raw-grant record. These records establish a permanent negative fact: the Run cannot prove complete ownership of its native execution. They do not prove a main-task join, destruction of an object, or a resource policy.

Every currently supported creation route is raw, including an admission completed by an owned preparation task. The task's original registration and selected driver source remain useful facts, but neither adopts all native grant holders into that task. There is no positive enrollment API in this foundation.

## Birth, legacy state and replay

PostgreSQL schema 22 and SQLite schema 18 attach an immutable enrollment marker and exact origin ID to each newly created Run. The origin names its original session/Run, native issuance sequence 1 and issuance digest. A separate immutable record binds the first raw grant and its target digest to that origin. Deferred foreign keys require a raw Run, origin, native first source and first raw record to be committed together. A failed transaction publishes none of them.

Retained Runs receive marker 0 and no origin during migration. Later claims, status writes and owned driver registration cannot promote these Runs. An identical admission retry replays its original receipt and preserves the original records. Neither replay nor an original-source metadata read grants fresh dispatch permission.

The SQL origin has a reserved `pending` storage discriminator to permit a later producer protocol without rebuilding its protected table. Current Rust supports only `raw`: readers reject the reserved discriminator, including a row indexed `pending` that otherwise contains valid raw source and poison bytes. Future Pending support requires a new protected producer protocol/schema version and complete native caller, loan and effect instrumentation. It cannot relabel an existing raw origin.

## Read boundaries

The original-source reader authenticates the exact first protected native source, actual first issuer, indexed routing fields, origin and first raw record. Its result is independent of later mutable Run tokens or source heads. Fresh native validation separately verifies the complete current native source history and also requires a consistent original enrollment record.

SQL readers charge selected text sizes against the caller's proof allowance before transfer or decoding. Memory readers charge borrowed selected records before hashing or cloning them. Prepared admission and Queue admission pass their enclosing proof through the actual initial publisher. Ordinary compatibility entry points use their own bounded allowance. These local bounds do not bound the inherited Memory whole-State transaction clone, database scan cost, total stored history or the number of lifetime operations.

## Erasure

Run enrollment identity, origins and first raw records are immutable. SQLite replacement guards cover alternate unique keys as well as primary keys. Erasure requires the exact session's existing native tombstone protocol; one scope's tombstone cannot release another scope's records.

Readiness checks reverse membership as well as each present Run. Removing the current Run set must not hide retained origin or first-raw rows. Scoped purge removes enrollment dependents before native source records and Runs. A missing or inconsistent record refuses authentication; it never reconstructs a clean birth or a positive ownership result.
