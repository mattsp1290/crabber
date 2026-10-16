# Original continuation reconciliation

Replaying an approval, native tool or frontend continuation, including its explicit Unstarted recovery, uses its original operation identity and captured request semantics. A matching receipt returns metadata and never creates another execution handle. Changed run, continuation generation, revision, supplied choices or frontend resolver assertions are rejected.

The native stores authenticate the original frozen batch against its protected suspension receipt and event before extracting its plan fingerprint. They bound that selected batch before transferring or cloning it. This read does not decode the mutable Run, checkpoint, current answers, owner tokens, or cross-kind handoff. Oversized unrelated current fields therefore do not require loading a private checkpoint merely to reconcile a receipt.

Custom stores that implement native continuations must also implement `original_approval_plan` and `original_tool_continuation_plan`. The default is Unsupported. Missing or changed original provenance fails closed; the runtime never initializes factories or dispatches work to recover it. Raw operation receipt lookup remains available independently. Fresh execution still validates the current Run, exact original binding, and native claim/Begin authority.
