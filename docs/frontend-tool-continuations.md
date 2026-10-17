# Frontend tool continuations

An `InputEnvelope` may declare `FrontendTool` entries. These declarations advertise tools to the model without registering backend executors. Their names must be unique and must not collide with registered backend tools. Input schemas and optional result contracts compile locally; remote schema references are unsupported.

When the model requests a declared frontend tool, the runtime freezes its original arguments, descriptor, assistant message and captured plan. It persists the call as waiting and pauses the run after sibling backend work has joined. Completed siblings remain completed. No `ToolExecute` hook runs for a frontend call.

Read `Agent::tool_continuation_snapshot` to obtain the required frontend call set. The embedding host authenticates external results and supplies `ContinueFrontend` with the exact continuation, expected revision, a `FrontendResolverClaim`, and one `FrontendToolResult` for every required frontend call. Partial, duplicate or foreign result sets are rejected. A result contains a typed `ToolOutput`; an optional result contract validates its canonical `content` and `is_error` fields. Input parameters never serve as an output contract. Failed and declined results must carry an error output.

A fresh claim reserves a new native attempt and result-processing phases. The runtime begins those phases normally, protects and redacts each supplied output, and publishes logical results before continuing the model. It does not dispatch a backend executor for frontend results. Native checkpoint questions in a mixed continuation must also have accepted answers before the complete batch can resume.

Use a stable operation ID for retries. Identical retries return a metadata receipt, including after the Agent closes its extensions; they never return execution authority. Changed results or resolver assertions under the same ID conflict. After a lost acknowledgement, look up or replay the original operation before choosing another action. `RecoverFrontend` can rotate an expired reservation only when result processing is positively unstarted. Started or uncertain work requires explicit host reconciliation and is never automatically repeated.

For a frozen plan with a preparation resource contract, the runtime checks the complete original result set, resolver assertion and protected native source before acquiring resources. The same retained task completes the native claim or recovery, verifies its acknowledgement, activates a distinct successor driver and processes the results. An unknown or mismatched acknowledgement permits only metadata reconciliation or an eligible unstarted recovery; actual acquired-resource cleanup remains independently retained. This does not qualify whole-Run resource ownership.

Reconciliation authenticates the original suspended plan without reading the mutable Run or current checkpoint. When a resumed model turn needs native approval, the protected handoff records the latest frontend claim or recovery receipt and its complete-result digest as its predecessor.

Durable cancellation of a waiting batch publishes interrupted logical results without inventing external answers. Historical suspended native phases remain suspended and complete. Store reads and keyed replay remain usable without initializing extensions.

This API shares the explicit native tool continuation barrier. It does not provide a browser transport, authenticate end users, or make public session snapshots include frontend result data. The host owns authentication and transport.
